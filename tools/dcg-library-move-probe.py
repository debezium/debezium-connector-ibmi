#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Captured library moves using two fresh owned libraries and local Kafka.

Reuses the public packaged runner, including its credential, receiver, worker
drain and exact fixture cleanup checks. No test provider enters the plugin.
"""
import argparse
import hashlib
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import zipfile

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('move_public', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)
NativeFixture = public.packaged.continuous.native.fixture_module.NativeFixture


def identities(rows):
    return {(r['OBJNAME'], r['OBJTYPE'], r['OBJATTRIBUTE'], r['OBJCREATED']) for r in rows}


def output(rows):
    """Keep exact order, keys and values; duplicates must not be normalized away."""
    return [{'op': row['value']['op'], 'key': row['key'], 'after': row['value'].get('after')}
            for row in rows if row['value'] is not None]


class Fixture(NativeFixture):
    destination_library = None

    def setup(self):
        super().setup()
        self.destination = NativeFixture(self.directory / 'destination', self.destination_library)
        self.destination.setup()
        self.destination.sql(f'DROP TABLE {self.destination.library}.B RESTRICT', 'remove-target-B.log', True)
        self.destination.TABLES = [t for t in self.destination.TABLES if t != 'B']
        self.destination.owned_objects = self.destination.inventory('objects-target-ready.json')

    def create_n(self):
        self.sql(f'CREATE TABLE {self.library}.N (ID INTEGER NOT NULL PRIMARY KEY, X INTEGER)', 'create-N.log', True)
        self.TABLES = [*self.TABLES, 'N']
        self.owned_objects = self.inventory('objects-created-N.json')

    def transfer(self, source, destination, table, label):
        assert table in ('B', 'N') and source is not destination
        before_source = source.inventory(label + '-source-before.json')
        before_destination = destination.inventory(label + '-destination-before.json')
        assert identities(before_source) == identities(source.owned_objects)
        assert identities(before_destination) == identities(destination.owned_objects)
        assert any(r['OBJNAME'] == table and r['OBJTYPE'] == '*FILE' for r in before_source)
        assert not any(r['OBJNAME'] == table and r['OBJTYPE'] == '*FILE' for r in before_destination)
        source.sql(f"CALL QSYS2.QCMDEXC('MOVOBJ OBJ({source.library}/{table}) OBJTYPE(*FILE) TOLIB({destination.library})')",
                   label + '.log', True)
        after_source = source.inventory(label + '-source-after.json')
        after_destination = destination.inventory(label + '-destination-after.json')
        removed = identities(before_source) - identities(after_source)
        added = identities(after_destination) - identities(before_destination)
        assert removed == added and any(row[0] == table and row[1] == '*FILE' for row in removed)
        assert not identities(after_source) - identities(before_source)
        assert not identities(before_destination) - identities(after_destination)
        assert all(row[1] == '*FILE' and row[0] not in ('A', 'M', 'U') for row in removed)
        source.owned_objects, destination.owned_objects = after_source, after_destination
        source.TABLES = [t for t in source.TABLES if t != table]
        destination.TABLES = [*destination.TABLES, table]
        source.save(label + '-ownership-transfer.json', {
            'source': source.library, 'destination': destination.library,
            'objects': [list(row[:3]) + [str(row[3])] for row in sorted(removed)]})
        journaling = destination.sql(
            f"SELECT JOURNAL_LIBRARY,JOURNAL_NAME FROM TABLE(QSYS2.OBJECT_STATISTICS('{destination.library}','*FILE','{table}')) X",
            label + '-journaling.json')
        assert journaling == [{'JOURNAL_LIBRARY': self.library, 'JOURNAL_NAME': 'QSQJRN'}]

    def cleanup(self):
        destination = getattr(self, 'destination', None)
        if destination is not None:
            result = destination.cleanup()
            destination.save('fixture-cleanup.json', result)
            assert result['status'] in ('passed', 'not-created')
        result = super().cleanup()
        if destination is not None:
            result['destination_fixture_cleanup'] = str(destination.directory / 'fixture-cleanup.json')
        return result


class Experiment(public.Experiment):
    selection = 'both'
    distribution = None

    def snapshot_rows(self, table):
        return 0

    def selected(self, library):
        return self.selection == 'both' or library == (
            self.library if self.selection == 'source' else self.fixture.destination.library)

    def configuration(self, enabled):
        config = super().configuration(enabled)
        # Use standard exclusions so legacy startup does not try to resolve
        # files that will only be created or moved later in the experiment.
        config.pop('table.include.list')
        excludes = [r'.*\.U']
        for library in (self.library, self.fixture.destination.library):
            if not self.selected(library):
                excludes.extend(library + r'\.' + t for t in ('B', 'N'))
        config.update({'snapshot.mode': 'no_data', 'table.exclude.list': ','.join(excludes)})
        if enabled:
            config['truncate.buffer.storage'] = 'disk'
        return config

    def stage(self):
        super().stage()
        for path in (self.plugin / 'ibmi').iterdir():
            path.unlink()
        with zipfile.ZipFile(self.distribution) as archive:
            for entry in archive.infolist():
                if entry.filename.endswith('.jar'):
                    (self.plugin / 'ibmi' / Path(entry.filename).name).write_bytes(archive.read(entry))
        self.report.update(
            scope='Captured CREATE/ALTER/library moves, exact SQL-name selection and topics, open clear and replacement',
            selection=self.selection, destination_library=self.fixture.destination.library,
            source_checkpoint=subprocess.check_output(['git', '-C', str(public.ROOT), 'rev-parse', 'HEAD'], text=True).strip(),
            distribution_sha256=hashlib.sha256(self.distribution.read_bytes()).hexdigest(),
            plugin_sha256={str(p.relative_to(self.plugin)): hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in (self.plugin / 'ibmi').iterdir()})
        self.report['harness_sha256']['tools/dcg-library-move-probe.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.expected = {(library, table): [] for library in (self.library, self.fixture.destination.library) for table in ('B', 'N')}
        self.barriers = []
        self.save()

    def topic(self, library, table):
        return f'test.{self.fixture.server_name}.{library}.{table}'

    def setup_connector(self, worker):
        for library, table in self.expected:
            if library == self.library and table == 'B':
                continue  # Created by the shared runner.
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', self.topic(library, table), '--partitions', '1', '--replication-factor', '1')
        return super().setup_connector(worker)

    def expect_row(self, library, table, key, extra=None):
        if self.selected(library):
            self.expected[library, table].append({'op': 'c', 'key': {'ID': key}, 'after': {'ID': key, **(extra or {})}})

    def expect_clear(self, library, table):
        if self.selected(library):
            self.expected[library, table].append({'op': 't', 'key': None, 'after': None})

    def check_ledger(self, label):
        ledger = {}
        for (library, table), expected in self.expected.items():
            rows = self.records(self.observer_container, self.topic(library, table), label + '-' + library + '-' + table)
            assert all(r['value'] is not None for r in rows), 'Unexpected tombstone'
            actual = output(rows)
            ledger[self.topic(library, table)] = actual
            self.check('Exact topic/key/value/order ledger: ' + label + ' ' + library + '.' + table,
                       actual == expected, 'control/' + label + '-' + library + '-' + table + '.json')
        self.report.setdefault('output_ledgers', {})[label] = ledger
        self.save()

    def progress(self, key, label):
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES({key})', label + '-insert.log', True)
        self.barriers.append(key)
        checkpoint = self.row_progress(self.inserted('A', key, label), label)
        self.report.setdefault('progress_checkpoints', {})[label] = checkpoint
        self.save()
        return checkpoint

    def replace(self, worker, role):
        self.command('docker', 'kill', worker)
        replacement = self.start_worker('automatic', role)
        self.lifecycle(replacement, 'recovered')
        return replacement

    def exercise(self):
        self.capture.deadline = None
        source, target = self.fixture, self.fixture.destination
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        source.sql(f'INSERT INTO {source.library}.B VALUES(1001)', 'source-before-move.log', True)
        self.expect_row(source.library, 'B', 1001)
        self.progress(9700, 'before-moves')
        self.check_ledger('before-moves')

        self.command('docker', 'kill', worker)
        source.sql(f'ALTER TABLE {source.library}.B ADD COLUMN X INTEGER', 'alter-B.log', True)
        source.sql(f'INSERT INTO {source.library}.B VALUES(1002,2)', 'altered-source-row.log', True)
        self.expect_row(source.library, 'B', 1002, {'X': 2})
        source.transfer(source, target, 'B', 'alter-move-B')
        target.sql(f'INSERT INTO {target.library}.B VALUES(1003,3)', 'moved-B-row.log', True)
        self.expect_row(target.library, 'B', 1003, {'X': 3})
        source.create_n()
        source.sql(f'INSERT INTO {source.library}.N VALUES(2001,11)', 'created-source-row.log', True)
        self.expect_row(source.library, 'N', 2001, {'X': 11})
        source.transfer(source, target, 'N', 'create-move-N')
        target.sql(f'INSERT INTO {target.library}.N VALUES(2002,12)', 'moved-N-row.log', True)
        self.expect_row(target.library, 'N', 2002, {'X': 12})
        worker = self.start_worker('automatic', 'w2')
        self.lifecycle(worker, 'recovered')
        self.progress(9701, 'after-catchup')
        self.check_ledger('after-catchup')

        transaction = self.capture.connection(transaction=True)
        self.capture.execute(transaction, 'moved-clear', f'TRUNCATE TABLE {target.library}.N REUSE STORAGE IGNORE DELETE TRIGGERS')
        self.capture.execute(transaction, 'moved-reload', f'INSERT INTO {target.library}.N VALUES(2003,13)')
        held = self.progress(9702, 'clear-open')
        self.check('Open moved-table transaction pins F while unrelated W advances',
                   int(held['replay_from.sequence']) < int(held['completed_through.sequence']), 'control/')
        self.check_ledger('clear-open')
        worker = self.replace(worker, 'w3')
        self.progress(9703, 'recovered-open')
        self.check_ledger('recovered-open')
        start = self.point()
        self.capture.execute(transaction, 'commit')
        self.expect_clear(target.library, 'N')
        self.expect_row(target.library, 'N', 2003, {'X': 13})
        self.progressed(self.terminal(start, transaction[1], 'CM', 'moved-commit'), 'moved-commit')
        self.check_ledger('committed')
        self.capture.execute(transaction, 'moved-rollback-clear', f'TRUNCATE TABLE {target.library}.N REUSE STORAGE IGNORE DELETE TRIGGERS')
        self.capture.execute(transaction, 'moved-rollback-row', f'INSERT INTO {target.library}.N VALUES(2004,14)')
        start = self.point()
        self.capture.execute(transaction, 'rollback')
        self.progressed(self.terminal(start, transaction[1], 'RB', 'moved-rollback'), 'moved-rollback')
        self.close_session(transaction)
        self.check_ledger('rolled-back')

        for table, key, x in [('B', 1004, 4), ('N', 2005, 15)]:
            source.transfer(target, source, table, 'move-back-' + table)
            source.sql(f'INSERT INTO {source.library}.{table} VALUES({key},{x})', 'restored-' + table + '.log', True)
            self.expect_row(source.library, table, key, {'X': x})
        self.progress(9704, 'moved-back')
        self.check_ledger('moved-back')
        worker = self.replace(worker, 'w4')
        self.progress(9705, 'final-recovery')
        self.check_ledger('final-recovery')
        rows = self.table_records('A', 'final-A')
        self.check('Every unrelated barrier appears once across replacements',
                   [r['key']['ID'] for r in rows] == [10, *self.barriers]
                   and all(r['value']['op'] == 'c' for r in rows), 'control/final-A.json')
        source.verify_rows({'A': [1, 2, 3, 10, *self.barriers], 'B': [1, 2, 3, 1001, 1002, 1003, 1004], 'N': [2003, 2005]})
        self.raw_interval(self.audit_start, 'captured-move-lifetime')
        self.report.update(status='passed', limitation='Bounded same-journal library moves; no cross-pool/save-restore or multi-member clear claim')
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    parser.add_argument('source_library')
    parser.add_argument('destination_library')
    parser.add_argument('--delivery', choices=['source-offsets', 'eos'], default='source-offsets')
    parser.add_argument('--selection', choices=['both', 'source', 'destination'], default='both')
    parser.add_argument('--distribution', type=Path, required=True)
    args = parser.parse_args()
    if (any(not re.fullmatch(r'DCNC[0-9]{4}', name) for name in (args.source_library, args.destination_library))
            or args.source_library == args.destination_library or not args.distribution.is_file()):
        parser.error('Two distinct fresh DCNCdddd libraries and an existing plugin ZIP are required')
    Fixture.destination_library = args.destination_library
    Experiment.selection = args.selection
    Experiment.distribution = args.distribution.resolve()
    sys.argv = [sys.argv[0], args.directory, args.source_library, args.delivery]
    public.main(Experiment, Fixture)


if __name__ == '__main__':
    main()
