#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Owned native regression for a transactional clear before online activation.

Uses the public probe's explicit fixture authorization, local Kafka, credential
redaction and cleanup. No production source or independent startup authority.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location('public', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Experiment(public.Experiment):

    def stage(self):
        super().stage()
        self.report['harness_sha256']['tools/dcg-deferred-clear-probe.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def start_worker(self, scenario, role):
        worker = super().start_worker(scenario, role)
        self.current_worker = worker
        return worker

    def healthy(self, label):
        state = self.rest(self.current_worker, '/connectors/automatic/status')
        (self.directory / (label + '-status.json')).write_text(self.fixture.redact(json.dumps(state, indent=2)))
        if any(t['state'] == 'FAILED' for t in state['tasks']):
            raise RuntimeError('Task failed during ' + label)
        return state

    def row(self, table, key, label):

        def observed():
            self.healthy(label)
            rows = [r for r in self.table_records(table, label)
                    if r['value'] and r['value']['op'] == 'c' and r['value']['after']['ID'] == key]
            return rows[-1] if rows else None
        return public.base.wait_for(label, observed, 150)

    def exercise(self):
        e = self.capture
        e.deadline = None
        w = self.start_worker('automatic', 'w1')
        baseline = self.setup_connector(w)
        busy = e.connection(transaction=True)
        e.execute(busy, 'hold-admission', f'INSERT INTO {self.library}.U VALUES(90)')
        config = self.configuration(True)
        self.request(w, 'automatic', 'PUT', '/config', config)
        self.request(w, 'automatic', 'PUT', '/resume')
        writer = e.connection()
        tx = e.connection(transaction=True)
        e.execute(writer, 'prefix-row', f'INSERT INTO {self.library}.A VALUES(1000)')
        self.row('A', 1000, 'prefix-row')
        saved = self.checkpoint(self.observer_container, 'automatic', 'prefix-offset')
        self.check('Busy excluded transaction defers activation while legacy publication continues', 'offset.version' not in saved, 'control/')
        start = self.point()
        for outcome, key in [('rollback', 1001), ('commit', 1002), ('savepoint', 1003), ('restart', 1004)]:
            before = self.table_records('B', outcome + '-before')
            if outcome == 'savepoint':
                e.execute(tx, 'savepoint', 'SAVEPOINT S ON ROLLBACK RETAIN CURSORS')
            clear = (f'DELETE FROM {self.library}.B' if outcome == 'commit'
                     else f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
            e.execute(tx, outcome + '-clear', clear)
            e.execute(tx, outcome + '-replacement', f'INSERT INTO {self.library}.B VALUES({key})')
            e.execute(writer, outcome + '-unrelated', f'INSERT INTO {self.library}.A VALUES({key})')
            row = self.row('A', key, outcome + '-unrelated')
            held = self.checkpoint(self.observer_container, 'automatic', outcome + '-held-offset')
            self.check(outcome + ': unrelated row is published while clear and replacement remain held',
                       self.table_records('B', outcome + '-held') == before, 'control/')
            self.check(outcome + ': durable legacy position precedes the open clear',
                       int(held['offset.event_sequence']) < int(row['value']['source']['sequence']), 'control/')
            if outcome == 'commit':
                e.execute(writer, 'held-flood', f'INSERT INTO {self.library}.A VALUES '
                          + ','.join(f'({k})' for k in range(20000, 20600)))
                self.row('A', 20599, 'held-flood')
                flood = [r for r in self.table_records('A', 'held-flood-all')
                         if r['value'] and r['value']['op'] == 'c' and 20000 <= r['value']['after']['ID'] < 20600]
                after_flush = self.checkpoint(self.observer_container, 'automatic', 'held-flood-offset')
                self.check('600 unrelated rows cross automatic batch boundaries without releasing the clear offset',
                           len(flood) == 600 and after_flush['offset.event_sequence'] == held['offset.event_sequence'], 'control/')
            if outcome == 'restart':
                old, new = self.fixture.rotate()
                self.check('Open clear spans different owned receiver identities', old != new, 'native/rotation-after.json')
                self.command('docker', 'kill', w)
                w = self.start_worker('automatic', 'w2')
                e.execute(writer, 'replacement-reader-progress', f'INSERT INTO {self.library}.A VALUES(1005)')
                self.row('A', 1005, 'replacement-reader-progress')
                self.check('Replacement keeps the open clear unpublished', self.table_records('B', 'restart-held') == before, 'control/')
            if outcome == 'savepoint':
                e.execute(tx, 'rollback-savepoint', 'ROLLBACK TO SAVEPOINT S')
                e.execute(tx, 'release-savepoint', 'RELEASE SAVEPOINT S')
                e.execute(tx, 'commit')
            else:
                e.execute(tx, 'rollback' if outcome == 'rollback' else 'commit')
            e.execute(writer, outcome + '-after', f'INSERT INTO {self.library}.A VALUES({key + 100})')
            after = self.row('A', key + 100, outcome + '-after')

            def advanced():
                self.healthy(outcome + '-complete')
                pos = self.checkpoint(self.observer_container, 'automatic', outcome + '-complete')
                return pos if int(pos.get('offset.event_sequence', '0')) >= int(after['value']['source']['sequence']) else None
            public.base.wait_for(outcome + ' outcome acknowledged', advanced, 180)
            rows = self.table_records('B', outcome + '-result')
            if outcome in ['rollback', 'savepoint']:
                self.check(outcome + ': no truncate or replacement escapes rolled-back work', rows == before, 'control/')
            else:
                suffix = rows[len(before):]
                self.check(outcome + ': committed clear precedes replacement', [(r['value']['op'], r['value'].get('after')) for r in suffix] == [('t', None), ('c', {'ID': key})], 'control/')
        before = self.table_records('B', 'consecutive-before')
        for key in [3000, 3001]:
            e.execute(tx, 'consecutive-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
            e.execute(tx, 'consecutive-replacement', f'INSERT INTO {self.library}.B VALUES({key})')
            e.execute(tx, 'commit')
        e.execute(writer, 'consecutive-progress', f'INSERT INTO {self.library}.A VALUES(1190)')
        after = self.row('A', 1190, 'consecutive-progress')

        def consecutive_complete():
            self.healthy('consecutive-complete')
            saved = self.checkpoint(self.observer_container, 'automatic', 'consecutive-complete')
            return int(saved.get('offset.event_sequence', '0')) >= int(after['value']['source']['sequence'])
        public.base.wait_for('consecutive clear acknowledgements', consecutive_complete, 180)
        suffix = self.table_records('B', 'consecutive-after')[len(before):]
        self.check('Consecutive cleared transactions preserve table order and both terminals', [(r['value']['op'], r['value'].get('after')) for r in suffix] == [('t', None), ('c', {'ID': 3000}), ('t', None), ('c', {'ID': 3001})], 'control/')
        before = self.table_records('B', 'member-clear-before')
        e.execute(tx, 'member-clear-transaction',
                  f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        e.execute(tx, 'member-clear-transaction-row', f'INSERT INTO {self.library}.B VALUES(4000)')
        e.execute(writer, 'member-clear-unrelated', f'INSERT INTO {self.library}.A VALUES(1191)')
        held_row = self.row('A', 1191, 'member-clear-unrelated')
        held = self.checkpoint(self.observer_container, 'automatic', 'member-clear-held')
        self.check('Following member-clear case begins with a pinned transactional clear',
                   int(held['offset.event_sequence']) < int(held_row['value']['source']['sequence'])
                   and self.table_records('B', 'member-clear-held') == before, 'control/')
        self.command('docker', 'kill', w)
        e.execute(tx, 'commit')
        e.execute(writer, 'following-native-member-clear',
                  f"CALL QSYS2.QCMDEXC('CLRPFM FILE({self.library}/B)')")
        e.execute(writer, 'following-member-reload', f'INSERT INTO {self.library}.B VALUES(4001)')
        w = self.start_worker('automatic', 'w3')
        e.execute(writer, 'member-clear-after', f'INSERT INTO {self.library}.A VALUES(1192)')
        after = self.row('A', 1192, 'member-clear-after')

        def member_clear_complete():
            self.healthy('member-clear-complete')
            saved = self.checkpoint(self.observer_container, 'automatic', 'member-clear-complete')
            return int(saved.get('offset.event_sequence', '0')) >= int(after['value']['source']['sequence'])

        public.base.wait_for('following member clear acknowledged after restart', member_clear_complete, 180)
        suffix = self.table_records('B', 'member-clear-result')[len(before):]
        self.check('Pinned restart delivers transactional clear, native member clear and reload in source order',
                   [(r['value']['op'], r['value'].get('after')) for r in suffix]
                   == [('t', None), ('c', {'ID': 4000}), ('t', None), ('c', {'ID': 4001})], 'control/')
        e.execute(busy, 'rollback')
        self.close_session(busy)
        self.lifecycle(w, 'handoff-complete')
        e.execute(writer, 'native-progress', f'INSERT INTO {self.library}.A VALUES(1200)')
        final = self.row_progress(self.row('A', 1200, 'native-progress'), 'native-progress')
        self.check('Normal native recovery takes over after deferred outcomes drain', final.get('offset.version') in ('2', '4'), 'control/')
        self.check('Enablement and restart do not resnapshot', all((sum((r['value'] and r['value']['op'] == 'r' for r in self.table_records(t, 'final-' + t))) == sum((r['value'] and r['value']['op'] == 'r' for r in rows)) for t, rows in baseline.items())), 'control/')
        raw = self.raw_interval(start, 'deferred-outcomes')
        subtypes = [bytes.fromhex(r['DATA_HEX'])[108:109].decode('cp037') for r in raw if r['JOURNAL_CODE'] == 'D' and r['JOURNAL_ENTRY_TYPE'] == 'CG']
        self.check('Native evidence includes both optimized DELETE and TRUNCATE', {'3', '8'} <= set(subtypes), 'deferred-outcomes-raw.json')
        self.check('Native evidence includes the following F/CR',
                   any(r['JOURNAL_CODE'] == 'F' and r['JOURNAL_ENTRY_TYPE'] == 'CR' for r in raw),
                   'deferred-outcomes-raw.json')
        self.close_session(tx)
        self.close_session(writer)
        self.report['status'] = 'passed'
        self.save()
if __name__ == '__main__':
    public.main(Experiment)
