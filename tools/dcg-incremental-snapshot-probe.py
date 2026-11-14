#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Packaged connector: ordinary signaling, chunk restart and committed clear interleaving.

Fresh owned IBM i fixture and local Kafka only. Uses the public probe's credential,
artifact, raw-journal and verified cleanup machinery.
"""
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location('public_incremental', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Fixture(public.packaged.continuous.MixedFixture):
    def setup(self):
        super().setup()
        self.sql(f'CREATE TABLE {self.library}.SIGNALS (ID VARCHAR(42) NOT NULL PRIMARY KEY, TYPE VARCHAR(32) NOT NULL, DATA VARCHAR(2048))',
                 'create-signals.json', True)
        self.TABLES = [*self.TABLES, 'SIGNALS']
        self.owned_objects = self.inventory('objects-with-signals.json')


class Experiment(public.Experiment):
    def configuration(self, enabled):
        config = super().configuration(enabled)
        config.update({'signal.data.collection': self.fixture.server_name + '.' + self.library + '.SIGNALS',
                       'signal.enabled.channels': 'source', 'signal.poll.interval.ms': '100',
                       'incremental.snapshot.chunk.size': '10'})
        config['table.include.list'] += ',' + self.library + '.SIGNALS'
        return config

    def signal(self, name, kind='execute-snapshot', table='A'):
        payload = json.dumps({'data-collections': [self.fixture.server_name + '.' + self.library + '.' + table], 'type': 'incremental'})
        self.fixture.sql(f"INSERT INTO {self.library}.SIGNALS VALUES ('{name}','{kind}','{payload}')", name + '-signal.json', True)

    def healthy(self, worker):
        status = self.rest(worker, '/connectors/automatic/status')
        (self.directory / 'incremental-status.json').write_text(self.fixture.redact(json.dumps(status, indent=2)))
        if any(task['state'] == 'FAILED' for task in status['tasks']):
            raise RuntimeError('Task failed during incremental snapshot; retained status and worker log')

    def active_snapshot(self, worker, label):
        self.healthy(worker)
        saved = self.checkpoint(self.observer_container, 'automatic', label)
        return saved if saved and saved.get('offset.version') == '3' and saved.get('incremental_snapshot_collections') else None

    def snapshot_done(self, worker, label):
        self.healthy(worker)
        saved = self.checkpoint(self.observer_container, 'automatic', label)
        return saved if saved and saved.get('offset.version') == '2' else None

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        baseline = self.setup_connector(worker)
        self.audit_start = self.point()
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ' + ','.join(f'({key})' for key in range(100, 400)), 'snapshot-seed.json', True)
        # Queue a real source signal before enabling the feature. Handoff must
        # preserve its execution instead of acknowledging and discarding it.
        self.signal('during-handoff')
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        expected = {1, 2, 3, 10, *range(100, 400)}
        legacy = self.table_records('A', 'handoff-incremental')[len(baseline['A']):]
        reread = [row for row in legacy if row['value'] and row['value']['op'] == 'r']
        self.check('A source snapshot request queued before handoff completes through ordinary CDC',
                   {row['key']['ID'] for row in reread} == expected, 'control/handoff-incremental.json')
        before = len(self.table_records('A', 'before-native-snapshot'))
        self.signal('native-restart')
        saved = public.base.wait_for('native incremental checkpoint', lambda: self.active_snapshot(worker, 'snapshot-active'), 180)
        history = self.records(self.observer_container, 'bindings-automatic', 'snapshot-history-before')
        self.check('Snapshot chunk state is durable alongside conservative F/W',
                   saved.get('replay_from.sequence') and saved.get('completed_through.sequence'), 'control/')
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'w2')
        self.lifecycle(worker, 'recovered')
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (500)', 'unrelated-during-snapshot.json', True)
        self.row_progress(self.inserted('B', 500, 'unrelated-snapshot'), 'unrelated-snapshot')
        public.base.wait_for('restarted incremental snapshot complete', lambda: self.snapshot_done(worker, 'snapshot-complete'), 240)
        rows = self.table_records('A', 'snapshot-after-restart')[before:]
        self.check('A fresh task completes every snapshot key after interruption without new binding history',
                   {row['key']['ID'] for row in rows if row['value'] and row['value']['op'] == 'r'} == expected
                   and self.records(self.observer_container, 'bindings-automatic', 'snapshot-history-after') == history,
                   'control/snapshot-after-restart.json')
        self.signal('native-clear')
        public.base.wait_for('snapshot active before clear', lambda: self.active_snapshot(worker, 'clear-window'), 180)
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'snapshot-clear', f'TRUNCATE TABLE {self.library}.A REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'snapshot-reload', f'INSERT INTO {self.library}.A VALUES ' + ','.join(f'({key})' for key in range(5000, 5010)))
            start = self.point()
            self.capture.execute(transaction, 'commit')
            self.progressed(self.terminal(start, transaction[1], 'CM', 'snapshot-commit'), 'snapshot-commit')
        finally:
            self.close_session(transaction)
        public.base.wait_for('snapshot complete after clear', lambda: self.snapshot_done(worker, 'clear-complete'), 240)
        projected = set()
        for row in self.table_records('A', 'snapshot-clear-projection'):
            value = row['value']
            if not value:
                continue
            if value['op'] == 't':
                projected.clear()
            elif value['op'] in ('r', 'c', 'u'):
                projected.add(value['after']['ID'])
            elif value['op'] == 'd':
                projected.discard(value['before']['ID'])
        self.check('Incremental snapshot and committed truncate converge to the exact native rows',
                   projected == set(range(5000, 5010)), 'control/snapshot-clear-projection.json')
        self.raw_interval(self.audit_start, 'incremental-transactions')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': list(range(5000, 5010)), 'B': [1, 2, 3, 500], 'M': [1, 2, 3], 'U': [1, 2, 3]})
        self.report.update(status='passed', scope='Packaged public connector, standard source signaling and actual native journal')
        self.save()


if __name__ == '__main__':
    public.main(Experiment, Fixture)
