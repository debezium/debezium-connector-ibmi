#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Requested full snapshot from retained native F/W using the public connector.

Runs only in the public harness's fresh owned library; retains raw journal,
worker and offset evidence and uses its verified native/container cleanup.
"""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('public_snapshot', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Experiment(public.Experiment):
    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ' + ','.join(f'({key})' for key in range(4000, 4300)),
                         'snapshot-seed.json', True)
        self.row_progress(self.inserted('A', 4299, 'snapshot-seed'), 'snapshot-seed')
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'hold-unrelated-transaction', f'INSERT INTO {self.library}.U VALUES (900)')
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (500)', 'snapshot-boundary-progress.json', True)
            before = self.row_progress(self.inserted('A', 500, 'snapshot-boundary-progress'), 'snapshot-boundary-progress')
            self.check('Requested snapshot starts with retained history for an unrelated open transaction',
                       int(before['replay_from.sequence']) < int(before['completed_through.sequence']), 'control/')
            history = self.records(self.observer_container, 'bindings-automatic', 'before-requested-snapshot')
            config = self.configuration(True)
            config['snapshot.mode'] = 'always'
            self.request(worker, 'automatic', 'PUT', '/config', config)
            def completed():
                saved = self.checkpoint(self.observer_container, 'automatic', 'requested-snapshot')
                rows = self.table_records('A', 'requested-A')
                snapshots = [r for r in rows if r['value'] and r['value']['op'] == 'r']
                return saved if saved and 'full_snapshot.pending' not in saved and len(snapshots) == 308 else None
            saved = public.base.wait_for('requested full snapshot with existing F/W', completed, 180)
            snapshots = [r['value'] for table in ['A', 'B', 'M'] for r in self.table_records(table, 'snapshot-flags-' + table)
                         if r['value'] and r['value']['op'] == 'r']
            self.check('A new ordinary snapshot completes with one last marker and retains the open transaction floor',
                       sum(v['source']['snapshot'] == 'last' for v in snapshots) == 2
                       and saved['replay_from.sequence'] == before['replay_from.sequence']
                       and int(saved['completed_through.sequence']) >= int(before['completed_through.sequence']), 'control/')
            self.check('Requested full snapshot preserves its binding history generation',
                       self.records(self.observer_container, 'bindings-automatic', 'after-requested-snapshot') == history,
                       'control/after-requested-snapshot.json')
            self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (500)', 'after-snapshot-insert.json', True)
            self.row_progress(self.inserted('B', 500, 'after-snapshot-insert'), 'after-snapshot-insert')
            start = self.point()
            self.capture.execute(transaction, 'rollback')
            self.progressed(self.terminal(start, transaction[1], 'RB', 'snapshot-open-rollback'), 'snapshot-open-rollback')
        finally:
            self.close_session(transaction)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'w2')
        self.lifecycle(worker, 'recovered')
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'after-snapshot-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'after-snapshot-reload', f'INSERT INTO {self.library}.B VALUES (700)')
            start = self.point()
            self.capture.execute(transaction, 'commit')
            self.progressed(self.terminal(start, transaction[1], 'CM', 'after-snapshot-commit'), 'after-snapshot-commit')
        finally:
            self.close_session(transaction)
        rows = self.table_records('B', 'after-requested-snapshot-restart')
        self.check('Ordinary restart after the requested snapshot preserves truncate/reload order',
                   [r['value']['op'] for r in rows[-2:] if r['value']] == ['t', 'c']
                   and rows[-1]['value']['after'] == {'ID': 700}, 'control/')
        self.check('Returning to initial mode does not repeat the completed snapshot',
                   sum(r['value'] and r['value']['op'] == 'r' for r in self.table_records('A', 'final-snapshot-count')) == 308, 'control/')
        self.raw_interval(self.audit_start, 'requested-snapshot')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 500, *range(4000, 4300)], 'B': [700], 'M': [1, 2, 3], 'U': [1, 2, 3]})
        self.report.update(status='passed', scope='Public connector requested full snapshot with existing F/W, open transaction and restart')
        self.save()


if __name__ == '__main__':
    public.main(Experiment)
