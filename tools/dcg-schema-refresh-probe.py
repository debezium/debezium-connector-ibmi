#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Packaged connector: observed ALTER, history replay and truncate after refresh.

Uses the public probe's fresh owned fixture, credentials, artifacts and verified
cleanup. No reconstruction library or changes to application objects are needed.
"""
import importlib.util
import concurrent.futures
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location('public_schema', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Experiment(public.Experiment):
    def alter(self, statement, label):
        def execute():
            session = self.capture.connection()
            try:
                # Removing fixture data requires acknowledging IBM i's data-loss
                # inquiry. Override only this newly owned job, never the system
                # reply list or a shared QAQQINI table. Restore it before close.
                # https://www.ibm.com/docs/en/i/7.5.0?topic=qaqqini-query-options
                self.capture.execute(session, label + '-options-start', 'CALL QSYS2.OVERRIDE_QAQQINI(1)')
                try:
                    self.capture.execute(session, label + '-options',
                                         "CALL QSYS2.OVERRIDE_QAQQINI(2, 'SUPPRESS_INQUIRY_MESSAGES', '*YES')")
                    self.capture.execute(session, label, statement)
                finally:
                    self.capture.execute(session, label + '-options-reset', 'CALL QSYS2.OVERRIDE_QAQQINI(3)')
            finally:
                self.close_session(session)

        # Retain independent lock evidence while a bounded DDL call is pending.
        # Never end a job or release another session's locks to make it pass.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(execute)
            attempt = 0
            while not pending.done():
                attempt += 1
                self.fixture.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{self.library}','B','*FILE',0)) X",
                                 f'{label}-locks-{attempt}.json', timeout=5)
                if not pending.done():
                    time.sleep(1)
            return pending.result()

    def schema_checkpoint(self, ordinal, floor=None):
        saved = self.checkpoint(self.observer_container, 'automatic', 'schema-state')
        if not saved or saved.get('schema.completed.ordinal') != str(ordinal):
            return None
        if floor is not None and saved.get('schema.floor.ordinal') != str(floor):
            return None
        return saved

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        baseline = self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        unrelated = self.capture.connection(transaction=True)
        try:
            self.capture.execute(unrelated, 'hold-unrelated-cycle', f'INSERT INTO {self.library}.A VALUES (900)')
            self.alter(f"ALTER TABLE {self.library}.B ADD COLUMN NOTE VARCHAR(20) DEFAULT 'seed'", 'add-column')
            self.fixture.sql(f"INSERT INTO {self.library}.B (ID,NOTE) VALUES (10,'new')", 'new-definition-row.json', True)
            self.raw_interval(self.audit_start, 'add-column')
            self.row_progress(self.inserted('B', 10, 'after-add'), 'after-add')
            added = [record for record in self.table_records('B', 'added-schema') if record['value']
                     and (record['value'].get('after') or {}).get('ID') == 10]
            self.check('A committed ALTER refreshes native values and keys',
                       len(added) == 1 and added[0]['key'] == {'ID': 10}
                       and added[0]['value']['after'] == {'ID': 10, 'NOTE': 'new'}, 'control/added-schema.json')
            saved = public.base.wait_for('schema checkpoint with unrelated cycle open', lambda: self.schema_checkpoint(1, 0), 180)
            self.check('An unrelated open transaction retains old F while changed-table CDC advances W',
                       int(saved['replay_from.sequence']) < int(saved['completed_through.sequence']), 'control/schema-state.json')
            history = self.records(self.observer_container, 'bindings-automatic', 'schema-history-before-restart')
            self.command('docker', 'kill', worker)
            worker = self.start_worker('automatic', 'w2')
            self.lifecycle(worker, 'recovered')
            self.fixture.sql(f"INSERT INTO {self.library}.B (ID,NOTE) VALUES (12,'restart')", 'restarted-definition-row.json', True)
            self.row_progress(self.inserted('B', 12, 'after-schema-restart'), 'after-schema-restart')
            self.check('A replacement replays the schema boundary without appending another definition',
                       self.records(self.observer_container, 'bindings-automatic', 'schema-history-after-restart') == history,
                       'control/schema-history-after-restart.json')
            start = self.point()
            self.capture.execute(unrelated, 'rollback')
            self.progressed(self.terminal(start, unrelated[1], 'RB', 'unrelated-rollback'), 'unrelated-rollback')
        finally:
            self.close_session(unrelated)
        public.base.wait_for('schema floor advances after unrelated rollback', lambda: self.schema_checkpoint(1, 1), 180)
        self.alter(f'ALTER TABLE {self.library}.B DROP COLUMN NOTE', 'drop-column')
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (20)', 'restored-definition-row.json', True)
        self.raw_interval(self.audit_start, 'drop-column')
        self.row_progress(self.inserted('B', 20, 'after-drop'), 'after-drop')
        public.base.wait_for('second observed schema checkpoint', lambda: self.schema_checkpoint(2), 180)
        rows = self.table_records('B', 'dropped-schema')
        restored = [record for record in rows if record['value'] and (record['value'].get('after') or {}).get('ID') == 20]
        self.check('A later observed ALTER can restore the original layout without confusing earlier rows',
                   len(restored) == 1 and restored[0]['value']['after'] == {'ID': 20}, 'control/dropped-schema.json')
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'schema-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.fixture.sql(f"UPDATE {self.library}.M SET V='XYZ' WHERE ID=1", 'unrelated-after-schema.json', True)
            progress = public.base.wait_for('unrelated CDC during clear after schema refresh', lambda: next(
                (record for record in self.table_records('M', 'unrelated-schema-progress')[len(baseline['M']):]
                 if record['value'] and record['value']['op'] == 'u'), None), 150)
            self.row_progress(progress, 'unrelated-schema-progress')
            self.capture.execute(transaction, 'schema-reload', f'INSERT INTO {self.library}.B VALUES (500)')
            start = self.point()
            self.capture.execute(transaction, 'commit')
            self.progressed(self.terminal(start, transaction[1], 'CM', 'schema-clear-commit'), 'schema-clear-commit')
        finally:
            self.close_session(transaction)
        cleared = self.table_records('B', 'schema-clear')[len(rows):]
        self.check('Transactional truncate still precedes reload after both schema changes',
                   [record['value']['op'] for record in cleared if record['value']] == ['t', 'c']
                   and cleared[-1]['value']['after'] == {'ID': 500}, 'control/schema-clear.json')
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'w3')
        self.lifecycle(worker, 'recovered')
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (501)', 'schema-floor-restart-row.json', True)
        final = self.inserted('B', 501, 'after-schema-floor-restart')
        self.row_progress(final, 'after-schema-floor-restart')
        self.check('Restart after F advances restores the new schema directly and continues CDC',
                   final['key'] == {'ID': 501} and final['value']['after'] == {'ID': 501}, 'control/')
        worker = self.change_selection(worker)
        self.raw_interval(self.audit_start, 'schema-transitions')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 800], 'B': [500, 501], 'M': [1, 2, 3], 'U': [700]})
        self.report.update(status='passed', scope='Packaged public connector; observed native schema changes with durable history and task restart')
        self.save()

    def change_selection(self, worker):
        self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                     '--create', '--topic', f'test.{self.fixture.server_name}.{self.library}.U', '--partitions', '1', '--replication-factor', '1')
        history = self.records(self.observer_container, 'bindings-automatic', 'selection-history-before')
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'excluded-clear', f'TRUNCATE TABLE {self.library}.U REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'excluded-reload', f'INSERT INTO {self.library}.U VALUES (700)')
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (800)', 'excluded-unrelated.json', True)
            saved = self.row_progress(self.inserted('A', 800, 'excluded-unrelated'), 'excluded-unrelated')
            self.check('Excluded open transactions retain F while selected output advances W',
                       int(saved['replay_from.sequence']) < int(saved['completed_through.sequence'])
                       and self.table_records('U', 'still-excluded') == [], 'control/')
            changed = self.configuration(True)
            changed['table.include.list'] += ',' + self.library + '.U'
            self.request(worker, 'automatic', 'PUT', '/config', changed)
            self.command('docker', 'kill', worker)
            worker = self.start_worker('automatic', 'w4')
            self.lifecycle(worker, 'recovered')
            self.check('Selection restart retains the exact binding and schema history without resnapshot',
                       self.records(self.observer_container, 'bindings-automatic', 'selection-history-after') == history
                       and self.table_records('U', 'included-before-commit') == [], 'control/')
            start = self.point()
            self.capture.execute(transaction, 'commit')
            self.progressed(self.terminal(start, transaction[1], 'CM', 'selection-commit'), 'selection-commit')
            rows = self.table_records('U', 'selection-committed')
            self.check('Newly selected table reconstructs its earlier clear and reload from F',
                       [record['value']['op'] for record in rows if record['value']] == ['t', 'c']
                       and rows[-1]['key'] == {'ID': 700} and rows[-1]['value']['after'] == {'ID': 700}, 'control/selection-committed.json')
        finally:
            self.close_session(transaction)
        return worker


if __name__ == '__main__':
    public.main(Experiment)
