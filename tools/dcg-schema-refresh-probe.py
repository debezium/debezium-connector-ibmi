#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Packaged connector: observed ALTER, history replay and truncate after refresh.

Uses the public probe's fresh owned fixture, credentials, artifacts and verified
cleanup. No reconstruction library or changes to application objects are needed.
"""
import importlib.util
import concurrent.futures
from contextlib import closing
import sys
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
            self.request(worker, 'automatic', 'PUT', '/stop')
            self.lifecycle(worker, 'drained')
            self.alter(f"ALTER TABLE {self.library}.B ADD COLUMN NOTE VARCHAR(20) DEFAULT 'seed'", 'add-column')
            self.fixture.sql(f"LABEL ON COLUMN {self.library}.B.NOTE IS 'isolated schema label'", 'label-added-column.log', True)
            self.request(worker, 'automatic', 'PUT', '/resume')
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
        worker = self.transactional_ddl(worker)
        worker = self.change_selection(worker)
        self.raw_interval(self.audit_start, 'schema-transitions')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 800, 801, 802, 803, 804], 'B': [500, 501, 502, 503], 'M': [1, 2, 3], 'U': [700]})
        self.report.update(status='passed', scope='Packaged public connector; observed native schema changes with durable history and task restart')
        self.save()

    def transactional_ddl(self, worker):
        for table in ['B', 'U']:
            for operation in ['LABEL', 'COMMENT']:
                self.fixture.sql(f"{operation} ON TABLE {self.library}.{table} IS 'isolated schema validation'",
                                 operation.lower() + '-' + table + '.log', True)
        self.fixture.sql(f"CALL QSYS2.QCMDEXC('CHGJRNOBJ OBJ(({self.library}/U *FILE)) ATR(*IMAGES) IMAGES(*AFTER)')",
                         'excluded-after-images.log', True)
        self.fixture.sql(f'UPDATE {self.library}.U SET ID=ID', 'excluded-after-update.log', True)
        self.fixture.sql(f"CALL QSYS2.QCMDEXC('CHGJRNOBJ OBJ(({self.library}/U *FILE)) ATR(*IMAGES) IMAGES(*BOTH)')",
                         'excluded-both-images.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (801)', 'after-administration.log', True)
        self.row_progress(self.inserted('A', 801, 'after-administration'), 'after-administration')
        self.check('LABEL, COMMENT and excluded image changes preserve subsequent captured output', True, 'control/')
        transaction = self.capture.connection(transaction=True)
        try:
            for table in ['B', 'U']:
                self.capture.execute(transaction, 'grant-' + table, f'GRANT SELECT ON {self.library}.{table} TO PUBLIC')
                self.capture.execute(transaction, 'commit')
                self.capture.execute(transaction, 'revoke-' + table, f'REVOKE SELECT ON {self.library}.{table} FROM PUBLIC')
                self.capture.execute(transaction, 'commit')
                self.capture.execute(transaction, 'label-' + table, f"LABEL ON TABLE {self.library}.{table} IS 'transactional label'")
                self.capture.execute(transaction, 'commit')
                self.capture.execute(transaction, 'comment-' + table, f"COMMENT ON TABLE {self.library}.{table} IS 'rolled back comment'")
                self.capture.execute(transaction, 'rollback')
            self.capture.execute(transaction, 'transactional-alter', f'ALTER TABLE {self.library}.B ADD COLUMN X INTEGER')
            self.capture.execute(transaction, 'second-table-alter', f'ALTER TABLE {self.library}.U ADD COLUMN TX_NOTE INTEGER')
            self.capture.execute(transaction, 'label-altered-column', f"LABEL ON COLUMN {self.library}.B.X IS 'same transaction label'")
            self.capture.execute(transaction, 'altered-row', f'INSERT INTO {self.library}.B VALUES (502,7)')
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (802)', 'while-alters-open.log', True)
            self.row_progress(self.inserted('A', 802, 'while-alters-open'), 'while-alters-open')
            self.check('Unrelated rows publish before either schema transaction commits', all(
                (r['value'].get('after') or {}).get('ID') != 502 for r in self.table_records('B', 'uncommitted-schema') if r['value']), 'control/')
            self.command('docker', 'kill', worker)
            worker = self.start_worker('automatic', 'wddl')
            self.lifecycle(worker, 'recovered')
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (803)', 'replacement-alters-open.log', True)
            self.row_progress(self.inserted('A', 803, 'replacement-alters-open'), 'replacement-alters-open')
            self.check('A replacement reconstructs unfinished ALTERs while unrelated CDC continues', True, 'control/')
            self.capture.execute(transaction, 'commit')
            row = self.inserted('B', 502, 'transactional-schema')
            self.row_progress(row, 'transactional-schema')
            self.check('Commitment-control authority changes and two-table nested ALTER preserve publication',
                       row['value']['after'] == {'ID': 502, 'X': 7}, 'control/')
            self.alter(f'ALTER TABLE {self.library}.U DROP COLUMN TX_NOTE', 'restore-excluded-schema')
            self.request(worker, 'automatic', 'PUT', '/stop')
            self.lifecycle(worker, 'drained')
            self.alter(f'ALTER TABLE {self.library}.B ADD COLUMN Y INTEGER', 'consecutive-alter-1')
            self.alter(f'ALTER TABLE {self.library}.B ADD COLUMN Z INTEGER', 'consecutive-alter-2')
            self.request(worker, 'automatic', 'PUT', '/resume')
            self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (503,8,9,10)', 'consecutive-alter-row.log', True)
            row = self.inserted('B', 503, 'consecutive-schema')
            self.row_progress(row, 'consecutive-schema')
            self.check('Consecutive ALTERs without intervening rows use the final definition',
                       row['value']['after'] == {'ID': 503, 'X': 8, 'Y': 9, 'Z': 10}, 'control/')
            # Rollback restores the later ALTER before the earlier clear. Keep
            # its row-discard schema context until both native restorations drain.
            self.capture.execute(transaction, 'rollback-prior-clear', f'TRUNCATE TABLE {self.library}.U REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'rollback-alter', f'ALTER TABLE {self.library}.B ADD COLUMN UNDONE INTEGER')
            self.capture.execute(transaction, 'rollback-row', f'INSERT INTO {self.library}.B VALUES (5999,1,2,3,4)')
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (804)', 'while-rollback-alter-open.log', True)
            self.row_progress(self.inserted('A', 804, 'while-rollback-alter-open'), 'while-rollback-alter-open')
            self.capture.execute(transaction, 'rollback')
            self.capture.execute(transaction, 'rollback-create', f'CREATE TABLE {self.library}.UNDONE (ID INTEGER NOT NULL PRIMARY KEY)')
            self.capture.execute(transaction, 'rollback-create-row', f'INSERT INTO {self.library}.UNDONE VALUES (1)')
            start = self.point()
            self.capture.execute(transaction, 'rollback')
            self.progressed(self.terminal(start, transaction[1], 'RB', 'ddl-rollback'), 'ddl-rollback')
            self.check('Rolled-back schema rows never reach the topic', all(
                (r['value'].get('after') or {}).get('ID') != 5999 for r in self.table_records('B', 'rollback-schema') if r['value']), 'control/')
        finally:
            self.close_session(transaction)
        return worker

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


class OpenObjectsExperiment(public.Experiment):
    """CREATE/DROP stay uncommitted across unrelated output and worker replacement."""

    def configuration(self, enabled):
        config = super().configuration(enabled)
        config.pop('table.include.list')
        config['table.exclude.list'] = self.library + '.U'
        return config

    def assert_absent(self, table, key, label):
        self.check(label, all((r['value'].get('after') or {}).get('ID') != key
                             for r in self.table_records(table, label) if r['value']), 'control/')

    def exercise(self):
        self.capture.deadline = None
        for table in ['N1', 'N2']:
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', f'test.{self.fixture.server_name}.{self.library}.{table}',
                         '--partitions', '1', '--replication-factor', '1')
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        audit_start = self.audit_start = self.point()
        key = 800
        for operation, outcome, table in [('CREATE', 'commit', 'N1'), ('CREATE', 'rollback', 'N2'),
                                           ('DROP', 'rollback', 'N1'), ('DROP', 'commit', 'N1')]:
            label = operation.lower() + '-' + outcome
            transaction = self.capture.connection(transaction=True)
            try:
                own_key = key + 1000
                if operation == 'CREATE':
                    self.capture.execute(transaction, label, f'CREATE TABLE {self.library}.{table} (ID INTEGER NOT NULL PRIMARY KEY)')
                    self.capture.execute(transaction, label + '-row', f'INSERT INTO {self.library}.{table} VALUES ({own_key})')
                    self.capture.execute(transaction, label + '-metadata', f"LABEL ON TABLE {self.library}.{table} IS 'open creation'")
                else:
                    self.capture.execute(transaction, label + '-prior-row', f'INSERT INTO {self.library}.{table} VALUES ({own_key})')
                    self.capture.execute(transaction, label, f'DROP TABLE {self.library}.{table}')
                for replacement in [False, True]:
                    if replacement:
                        self.command('docker', 'kill', worker)
                        worker = self.start_worker('automatic', label + '-replacement')
                        self.lifecycle(worker, 'recovered')
                    key += 1
                    phase = label + ('-replacement' if replacement else '-open')
                    self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', phase + '.log', True)
                    saved = self.row_progress(self.inserted('A', key, phase), phase)
                    self.check(phase + ': unrelated output advances while replay history stays pinned',
                               int(saved['replay_from.sequence']) < int(saved['completed_through.sequence']), 'control/')
                    self.assert_absent(table, own_key, phase + ': uncommitted row remains invisible')
                start = self.point()
                self.capture.execute(transaction, outcome)
                if operation == 'CREATE' and outcome == 'commit':
                    self.fixture.TABLES = [*self.fixture.TABLES, table]
                    self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')
                elif operation == 'DROP' and outcome == 'commit':
                    self.fixture.TABLES = [name for name in self.fixture.TABLES if name != table]
                    self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')
                self.progressed(self.terminal(start, transaction[1], 'CM' if outcome == 'commit' else 'RB', label), label)
                if outcome == 'commit':
                    self.inserted(table, own_key, label + '-committed')
                else:
                    self.assert_absent(table, own_key, label + ': rollback discards row')
                key += 1
                self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-after.log', True)
                self.row_progress(self.inserted('A', key, label + '-after'), label + '-after')
                self.raw_interval(audit_start, label)
            finally:
                self.close_session(transaction)
        self.verify_output({table: self.table_records(table, 'open-object-final-' + table) for table in ['A', 'N1', 'N2']})
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.report.update(status='passed', scope='Packaged task: unrelated CDC during native CREATE/DROP, restart, commit and rollback')
        self.save()

    def verify_output(self, records):
        expected = {'A': [10, *range(801, 813)], 'N1': [1800, 1809], 'N2': []}
        for table, keys in expected.items():
            rows = [r for r in records[table] if r['value'] and r['value']['op'] != 'r']
            self.check(table + ': final output matches the committed writer ledger without duplicates',
                       [r['value']['op'] for r in rows] == ['c'] * len(keys)
                       and [r['key'] for r in rows] == [{'ID': key} for key in keys]
                       and [r['value']['after'] for r in rows] == [{'ID': key} for key in keys], 'control/')


class SchemaTimingExperiment(OpenObjectsExperiment):
    """Compare bounded end-to-end probe latency; this is not a saturation benchmark."""

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        keys = []
        self.report['schema_probe_timings'] = []
        for round_number in range(2):
            cases = [('baseline', None)]
            for table in ['U', 'B']:
                cases += [(f'alter-{table}', f'ALTER TABLE {self.library}.{table} ADD COLUMN P{round_number} INTEGER'),
                          (f'label-{table}', f"LABEL ON TABLE {self.library}.{table} IS 'timing {round_number}'")]
            cases.append(('baseline-after', None))
            for label, statement in cases:
                label = f'{round_number}-{label}'
                ddl_started = time.monotonic()
                if statement:
                    self.fixture.sql(statement, label + '-ddl.log', True)
                ddl_seconds = time.monotonic() - ddl_started
                key = 8100 + len(keys)
                started = time.monotonic()
                self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-row.log', True)
                row = self.inserted('A', key, label + '-output')
                observed = time.monotonic() - started
                self.row_progress(row, label + '-offset')
                self.report['schema_probe_timings'].append({'case': label, 'ddl_seconds': ddl_seconds,
                                                           'row_to_observation_seconds': observed,
                                                           'row_to_durable_seconds': time.monotonic() - started})
                keys.append(key)
                self.save()
        rows = [r for r in self.table_records('A', 'timing-final') if r['value'] and r['value']['op'] != 'r']
        self.check('Every timed probe is published once with the retained key',
                   [r['key'] for r in rows] == [{'ID': key} for key in [10, *keys]]
                   and [r['value']['after']['ID'] for r in rows] == [10, *keys], 'control/timing-final.json')
        self.raw_interval(self.audit_start, 'timing-interval')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, *keys], 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3]})
        self.report.update(status='passed', scope='Packaged task: serial DDL-to-probe latency with exact output and cleanup checks')
        self.save()


class MixedOutcomesExperiment(OpenObjectsExperiment):
    """Catch-up and restart across ALTER/clear rollback, CREATE/ALTER and row undo."""

    def configuration(self, enabled):
        config = super().configuration(enabled)
        config['table.exclude.list'] = ','.join(self.library + '.' + table
                                              for table in ['U', 'LONG_EXCLUDED_NAME', 'LONG_EXCLUDED_RENAMED'])
        return config

    def creation_clear(self, worker):
        import pyodbc
        cases = [('commit', 'N5', 8601, 8801, False), ('rollback', 'N6', 8701, 8901, False),
                 ('commit', 'N7', 9001, 9201, True), ('rollback', 'N8', 9101, 9301, True)]
        for outcome, table, first, probe, abandoned in cases:
            label = 'created-clear-' + table.lower() + '-' + outcome
            transaction = self.capture.connection(transaction=True)
            try:
                self.capture.execute(transaction, label, f'CREATE TABLE {self.library}.{table} (ID INTEGER NOT NULL PRIMARY KEY)')
                # IBM i rejects TRUNCATE after an uncommitted INSERT (SQL0910).
                # An empty newly created table does produce D/CG, with SQL0100.
                # Closing the cursor must not implicitly commit the transaction.
                sql = f'TRUNCATE TABLE {self.library}.{table} REUSE STORAGE IGNORE DELETE TRIGGERS'
                if abandoned:
                    self.capture.execute(transaction, label + '-before', f'INSERT INTO {self.library}.{table} VALUES ({first})')
                    try:
                        self.capture.execute(transaction, label + '-rejected-truncate', sql)
                    except pyodbc.Error as error:
                        self.check(label + ': prior uncommitted INSERT rejects TRUNCATE with SQL0910',
                                   'SQL0910' in str(error) and '(-910)' in str(error), '../native/report.json')
                    else:
                        raise AssertionError('Native uncommitted INSERT/TRUNCATE rejection changed')
                else:
                    with closing(transaction[0].cursor()) as cursor:
                        cursor.execute(sql)
                        messages = [(state, self.fixture.redact(message)) for state, message in cursor.messages]
                    self.report.setdefault('empty_creation_clears', []).append({'sql': sql, 'messages': messages})
                    self.check(label + ': empty-table TRUNCATE reports only the expected no-row warning',
                               bool(messages) and all(state in ['[01000] (100)', '02000'] and 'SQL0100' in message
                                                      for state, message in messages), 'report.json')
                self.capture.execute(transaction, label + '-after', f'INSERT INTO {self.library}.{table} VALUES ({first + 1})')
                for replacement in [False, True]:
                    phase = label + ('-replacement' if replacement else '-open')
                    if replacement:
                        self.command('docker', 'kill', worker)
                        worker = self.start_worker('automatic', phase)
                        self.lifecycle(worker, 'recovered')
                    self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({probe + int(replacement)})', phase + '.log', True)
                    saved = self.row_progress(self.inserted('A', probe + int(replacement), phase), phase)
                    self.check(phase + ': unrelated output advances while the creation transaction remains open',
                               int(saved['replay_from.sequence']) < int(saved['completed_through.sequence']), 'control/')
                    self.check(phase + ': neither the created rows nor truncate are published early',
                               not self.table_records(table, phase + '-pending'), 'control/')
                start = self.point()
                self.capture.execute(transaction, outcome)
                if outcome == 'commit':
                    self.fixture.TABLES = [*self.fixture.TABLES, table]
                else:
                    self.check(label + ': rollback removes the newly created table',
                               self.fixture.sql(f"SELECT TABLE_NAME FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='{self.library}' AND TABLE_NAME='{table}'",
                                                label + '-absent.json') == [], '../native/native/')
                self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')
                self.progressed(self.terminal(start, transaction[1], 'CM' if outcome == 'commit' else 'RB', label), label)
                self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({probe + 2})', label + '-completed.log', True)
                self.row_progress(self.inserted('A', probe + 2, label + '-completed'), label + '-completed')
                journal = self.raw_interval(self.audit_start, label)
                clears = [row for row in journal if row['JOURNAL_CODE'] == 'D' and row['JOURNAL_ENTRY_TYPE'] == 'CG'
                          and row['OBJECT'].startswith(table.ljust(10))]
                self.check(label + ': raw journal distinguishes an actual clear from its abandoned attempt',
                           not clears if abandoned else len(clears) == 1 and bytes.fromhex(clears[0]['DATA_HEX'])[108:109].decode('cp037') == '8',
                           label + '-raw.json')
            finally:
                self.close_session(transaction)
        return worker

    def exercise(self):
        self.capture.deadline = None
        for table in ['N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'LONG_CAPTURED_NAME', 'LONG_RENAMED_NAME', 'LONG_EXCLUDED_NAME', 'LONG_EXCLUDED_RENAMED']:
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', f'test.{self.fixture.server_name}.{self.library}.{table}',
                         '--partitions', '1', '--replication-factor', '1')
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()

        # Hold the reader, not application writers, to make the observation
        # window deterministic: the schema read must see the later rollback.
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'ALTER TABLE {self.library}.U ADD COLUMN X1 INTEGER', 'alter-before-clear.log', True)
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'later-clear', f'TRUNCATE TABLE {self.library}.U REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'rollback')
        finally:
            self.close_session(transaction)
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (7101)', 'after-clear-rollback.log', True)
        self.row_progress(self.inserted('A', 7101, 'after-clear-rollback'), 'after-clear-rollback')
        self.check('ALTER observation accepts a later rolled-back clear without stopping unrelated CDC', True, 'control/')
        self.raw_interval(self.audit_start, 'alter-clear-rollback')

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'CREATE TABLE {self.library}.N1 (ID INTEGER NOT NULL PRIMARY KEY)', 'create-before-alter.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'N1']
        self.fixture.owned_objects = self.fixture.inventory('create-before-alter-owned.json')
        self.fixture.sql(f'ALTER TABLE {self.library}.N1 ADD COLUMN X1 INTEGER', 'alter-created-table.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.N1 VALUES (7201,7)', 'new-layout-row.log', True)
        self.request(worker, 'automatic', 'PUT', '/resume')
        row = self.inserted('N1', 7201, 'created-and-altered')
        self.row_progress(row, 'created-and-altered')
        self.check('CREATE followed by ALTER acquires the final layout and publishes its row correctly',
                   row['key'] == {'ID': 7201} and row['value']['after'] == {'ID': 7201, 'X1': 7}, 'control/')
        self.raw_interval(self.audit_start, 'create-alter')

        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'alter-before-savepoint', f'ALTER TABLE {self.library}.U ADD COLUMN X2 INTEGER')
            self.capture.execute(transaction, 'row-savepoint', 'SAVEPOINT S2 ON ROLLBACK RETAIN CURSORS')
            self.capture.execute(transaction, 'discarded-row', f'INSERT INTO {self.library}.A VALUES (7301)')
            self.capture.execute(transaction, 'row-rollback', 'ROLLBACK TO SAVEPOINT S2')
            self.capture.execute(transaction, 'committed-row', f'INSERT INTO {self.library}.A VALUES (7302)')
            self.capture.execute(transaction, 'commit')
        finally:
            self.close_session(transaction)
        self.row_progress(self.inserted('A', 7302, 'row-savepoint-commit'), 'row-savepoint-commit')
        self.assert_absent('A', 7301, 'Row savepoint rollback discards only its row while the ALTER commits')
        self.raw_interval(self.audit_start, 'alter-row-savepoint')

        for table in ['U', 'B']:
            self.fixture.sql(f"CALL QSYS2.QCMDEXC('CHGPF FILE({self.library}/{table}) SIZE(*NOMAX)')",
                             'change-size-' + table + '.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (7303)', 'after-change-size.log', True)
        self.row_progress(self.inserted('A', 7303, 'after-change-size'), 'after-change-size')
        self.check('Changing captured and excluded physical-file size preserves CDC and the captured layout', True, 'control/')
        self.fixture.sql(f'ALTER TABLE {self.library}.B ADD CONSTRAINT {self.library}.BCHK CHECK (ID > 0)', 'add-captured-check.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (7304)', 'after-add-check.log', True)
        self.row_progress(self.inserted('B', 7304, 'after-add-check'), 'after-add-check')
        self.fixture.sql(f'ALTER TABLE {self.library}.B DROP CONSTRAINT {self.library}.BCHK', 'drop-captured-check.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (7305)', 'after-drop-check.log', True)
        self.row_progress(self.inserted('B', 7305, 'after-drop-check'), 'after-drop-check')

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'CREATE TABLE {self.library}.N2 (ID INTEGER NOT NULL PRIMARY KEY)', 'create-ephemeral.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'N2']
        self.fixture.owned_objects = self.fixture.inventory('create-ephemeral-owned.json')
        self.fixture.sql(f'DROP TABLE {self.library}.N2', 'drop-ephemeral.log', True)
        self.fixture.TABLES.remove('N2')
        self.fixture.owned_objects = self.fixture.inventory('drop-ephemeral-owned.json')
        self.fixture.sql(f'CREATE TABLE {self.library}.N3 (ID INTEGER NOT NULL PRIMARY KEY)', 'create-before-clear.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'N3']
        self.fixture.owned_objects = self.fixture.inventory('create-before-clear-owned.json')
        self.fixture.sql(f'INSERT INTO {self.library}.N3 VALUES (7501), (7502)', 'load-before-clear.log', True)
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'clear-new-table', f'TRUNCATE TABLE {self.library}.N3 REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'commit')
        finally:
            self.close_session(transaction)
        self.fixture.sql(f'INSERT INTO {self.library}.N3 VALUES (7503)', 'reload-new-table.log', True)
        self.fixture.sql(f'CREATE TABLE {self.library}.N4 (ID INTEGER NOT NULL)', 'create-before-key.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'N4']
        self.fixture.owned_objects = self.fixture.inventory('create-before-key-owned.json')
        self.fixture.sql(f'ALTER TABLE {self.library}.N4 ADD CONSTRAINT {self.library}.N4PK PRIMARY KEY (ID)', 'add-created-key.log', True)
        self.fixture.owned_objects = self.fixture.inventory('add-created-key-owned.json')
        self.fixture.sql(f'INSERT INTO {self.library}.N4 VALUES (7601)', 'created-key-row.log', True)
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.row_progress(self.inserted('N3', 7503, 'new-table-cleared'), 'new-table-cleared')
        self.row_progress(self.inserted('N4', 7601, 'new-table-key'), 'new-table-key')
        self.check('An empty CREATE/DROP needs no missing catalog definition', not self.table_records('N2', 'empty-retired-table'), 'control/')
        self.raw_interval(self.audit_start, 'new-table-transitions')
        self.fixture.sql(f'ALTER TABLE {self.library}.N4 DROP CONSTRAINT {self.library}.N4PK', 'drop-retained-key.log', True)
        self.fixture.sql(f"LABEL ON TABLE {self.library}.N4 IS 'retained output key'", 'label-retained-key.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.N4 VALUES (7602)', 'row-with-retained-key.log', True)
        row = self.inserted('N4', 7602, 'retained-key')
        self.row_progress(row, 'retained-key')
        self.check('A constraint-only key removal preserves the already published event key', row['key'] == {'ID': 7602}, 'control/')

        for old, new, physical, renamed, key, captured in [
                ('LONG_CAPTURED_NAME', 'LONG_RENAMED_NAME', 'LCN1', 'LRN1', 7701, True),
                ('LONG_EXCLUDED_NAME', 'LONG_EXCLUDED_RENAMED', 'LEN1', 'LEN2', 7801, False)]:
            self.fixture.sql(f'CREATE TABLE {self.library}.{old} FOR SYSTEM NAME {physical} (ID INTEGER NOT NULL PRIMARY KEY)',
                             'create-' + physical + '.log', True)
            self.fixture.TABLES = [*self.fixture.TABLES, physical]
            self.fixture.owned_objects = self.fixture.inventory('create-' + physical + '-owned.json')
            self.fixture.sql(f'INSERT INTO {self.library}.{old} VALUES ({key})', 'row-' + physical + '.log', True)
            if captured:
                self.row_progress(self.inserted(old, key, 'before-long-rename'), 'before-long-rename')
            else:
                self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (7901)', 'excluded-created-progress.log', True)
                self.row_progress(self.inserted('A', 7901, 'excluded-created-progress'), 'excluded-created-progress')
            self.fixture.sql(f'RENAME TABLE {self.library}.{old} TO {new} FOR SYSTEM NAME {renamed}', 'rename-' + physical + '.log', True)
            self.fixture.TABLES.remove(physical)
            self.fixture.TABLES.append(renamed)
            self.fixture.owned_objects = self.fixture.inventory('rename-' + physical + '-owned.json')
            self.fixture.sql(f'INSERT INTO {self.library}.{new} VALUES ({key + 1})', 'row-' + renamed + '.log', True)
            if captured:
                self.row_progress(self.inserted(new, key + 1, 'after-long-rename'), 'after-long-rename')
            else:
                self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (7902)', 'excluded-renamed-progress.log', True)
                self.row_progress(self.inserted('A', 7902, 'excluded-renamed-progress'), 'excluded-renamed-progress')
                self.check('Excluded long-name creation and rename preserve capture selection',
                           not self.table_records(old, 'excluded-old-name') and not self.table_records(new, 'excluded-new-name'), 'control/')
        self.raw_interval(self.audit_start, 'long-name-renames')

        worker = self.creation_clear(worker)

        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'replacement')
        self.lifecycle(worker, 'recovered')
        self.fixture.sql(f'ALTER TABLE {self.library}.N4 ADD CONSTRAINT {self.library}.N4PK PRIMARY KEY (ID)', 'restore-retained-key.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.N4 VALUES (7603)', 'restarted-retained-key.log', True)
        self.row_progress(self.inserted('N4', 7603, 'restarted-retained-key'), 'restarted-retained-key')
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (7401)', 'after-replacement-a.log', True)
        self.row_progress(self.inserted('A', 7401, 'after-replacement-a'), 'after-replacement-a')
        self.fixture.sql(f'INSERT INTO {self.library}.N1 VALUES (7202,8)', 'after-replacement-n1.log', True)
        self.row_progress(self.inserted('N1', 7202, 'after-replacement-n1'), 'after-replacement-n1')
        self.fixture.sql(f'INSERT INTO {self.library}.LONG_RENAMED_NAME VALUES (7703)', 'after-replacement-long-name.log', True)
        self.row_progress(self.inserted('LONG_RENAMED_NAME', 7703, 'restarted-long-name'), 'restarted-long-name')
        self.verify_output({table: self.table_records(table, 'mixed-outcomes-final-' + table)
                            for table in ['A', 'B', 'N1', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'LONG_CAPTURED_NAME', 'LONG_RENAMED_NAME']})
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 7101, 7302, 7303, 7401, 7901, 7902, 8801, 8802, 8803, 8901, 8902, 8903,
                                           9201, 9202, 9203, 9301, 9302, 9303],
                                  'B': [1, 2, 3, 7304, 7305], 'M': [1, 2, 3], 'U': [1, 2, 3],
                                  'N1': [7201, 7202], 'N3': [7503], 'N4': [7601, 7602, 7603], 'N5': [8602], 'N7': [9001, 9002],
                                  'LONG_RENAMED_NAME': [7701, 7702, 7703],
                                  'LONG_EXCLUDED_RENAMED': [7801, 7802]})
        self.report.update(status='passed', scope='Packaged task: mixed schema/transaction outcomes, durable replay and replacement')
        self.save()

    def verify_output(self, records):
        expected = {'A': [{'ID': key} for key in [10, 7101, 7302, 7303, 7901, 7902, 8801, 8802, 8803, 8901, 8902, 8903,
                                               9201, 9202, 9203, 9301, 9302, 9303, 7401]],
                    'B': [{'ID': 7304}, {'ID': 7305}],
                    'N1': [{'ID': 7201, 'X1': 7}, {'ID': 7202, 'X1': 8}], 'N4': [{'ID': 7601}, {'ID': 7602}, {'ID': 7603}],
                    'LONG_CAPTURED_NAME': [{'ID': 7701}], 'LONG_RENAMED_NAME': [{'ID': 7702}, {'ID': 7703}], 'N6': [],
                    'N7': [{'ID': 9001}, {'ID': 9002}], 'N8': []}
        for table, values in expected.items():
            rows = [r for r in records[table] if r['value'] and r['value']['op'] != 'r']
            self.check(table + ': mixed-outcome output matches the committed ledger without duplicates',
                       [r['value']['op'] for r in rows] == ['c'] * len(values)
                       and [r['key'] for r in rows] == [{'ID': value['ID']} for value in values]
                       and [r['value']['after'] for r in rows] == values, 'control/')
        cleared = records['N3']
        self.check('New-table rows, truncate and reload retain their exact order and keys',
                   [r['value']['op'] for r in cleared] == ['c', 'c', 't', 'c']
                   and [r['key'] for r in cleared] == [{'ID': 7501}, {'ID': 7502}, None, {'ID': 7503}]
                   and [(r['value'].get('after') or {}).get('ID') for r in cleared] == [7501, 7502, None, 7503], 'control/')
        created = records['N5']
        self.check('One committed CREATE/clear transaction publishes truncate before its row',
                   [r['value']['op'] for r in created] == ['t', 'c']
                   and [r['key'] for r in created] == [None, {'ID': 8602}]
                   and [(r['value'].get('after') or {}).get('ID') for r in created] == [None, 8602], 'control/')


class MetadataFollowupsExperiment(OpenObjectsExperiment):
    """Loaded excluded creations and SQL-only renames through the ordinary task."""

    RENAME_CASES = [('LONG_CAPTURED_NAME', 'LONG_RENAMED_NAME', 'SQLN1', True, True),
                    ('OLD_SELECTED_NAME', 'OLD_EXCLUDED_NAME', 'SQLN2', True, False),
                    ('NEW_EXCLUDED_NAME', 'NEW_SELECTED_NAME', 'SQLN3', False, True),
                    ('NEITHER_OLD_NAME', 'NEITHER_NEW_NAME', 'SQLN4', False, False)]

    def configuration(self, enabled):
        config = super().configuration(enabled)
        excluded = ['U', 'XALT', 'XKEY', 'XCMD']
        for old, new, _, old_selected, new_selected in self.RENAME_CASES:
            excluded.extend(name for name, selected in [(old, old_selected), (new, new_selected)] if not selected)
        config['table.exclude.list'] = ','.join(self.library + '.' + name for name in excluded)
        return config

    def exercise(self):
        self.capture.deadline = None
        created_name, renamed_name, altered_name = 'LONG_NEW_FILE', 'LONG_CREATED_RENAMED', 'LONG_ALTERED_RENAMED'
        names = ['XALT', 'XKEY', 'XCMD', 'B_RETIRED_OLD', created_name, renamed_name, altered_name,
                 *[name for old, new, *_ in self.RENAME_CASES for name in [old, new]]]
        for name in names:
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', f'test.{self.fixture.server_name}.{self.library}.{name}',
                         '--partitions', '1', '--replication-factor', '1')
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        probes = []

        def probe(label):
            key = 8101 + len(probes)
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-probe.log', True)
            self.row_progress(self.inserted('A', key, label + '-probe'), label + '-offset')
            probes.append(key)

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        for name, change in [('XALT', 'ALTER TABLE {lib}.XALT ADD COLUMN X1 INTEGER'),
                             ('XKEY', 'ALTER TABLE {lib}.XKEY ADD CONSTRAINT {lib}.XKEYPK PRIMARY KEY (ID)'),
                             ('XCMD', "CALL QSYS2.QCMDEXC('CHGPF FILE({lib}/XCMD) SIZE(*NOMAX)')")]:
            key = '' if name == 'XKEY' else ' PRIMARY KEY'
            self.fixture.sql(f'CREATE TABLE {self.library}.{name} (ID INTEGER NOT NULL{key})', name + '-create.log', True)
            self.fixture.TABLES = [*self.fixture.TABLES, name]
            self.fixture.owned_objects = self.fixture.inventory(name + '-owned.json')
            self.fixture.sql(f'INSERT INTO {self.library}.{name} VALUES (1)', name + '-before.log', True)
            self.fixture.sql(change.format(lib=self.library), name + '-change.log', True)
            values = '2,7' if name == 'XALT' else '2'
            self.fixture.sql(f'INSERT INTO {self.library}.{name} VALUES ({values})', name + '-after.log', True)
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('loaded-creations')
        for name in ['XALT', 'XKEY', 'XCMD']:
            self.check(name + ': loaded excluded creation preserves capture selection across later DDL',
                       not self.table_records(name, name + '-excluded'), 'control/')

        for old, new, physical, old_selected, new_selected in self.RENAME_CASES:
            self.fixture.sql(f'CREATE TABLE {self.library}.{old} FOR SYSTEM NAME {physical} (ID INTEGER NOT NULL PRIMARY KEY)',
                             physical + '-create.log', True)
            # Cleanup checks locks and drops by the unchanged native system name,
            # just as the existing long-name physical-rename experiment does.
            self.fixture.TABLES = [*self.fixture.TABLES, physical]
            self.fixture.owned_objects = self.fixture.inventory(physical + '-created.json')
            self.fixture.sql(f'INSERT INTO {self.library}.{old} VALUES (1)', physical + '-before.log', True)
            probe(physical + '-before')
            self.fixture.sql(f'RENAME TABLE {self.library}.{old} TO {new}', physical + '-rename.log', True)
            self.fixture.owned_objects = self.fixture.inventory(physical + '-renamed.json')
            observed = self.fixture.sql(f"SELECT TRIM(SYSTEM_TABLE_NAME) AS SYSTEM_TABLE_NAME FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='{self.library}' AND TABLE_NAME='{new}'",
                                        physical + '-system-name.json')
            self.check(physical + ': SQL rename retains the native system name', observed == [{'SYSTEM_TABLE_NAME': physical}], '../native/')
            self.fixture.sql(f'INSERT INTO {self.library}.{new} VALUES (2)', physical + '-after.log', True)
            probe(physical + '-after')

        def renamed_inventory(old, new, label):
            self.fixture.TABLES = [new if name == old else name for name in self.fixture.TABLES]
            self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'CREATE TABLE {self.library}.{created_name} FOR SYSTEM NAME RNC1 (ID INTEGER NOT NULL PRIMARY KEY)',
                         'create-before-rename.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'RNC1']
        self.fixture.owned_objects = self.fixture.inventory('create-before-rename-owned.json')
        self.fixture.sql(f'RENAME TABLE {self.library}.{created_name} TO {renamed_name} FOR SYSTEM NAME RNC2',
                         'rename-after-create.log', True)
        renamed_inventory('RNC1', 'RNC2', 'rename-after-create')
        self.fixture.sql(f'INSERT INTO {self.library}.{renamed_name} VALUES (1)', 'row-after-created-rename.log', True)
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('create-then-rename')

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'ALTER TABLE {self.library}.{renamed_name} ADD COLUMN X1 INTEGER', 'alter-before-rename.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.{renamed_name} VALUES (2,7)', 'row-before-altered-rename.log', True)
        self.fixture.sql(f'RENAME TABLE {self.library}.{renamed_name} TO {altered_name} FOR SYSTEM NAME RNC3',
                         'rename-after-alter.log', True)
        renamed_inventory('RNC2', 'RNC3', 'rename-after-alter')
        self.fixture.sql(f'INSERT INTO {self.library}.{altered_name} VALUES (3,7)', 'row-after-altered-rename.log', True)
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('alter-then-rename')

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.sql(f'RENAME TABLE {self.library}.B TO B_RETIRED_OLD FOR SYSTEM NAME BOLD', 'swap-retire-name.log', True)
        renamed_inventory('B', 'BOLD', 'swap-retire-name')
        self.fixture.sql(f'CREATE TABLE {self.library}.B (ID INTEGER NOT NULL PRIMARY KEY)', 'swap-create-replacement.log', True)
        self.fixture.TABLES = [*self.fixture.TABLES, 'B']
        self.fixture.owned_objects = self.fixture.inventory('swap-replacement-owned.json')
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (9001)', 'swap-replacement-row.log', True)
        self.fixture.sql(f'DROP TABLE {self.library}.B_RETIRED_OLD RESTRICT', 'swap-drop-old.log', True)
        self.fixture.TABLES.remove('BOLD')
        self.fixture.owned_objects = self.fixture.inventory('swap-old-dropped-owned.json')
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('rename-replace-drop')
        self.raw_interval(self.audit_start, 'metadata-followups')

        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'replacement')
        self.lifecycle(worker, 'recovered')
        for _, new, physical, _, _ in self.RENAME_CASES:
            self.fixture.sql(f'INSERT INTO {self.library}.{new} VALUES (3)', physical + '-restart.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.{altered_name} VALUES (4,7)', 'altered-rename-restart.log', True)
        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (9002)', 'swap-replacement-restart.log', True)
        probe('after-replacement')
        for old, new, physical, old_selected, new_selected in self.RENAME_CASES:
            for name, expected in [(old, [1] if old_selected else []), (new, [2, 3] if new_selected else [])]:
                rows = self.table_records(name, 'final-' + name)
                self.check(name + ': exact keys, values and selection survive SQL rename and replacement',
                           [row['key'] for row in rows] == [{'ID': key} for key in expected]
                           and [row['value']['op'] for row in rows] == ['c'] * len(expected)
                           and [row['value']['after'] for row in rows] == [{'ID': key} for key in expected], 'control/')
        for name, expected in [(created_name, []), (renamed_name, [{'ID': 1}, {'ID': 2, 'X1': 7}]),
                               (altered_name, [{'ID': 3, 'X1': 7}, {'ID': 4, 'X1': 7}])]:
            rows = self.table_records(name, 'final-' + name)
            self.check(name + ': creation/ALTER and physical rename retain their separate row and topic boundaries',
                       [row['key'] for row in rows] == [{'ID': row['ID']} for row in expected]
                       and [row['value']['op'] for row in rows] == ['c'] * len(expected)
                       and [row['value']['after'] for row in rows] == expected, 'control/')
        rows = [row for row in self.table_records('B', 'final-replacement') if row['value']['op'] != 'r']
        self.check('Dropping the renamed old object preserves the replacement binding and does not invent a truncate',
                   [row['key'] for row in rows] == [{'ID': 9001}, {'ID': 9002}]
                   and [row['value']['op'] for row in rows] == ['c', 'c']
                   and [row['value']['after'] for row in rows] == [{'ID': 9001}, {'ID': 9002}]
                   and not self.table_records('B_RETIRED_OLD', 'final-retired'), 'control/')
        rows = [row for row in self.table_records('A', 'final-probes') if row['value']['op'] != 'r']
        self.check('Unrelated CDC progresses exactly once through every metadata follow-up',
                   [row['key'] for row in rows] == [{'ID': key} for key in [10, *probes]], 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        expected = {'A': [1, 2, 3, 10, *probes], 'B': [9001, 9002], 'M': [1, 2, 3], 'U': [1, 2, 3],
                    'XALT': [1, 2], 'XKEY': [1, 2], 'XCMD': [1, 2],
                    altered_name: [1, 2, 3, 4],
                    **{new: [1, 2, 3] for _, new, *_ in self.RENAME_CASES}}
        self.fixture.verify_rows(expected)
        self.report.update(status='passed', scope='Packaged task: loaded excluded creations, SQL-only rename selection and fresh-task recovery')
        self.save()


class RetiredDefinitionsExperiment(public.Experiment):
    """Known ALTER/DROP and an empty CREATE/DROP racing catalog acquisition."""

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        start = self.point()
        probes = []

        def probe(label):
            key = 8501 + len(probes)
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-probe.log', True)
            self.row_progress(self.inserted('A', key, label + '-probe'), label + '-offset')
            probes.append(key)

        def drop(table, label):
            self.fixture.sql(f'DROP TABLE {self.library}.{table} RESTRICT', label + '-drop.log', True)
            self.fixture.TABLES = [name for name in self.fixture.TABLES if name != table]
            self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')

        self.fixture.sql(f'INSERT INTO {self.library}.B VALUES (301)', 'before-drop-row.log', True)
        self.row_progress(self.inserted('B', 301, 'before-drop-row'), 'before-drop-row')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        for table in ['U', 'B']:
            self.fixture.sql(f'ALTER TABLE {self.library}.{table} ADD COLUMN X1 INTEGER', table + '-alter.log', True)
            drop(table, table)
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('alter-then-drop')

        self.report['empty_creation_gaps'] = []
        for number, gap in enumerate([0.5, 1.5, 3.0, 4.0, 6.0, 8.0]):
            table = 'W' + str(number)
            label = 'empty-' + table
            opened = self.point()
            self.fixture.sql(f'CREATE TABLE {self.library}.{table} (ID INTEGER NOT NULL PRIMARY KEY)', label + '-create.log', True)
            # Start the gap at the completion of CREATE. Inventory remains outside
            # it so the requested timings are not dominated by metadata queries.
            created = time.monotonic()
            self.fixture.TABLES = [*self.fixture.TABLES, table]
            self.fixture.owned_objects = self.fixture.inventory(label + '-created-owned.json')
            time.sleep(max(0, gap - (time.monotonic() - created)))
            elapsed = time.monotonic() - created
            drop(table, label)
            probe(label)
            self.raw_interval(opened, label)
            self.report['empty_creation_gaps'].append({'table': table, 'requested_seconds': gap, 'actual_seconds': elapsed})
            self.save()
        self.raw_interval(start, 'retired-definitions')
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'replacement')
        self.lifecycle(worker, 'recovered')
        probe('after-replacement')
        rows = [r for r in self.table_records('A', 'final-probes') if r['value']['op'] != 'r']
        self.check('Unrelated output is exact across retirement, catalog races and replacement',
                   [r['key'] for r in rows] == [{'ID': key} for key in [10, *probes]]
                   and [r['value']['op'] for r in rows] == ['c'] * (len(probes) + 1), 'control/')
        rows = [r for r in self.table_records('B', 'final-retired') if r['value']['op'] != 'r']
        self.check('Retiring a captured binding preserves its published row and does not invent a truncate',
                   [r['key'] for r in rows] == [{'ID': 301}]
                   and [r['value']['op'] for r in rows] == ['c']
                   and [r['value']['after'] for r in rows] == [{'ID': 301}], 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.report['catalog_retries_observed'] = sum(path.read_text().count('Re-observing schema change at ')
                                                      for path in self.logs.values())
        self.fixture.verify_rows({'A': [1, 2, 3, 10, *probes], 'M': [1, 2, 3]})
        self.report.update(status='passed', scope='Packaged task: known ALTER/DROP, empty CREATE/DROP timings and durable restart')
        self.save()


class TransientTablesExperiment(public.Experiment):
    """Deleted excluded work tables use retained native names, never today's catalog."""

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        start = self.point()
        probes = []
        for number, kind in enumerate(['ordinary', 'ctas', 'transaction', 'sql-rename', 'quoted']):
            self.request(worker, 'automatic', 'PUT', '/stop')
            self.lifecycle(worker, 'drained')
            label = 'transient-' + kind
            table = 'WORK' + str(number)
            sqlname = 'VERY_LONG_TRANSIENT' if kind == 'sql-rename' else '\"MixedCaseTransient\"' if kind == 'quoted' else table
            transaction = self.capture.connection(transaction=kind == 'transaction')
            try:
                native = '' if sqlname == table else ' FOR SYSTEM NAME ' + table
                definition = 'AS (SELECT ID FROM ' + self.library + '.A) WITH DATA' if kind == 'ctas' else '(ID INTEGER NOT NULL PRIMARY KEY)'
                self.capture.execute(transaction, label + '-create', f'CREATE TABLE {self.library}.{sqlname}{native} {definition}')
                if kind != 'ctas':
                    self.capture.execute(transaction, label + '-load', f'INSERT INTO {self.library}.{sqlname} VALUES (501),(502)')
                self.fixture.TABLES = [*self.fixture.TABLES, table]
                if kind != 'transaction':
                    self.fixture.owned_objects = self.fixture.inventory(label + '-created.json')
                if kind == 'sql-rename':
                    self.capture.execute(transaction, label + '-label', f"LABEL ON TABLE {self.library}.{sqlname} IS 'work table'")
                    self.capture.execute(transaction, label + '-rename', f'RENAME TABLE {self.library}.{sqlname} TO VERY_LONG_RENAMED')
                    sqlname = 'VERY_LONG_RENAMED'
                    self.capture.execute(transaction, label + '-after-rename', f'INSERT INTO {self.library}.{sqlname} VALUES (503)')
                self.capture.execute(transaction, label + '-drop', f'DROP TABLE {self.library}.{sqlname} RESTRICT')
                if kind == 'transaction':
                    self.capture.execute(transaction, 'commit')
                self.fixture.TABLES.remove(table)
                self.fixture.owned_objects = self.fixture.inventory(label + '-dropped.json')
            finally:
                self.close_session(transaction)
            self.fixture.rotate()
            key = 8701 + len(probes)
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-probe.log', True)
            self.request(worker, 'automatic', 'PUT', '/resume')
            self.row_progress(self.inserted('A', key, label), label)
            probes.append(key)
            self.check(label + ': unrelated output advances past a vanished excluded table', True, 'control/')
            self.save()
        self.raw_interval(start, 'transient-lifetimes')
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'replacement')
        self.lifecycle(worker, 'recovered')
        key = 8701 + len(probes)
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', 'replacement-probe.log', True)
        self.row_progress(self.inserted('A', key, 'replacement-probe'), 'replacement-probe')
        probes.append(key)
        rows = [r for r in self.table_records('A', 'final-probes') if r['value']['op'] != 'r']
        self.check('Unrelated rows are exact across vanished tables and worker replacement',
                   [r['key'] for r in rows] == [{'ID': key} for key in [10, *probes]]
                   and [r['value']['op'] for r in rows] == ['c'] * (len(probes) + 1), 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, *probes], 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3]})
        self.report.update(status='passed', scope='Packaged task: excluded CREATE/load/DROP, CTAS, transaction, SQL-name changes, detached receivers and restart')
        self.save()


class SchemaObservationExperiment(public.Experiment):
    """Committed/restored definitions and DDL racing the catalog observation."""

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        start = self.point()
        probes = []

        def probe(label):
            key = 8801 + len(probes)
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', label + '-probe.log', True)
            self.row_progress(self.inserted('A', key, label), label)
            probes.append(key)

        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        for table in ['U', 'B']:
            self.fixture.sql(f'ALTER TABLE {self.library}.{table} ADD COLUMN KEPT INTEGER DEFAULT 7', table + '-committed.log', True)
            transaction = self.capture.connection(transaction=True)
            try:
                self.capture.execute(transaction, table + '-alter', f'ALTER TABLE {self.library}.{table} ADD COLUMN UNDONE INTEGER')
                self.capture.execute(transaction, 'rollback')
            finally:
                self.close_session(transaction)
        self.request(worker, 'automatic', 'PUT', '/resume')
        probe('committed-then-restored')
        self.fixture.sql(f'INSERT INTO {self.library}.B (ID) VALUES (501)', 'restored-layout.log', True)
        self.row_progress(self.inserted('B', 501, 'restored-layout'), 'restored-layout')
        self.check('A later rolled-back ALTER preserves the earlier committed captured layout',
                   next(r for r in self.table_records('B', 'restored-row') if r['value'] and
                        (r['value'].get('after') or {}).get('ID') == 501)['value']['after'] == {'ID': 501, 'KEPT': 7}, 'control/')

        self.report['catalog_observation_gaps'] = []
        for number, (kind, gap) in enumerate([('label', 3), ('label', 5), ('constraint', 4), ('alter', 4), ('rename', 4)]):
            table = 'RACE' + str(number)
            label = 'catalog-' + kind + '-' + str(number)
            self.fixture.sql(f'CREATE TABLE {self.library}.{table} (ID INTEGER NOT NULL PRIMARY KEY)', label + '-create.log', True)
            created = time.monotonic()
            self.fixture.TABLES = [*self.fixture.TABLES, table]
            self.fixture.owned_objects = self.fixture.inventory(label + '-owned.json')
            time.sleep(max(0, gap - (time.monotonic() - created)))
            elapsed = time.monotonic() - created
            if kind == 'label':
                statement = f"LABEL ON TABLE {self.library}.{table} IS 'catalog race'"
            elif kind == 'constraint':
                statement = f'ALTER TABLE {self.library}.{table} ADD CONSTRAINT {self.library}.C{number} CHECK (ID > 0)'
            elif kind == 'alter':
                statement = f'ALTER TABLE {self.library}.{table} ADD COLUMN NOTE INTEGER'
            else:
                statement = f'RENAME TABLE {self.library}.{table} TO RENAMED'
            self.fixture.sql(statement, label + '-change.log', True)
            if kind == 'rename':
                self.fixture.TABLES.remove(table)
                self.fixture.TABLES.append('RENAMED')
                self.fixture.owned_objects = self.fixture.inventory(label + '-renamed-owned.json')
            probe(label)
            self.report['catalog_observation_gaps'].append({'kind': kind, 'requested_seconds': gap, 'actual_seconds': elapsed})
            self.save()

        self.raw_interval(start, 'schema-observations')
        self.command('docker', 'kill', worker)
        worker = self.start_worker('automatic', 'replacement')
        self.lifecycle(worker, 'recovered')
        probe('after-replacement')
        self.fixture.sql(f'INSERT INTO {self.library}.B (ID) VALUES (502)', 'replacement-layout.log', True)
        self.row_progress(self.inserted('B', 502, 'replacement-layout'), 'replacement-layout')
        rows = [r for r in self.table_records('B', 'final-layout') if r['value']['op'] != 'r']
        self.check('Restored captured layout and its rows survive fresh-task recovery exactly',
                   [r['value']['after'] for r in rows] == [{'ID': 501, 'KEPT': 7}, {'ID': 502, 'KEPT': 7}], 'control/')
        rows = [r for r in self.table_records('A', 'final-probes') if r['value']['op'] != 'r']
        self.check('Unrelated CDC is exact across catalog changes and replacement',
                   [r['key'] for r in rows] == [{'ID': key} for key in [10, *probes]], 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.report['metadata_retries_observed'] = sum(path.read_text().count('qualified transition overlapped catalog acquisition')
                                                       for path in self.logs.values())
        self.report['missing_catalog_retries_observed'] = sum(path.read_text().count('retained journal evidence explains a missing catalog')
                                                              for path in self.logs.values())
        self.fixture.verify_rows({'A': [1, 2, 3, 10, *probes], 'B': [1, 2, 3, 501, 502], 'M': [1, 2, 3], 'U': [1, 2, 3],
                                  **{'RACE' + str(n): [] for n in range(4)}, 'RENAMED': []})
        self.report.update(status='passed', scope='Packaged task: committed ALTER then restored ALTER, moving catalog and durable restart')
        self.save()


class OpenLaterAlterExperiment(public.Experiment):
    """An excluded committed ALTER progresses despite a second open ALTER of that file."""

    assert_absent = OpenObjectsExperiment.assert_absent

    def configuration(self, enabled):
        result = super().configuration(enabled)
        if getattr(self, 'include_u', False):
            result['table.include.list'] += ',' + self.library + '.U'
        return result

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                     '--create', '--topic', f'test.{self.fixture.server_name}.{self.library}.U', '--partitions', '1', '--replication-factor', '1')
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        expected = [10]
        next_key = 9000
        for number, outcome in enumerate(['rollback', 'commit']):
            label = 'second-alter-' + outcome
            self.request(worker, 'automatic', 'PUT', '/stop')
            self.lifecycle(worker, 'drained')
            self.include_u = False
            self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
            self.fixture.sql(f'ALTER TABLE {self.library}.U ADD COLUMN KEPT{number} INTEGER DEFAULT 7', label + '-committed.log', True)
            transaction = self.capture.connection(transaction=True)
            own_key = 9400 + number
            try:
                self.capture.execute(transaction, label + '-open', f'ALTER TABLE {self.library}.U ADD COLUMN LATER{number} INTEGER')
                self.capture.execute(transaction, label + '-excluded-row', f'INSERT INTO {self.library}.U (ID) VALUES ({9600 + number})')
                self.capture.execute(transaction, label + '-captured-row', f'INSERT INTO {self.library}.A VALUES ({own_key})')
                self.request(worker, 'automatic', 'PUT', '/resume')
                for replacement in [False, True]:
                    if replacement:
                        self.command('docker', 'kill', worker)
                        worker = self.start_worker('automatic', label + '-replacement')
                        self.lifecycle(worker, 'recovered')
                    next_key += 1
                    phase = label + ('-replacement' if replacement else '-open')
                    self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({next_key})', phase + '-probe.log', True)
                    saved = self.row_progress(self.inserted('A', next_key, phase), phase)
                    expected.append(next_key)
                    self.check(phase + ': unrelated output advances with the second ALTER open and F retained',
                               int(saved['replay_from.sequence']) < int(saved['completed_through.sequence']), 'control/')
                    self.assert_absent('A', own_key, phase + ': transaction row remains invisible')
                self.capture.execute(transaction, outcome)
                if outcome == 'commit':
                    self.row_progress(self.inserted('A', own_key, label + '-committed-row'), label + '-committed-row')
                    expected.append(own_key)
                else:
                    self.assert_absent('A', own_key, label + ': rolled-back row remains absent')
            finally:
                self.close_session(transaction)
            next_key += 1
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({next_key})', label + '-closed-probe.log', True)
            self.row_progress(self.inserted('A', next_key, label + '-closed'), label + '-closed')
            expected.append(next_key)
            self.request(worker, 'automatic', 'PUT', '/stop')
            public.base.wait_for('task stopped before selection expansion',
                                 lambda: self.rest(worker, '/connectors/automatic/status')['connector']['state'] == 'STOPPED', 60)
            self.include_u = True
            self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
            self.request(worker, 'automatic', 'PUT', '/resume')
            self.fixture.sql(f'INSERT INTO {self.library}.U (ID) VALUES ({9800 + number})', label + '-selected-row.log', True)
            selected = self.inserted('U', 9800 + number, label + '-selected-row')
            self.row_progress(selected, label + '-selected-row')
            wanted = {'ID': 9800 + number, 'KEPT0': 7}
            if number == 1:
                wanted.update(KEPT1=7, LATER1=None)
            self.check(label + ': selection expands after the outcome without LABEL or resnapshot',
                       selected['value']['op'] == 'c' and selected['value']['after'] == wanted, 'control/')
        self.raw_interval(self.audit_start, 'two-alter-outcomes')
        rows = [r for r in self.table_records('A', 'final-probes') if r['value']['op'] != 'r']
        self.check('Commit, rollback and both replacements preserve the exact captured-row ledger',
                   [r['key'] for r in rows] == [{'ID': key} for key in expected]
                   and [r['value']['op'] for r in rows] == ['c'] * len(expected), 'control/')
        selected_rows = self.table_records('U', 'final-expanded-selection')
        self.check('Selection expansion publishes only the two new rows, without snapshot or replay',
                   [r['key'] for r in selected_rows] == [{'ID': 9800}, {'ID': 9801}]
                   and all(r['value']['op'] == 'c' for r in selected_rows), 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': sorted([1, 2, 3, *expected]), 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3, 9601, 9800, 9801]})
        self.report.update(status='passed', scope='Packaged task: committed excluded ALTER, later open ALTER, unrelated progress, replacement and both outcomes')
        self.save()


if __name__ == '__main__':
    objects = len(sys.argv) > 1 and sys.argv[1] == '--open-objects'
    mixed = len(sys.argv) > 1 and sys.argv[1] == '--mixed-outcomes'
    timing = len(sys.argv) > 1 and sys.argv[1] == '--schema-timing'
    followups = len(sys.argv) > 1 and sys.argv[1] == '--metadata-followups'
    retired = len(sys.argv) > 1 and sys.argv[1] == '--retired-definitions'
    transient = len(sys.argv) > 1 and sys.argv[1] == '--transient-tables'
    observation = len(sys.argv) > 1 and sys.argv[1] == '--schema-observation'
    later_alter = len(sys.argv) > 1 and sys.argv[1] == '--open-later-alter'
    if objects or mixed or timing or followups or retired or transient or observation or later_alter:
        del sys.argv[1]
    public.main(OpenLaterAlterExperiment if later_alter else SchemaObservationExperiment if observation else TransientTablesExperiment if transient else RetiredDefinitionsExperiment if retired else MetadataFollowupsExperiment if followups else SchemaTimingExperiment if timing else MixedOutcomesExperiment if mixed else OpenObjectsExperiment if objects else Experiment)
