#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Public connector discovery of a new native table, then replay and truncate.

Uses only the shared harness's fresh owned library and verified cleanup. The new
data topic is deliberately absent; ordinary Connect/broker topic creation owns it.
"""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('public_creation', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Experiment(public.Experiment):
    def configuration(self, enabled):
        config = super().configuration(enabled)
        config.pop('table.include.list')
        config['table.exclude.list'] = self.library + '.U'
        config['database.dbname'] = ''  # Ordinary CURRENT_SERVER discovery, also used by native handoff.
        return config

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker('automatic', 'w1')
        self.setup_connector(worker)
        self.request(worker, 'automatic', 'PUT', '/config', self.configuration(True))
        self.request(worker, 'automatic', 'PUT', '/resume')
        self.lifecycle(worker, 'handoff-complete')
        self.audit_start = self.point()
        unrelated = self.capture.connection(transaction=True)
        try:
            self.capture.execute(unrelated, 'hold-unrelated-cycle', f'INSERT INTO {self.library}.U VALUES (900)')
            self.fixture.sql(f'CREATE TABLE {self.library}.N (ID INTEGER NOT NULL PRIMARY KEY, NOTE VARCHAR(20))',
                             'create-new-table.json', True)
            self.fixture.TABLES = [*self.fixture.TABLES, 'N']
            self.fixture.owned_objects = self.fixture.inventory('objects-new-table.json')
            self.fixture.sql(f"INSERT INTO {self.library}.N VALUES (700,'new')", 'new-table-row.json', True)
            first = self.inserted('N', 700, 'new-table-row')
            saved = self.row_progress(first, 'new-table-row')
            self.check('A newly created table publishes its native key and values without a task restart or snapshot',
                       first['key'] == {'ID': 700} and first['value']['after'] == {'ID': 700, 'NOTE': 'new'}
                       and first['value']['op'] == 'c', 'control/')
            self.check('New-table output advances W while an unrelated transaction retains F before creation',
                       int(saved['replay_from.sequence']) < int(saved['completed_through.sequence'])
                       and saved['schema.floor.ordinal'] == '0' and saved['schema.completed.ordinal'] == '1', 'control/')
            history = self.records(self.observer_container, 'bindings-automatic', 'creation-history-before-restart')
            self.command('docker', 'kill', worker)
            worker = self.start_worker('automatic', 'w2')
            self.lifecycle(worker, 'recovered')
            self.fixture.sql(f"INSERT INTO {self.library}.N VALUES (701,'restart')", 'new-table-restart.json', True)
            self.row_progress(self.inserted('N', 701, 'new-table-restart'), 'new-table-restart')
            self.check('Fresh-task replay uses the saved creation history without appending another definition',
                       self.records(self.observer_container, 'bindings-automatic', 'creation-history-after-restart') == history,
                       'control/')
            rows = self.table_records('N', 'new-table-after-restart')
            self.check('Replaying creation does not repeat already completed rows',
                       [r['value']['after']['ID'] for r in rows if r['value']] == [700, 701], 'control/')
            start = self.point()
            self.capture.execute(unrelated, 'rollback')
            self.progressed(self.terminal(start, unrelated[1], 'RB', 'creation-unrelated-rollback'), 'creation-unrelated-rollback')
        finally:
            self.close_session(unrelated)
        transaction = self.capture.connection(transaction=True)
        try:
            self.capture.execute(transaction, 'created-table-clear', f'TRUNCATE TABLE {self.library}.N REUSE STORAGE IGNORE DELETE TRIGGERS')
            self.capture.execute(transaction, 'created-table-reload', f"INSERT INTO {self.library}.N VALUES (800,'reload')")
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (500)', 'created-unrelated-progress.json', True)
            self.row_progress(self.inserted('A', 500, 'created-unrelated-progress'), 'created-unrelated-progress')
            start = self.point()
            self.capture.execute(transaction, 'commit')
            self.progressed(self.terminal(start, transaction[1], 'CM', 'created-table-commit'), 'created-table-commit')
        finally:
            self.close_session(transaction)
        rows = self.table_records('N', 'created-table-clear')
        self.check('A transactional clear of the newly discovered table precedes its reload',
                   [r['value']['op'] for r in rows[-2:] if r['value']] == ['t', 'c']
                   and rows[-1]['value']['after'] == {'ID': 800, 'NOTE': 'reload'}, 'control/')
        self.raw_interval(self.audit_start, 'creation-and-clear')
        self.request(worker, 'automatic', 'PUT', '/stop')
        self.lifecycle(worker, 'drained')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 500], 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3], 'N': [800]})
        self.report.update(status='passed', scope='Public native table discovery, automatic data topic creation, restart and truncate')
        self.save()


if __name__ == '__main__':
    public.main(Experiment)
