#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded native transaction and memory soak using the packaged public connector."""
import hashlib
import importlib.util
from pathlib import Path
import time

SPEC = importlib.util.spec_from_file_location('performance', Path(__file__).with_name('dcg-performance-probe.py'))
performance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(performance)


class Experiment(performance.Experiment):
    def stage(self):
        if self.args.mode != 'transactional':
            raise ValueError('Transaction soak requires transactional mode')
        super().stage()
        self.report['harness_sha256']['tools/dcg-soak-probe.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.report['soak'] = {'requested_seconds': self.args.duration, 'large_transaction_rows': 5000,
                               'held_clear_minimum_seconds': 60, 'maximum_rows': 150000,
                               'offered_load': 'Serial mixed-column bursts; not a saturation benchmark'}
        self.save()

    def observer_arguments(self):
        return ['soak']

    def measured_rows(self, session, table, first, count, scenario, round_number):
        started = time.time_ns() / 1e6
        self.write_rows(session, table, first, count, 'c')
        finished = time.time_ns() / 1e6
        rows = self.await_rows(table, first, count, 'c')
        self.record(rows, started, finished, scenario, round_number, warmup=False)
        return rows

    def large_transaction(self, writer, outcome, first):
        transaction = self.capture.connection(transaction=True)
        try:
            for i in range(0, 5000, 500):
                self.write_rows(transaction, 'M', first+i, 500, 'c')
            self.check('Large transaction stays unpublished before '+outcome,
                       not self.matching('M', first, 5000, 'c'), 'control/performance-events.jsonl')
            started = time.time_ns() / 1e6
            self.capture.execute(transaction, outcome)
            finished = time.time_ns() / 1e6
            if outcome == 'commit':
                rows = self.await_rows('M', first, 5000, 'c')
                self.record(rows, started, finished, 'large-commit', 0, warmup=False,
                            timing='Commit request to last visible record; transaction loading excluded')
                self.row_progress(rows[-1], 'large-commit')
            else:
                rows = self.measured_rows(writer, 'B', first, 100, 'rollback-progress', 0)
                self.row_progress(rows[-1], 'large-rollback')
                self.check('Large rollback publishes no discarded rows', not self.matching('M', first, 5000, 'c'),
                           'control/performance-events.jsonl')
        finally:
            self.close_session(transaction)

    def held_clear(self, writer):
        transaction = self.capture.connection(transaction=True)
        before_count = len([r for r in self.new_events() if r['topic'].endswith('.A')])
        started = time.monotonic()
        try:
            self.capture.execute(transaction, 'held-truncate',
                                 f'TRUNCATE TABLE {self.library}.A REUSE STORAGE IGNORE DELETE TRIGGERS')
            for i in range(0, 1000, 250):
                self.write_rows(transaction, 'A', 6000000+i, 250, 'c')
            round_number = 0
            held = None
            while time.monotonic()-started < 60:
                rows = self.measured_rows(writer, 'B', 7000000+round_number*100, 100, 'unrelated-during-clear', round_number)
                self.check('Unrelated burst progresses within 15 seconds while clear is open',
                           self.measurements[-1]['elapsed_ms'] < 15000, 'report.json')
                self.check('Open clear and reload remain invisible',
                           len([r for r in self.new_events() if r['topic'].endswith('.A')]) == before_count,
                           'control/performance-events.jsonl')
                if held is None:
                    held = self.row_progress(rows[-1], 'held-clear-progress')
                    self.check('Durable replay floor stays behind completed unrelated output',
                               int(held['replay_from.sequence']) < int(held['completed_through.sequence']), 'control/')
                round_number += 1
                if round_number > 60:
                    raise RuntimeError('Held-clear workload bound exceeded')
            held_seconds = time.monotonic()-started
            commit_started = time.time_ns() / 1e6
            self.capture.execute(transaction, 'commit')
            commit_finished = time.time_ns() / 1e6
            rows = self.await_rows('A', 6000000, 1000, 'c')
            clears = [r for r in self.new_events() if r['topic'].endswith('.A') and r['value']['op']=='t']
            self.check('Committed clear precedes all reload rows',
                       len(clears)==1 and clears[0]['key'] is None and clears[0]['offset'] < min(r['offset'] for r in rows),
                       'control/performance-events.jsonl')
            self.additional_events = 1
            self.record(rows, commit_started, commit_finished, 'held-clear-commit', 0, warmup=False,
                        timing='Commit request to last reload row; open interval excluded')
            completed = self.row_progress(rows[-1], 'held-clear-completed')
            self.check('Committed clear releases the protected replay floor',
                       int(completed['replay_from.sequence']) > int(held['replay_from.sequence']), 'control/')
            self.report['soak'].update(held_clear_seconds=held_seconds, unrelated_bursts=round_number,
                                       held_checkpoint=held, completed_checkpoint=completed)
        finally:
            self.close_session(transaction)

    def extended_workload(self, worker, writer):
        self.large_transaction(writer, 'commit', 5000000)
        self.large_transaction(writer, 'rollback', 5100000)
        self.held_clear(writer)
        started = time.monotonic()
        self.report['soak']['started_ms'] = time.time_ns()/1e6
        round_number = 0
        restarted = False
        while time.monotonic()-started < self.args.duration:
            first = 8000000+round_number*1000
            self.burst(writer, 'A', first, 'c', 1000+round_number)
            self.burst(writer, 'M', first, 'c', 1000+round_number)
            self.burst(writer, 'M', first, 'u', 1000+round_number)
            if not restarted and time.monotonic()-started >= self.args.duration/2:
                self.stop_connector(worker)
                saved = self.checkpoint(self.observer_container, 'automatic', 'soak-before-restart')
                history = self.records(self.observer_container, 'bindings-automatic', 'soak-history-before')
                self.request(worker, 'automatic', 'PUT', '/resume')
                rows = self.measured_rows(writer, 'B', 7500000, 100, 'soak-restart-progress', 0)
                recovered = self.row_progress(rows[-1], 'soak-recovered')
                self.check('Soak restart preserves history and durable F/W progress',
                           history == self.records(self.observer_container, 'bindings-automatic', 'soak-history-after')
                           and int(recovered['completed_through.sequence']) >= int(saved['completed_through.sequence'])
                           and int(recovered['replay_from.sequence']) >= int(saved['replay_from.sequence']), 'control/')
                self.report['soak']['restart'] = {'before': saved, 'after': recovered}
                restarted = True
            round_number += 1
            if sum(m['rows'] for m in self.measurements)+getattr(self,'additional_events',0) > 145000:
                raise RuntimeError('Soak record bound exceeded')
        self.report['soak'].update(finished_ms=time.time_ns()/1e6, elapsed_seconds=time.monotonic()-started,
                                   rounds=round_number, task_restarted=restarted)
        self.check('Sustained workload ran for the requested duration with a real task restart',
                   restarted and time.monotonic()-started >= self.args.duration, 'report.json')
        self.save()


def configure(parser):
    parser.set_defaults(mode='transactional')
    parser.add_argument('--duration', type=int, choices=[600,900,1200], default=900)


if __name__ == '__main__':
    performance.main(Experiment, configure)
