#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded continuous native load; reuse the exact packaged performance runner.

Two warm-up rounds precede independent, paced writes. The writer never waits for
CDC. This measures lag and resources under the offered load, not maximum capacity.
"""
import importlib.util
import hashlib
import json
from pathlib import Path
import statistics
import sys
import threading
import time

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('performance', Path(__file__).with_name('dcg-performance-probe.py'))
performance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(performance)


def latency_summary(batches, events):
    """Match independent observed records to exact submitted operations."""
    expected = {}
    for batch in batches:
        for key in range(batch['first'], batch['first'] + batch['rows']):
            identity = (batch['table'], key, batch['operation'])
            if identity in expected:
                raise ValueError('Overlapping submitted operation')
            expected[identity] = batch['started_ms']
    latencies = []
    seen = set()
    for event in events:
        if event['value'] is None:
            continue
        identity = (event['topic'].rsplit('.', 1)[-1], event['key']['ID'], event['value']['op'])
        if identity not in expected:
            continue
        if identity in seen:
            raise ValueError('Duplicate sustained output')
        seen.add(identity)
        latency = event['observed_ms'] - expected[identity]
        if latency < 0:
            raise ValueError('Observation precedes source submission')
        latencies.append(latency)
    if seen != expected.keys():
        raise ValueError('Missing sustained output')
    return {'rows': len(latencies), 'p50_ms': statistics.median(latencies),
            'p95_ms': performance.percentile(latencies, .95), 'p99_ms': performance.percentile(latencies, .99),
            'max_ms': max(latencies)}


class Experiment(performance.Experiment):
    timed_rounds = 2

    def stage(self):
        super().stage()
        self.report['scope'] = 'Continuous independent paced native writes after warm-up; ordinary packaged connector and read_committed observer'
        self.report['harness_sha256']['tools/dcg-sustained-probe.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def extended_workload(self, worker, session):
        batches, progress, errors = [], [], []
        stop = threading.Event()
        self.writer_stop = stop
        started = time.time_ns() / 1e6

        def write():
            try:
                deadline = time.monotonic() + self.args.seconds
                next_write = time.monotonic()
                for cycle in range(80):  # <= 24,000 row operations, irrespective of timing.
                    if stop.is_set() or time.monotonic() >= deadline:
                        break
                    first = 2_000_000 + cycle * 100
                    for table, operation in [('A', 'c'), ('M', 'c'), ('M', 'u')]:
                        if stop.wait(max(0, next_write - time.monotonic())):
                            return
                        begin = time.time_ns() / 1e6
                        self.write_rows(session, table, first, 100, operation)
                        batches.append({'table': table, 'operation': operation, 'first': first, 'rows': 100,
                                        'started_ms': begin, 'returned_ms': time.time_ns() / 1e6})
                        next_write = max(next_write + .5, time.monotonic())
            except BaseException as error:
                errors.append(self.fixture.redact(str(error)))

        actor = threading.Thread(target=write, daemon=True)
        self.writer_actor = actor
        actor.start()
        try:
            while actor.is_alive():
                events = self.new_events()
                selected = [r for r in events if r['value'] and r['key']['ID'] >= 2_000_000]
                submitted = sum(b['rows'] for b in batches)
                # Observations can arrive before an executing SQL statement returns.
                progress.append({'at_ms': time.time_ns() / 1e6, 'completed_source_operations': submitted,
                                 'observed_operations': len(selected)})
                if self.observer_process.poll() is not None:
                    raise RuntimeError('Observer exited during continuous writes')
                actor.join(.5)
            if errors or not batches:
                raise RuntimeError('Continuous writer failed: ' + repr(errors))
            rows = []
            for batch in batches:
                # Reuse the independent observer's exact mixed-value and before-image assertions.
                rows.extend(self.await_rows(batch['table'], batch['first'], batch['rows'], batch['operation']))
            measured = latency_summary(batches, rows)
            measured.update(started_ms=started, source_finished_ms=batches[-1]['returned_ms'],
                            finished_ms=max(r['observed_ms'] for r in rows),
                            offered_rows_per_second=measured['rows'] * 1000 / (batches[-1]['returned_ms'] - started),
                            drain_after_writer_ms=max(0, max(r['observed_ms'] for r in rows) - batches[-1]['returned_ms']),
                            pacing='100 row operations per SQL, at most two SQL submissions per second',
                            timing='Per-record host batch-submit to read_committed observation; not source-commit latency',
                            nominal_seconds=self.args.seconds, batches=batches, progress=progress)
            self.report['sustained'] = measured
            self.additional_events = measured['rows']
            self.check('Continuous independent writes preserve every row, mixed value and update before-image', True,
                       'control/performance-events.jsonl')
            print(json.dumps({k: v for k, v in measured.items() if k not in ('batches', 'progress')}), flush=True)
        finally:
            stop.set()
            actor.join(40)
            if actor.is_alive():
                raise RuntimeError('Continuous native writer has not drained; fixture cleanup must wait')


def configure_parser(parser):
    parser.add_argument('--seconds', type=int, choices=[60, 120], default=60,
                        help='Bounded continuous offered load after warm-up, independently of delivery')


if __name__ == '__main__':
    performance.main(Experiment, configure_parser)
