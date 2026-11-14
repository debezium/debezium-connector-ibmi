#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Summarize completed dcg-performance-probe runs, excluding warm-up.

Each directory must contain connect/report.json, native/report.json and jfr.json.
Create jfr.json with `jfr print --json --events jdk.SocketRead,jdk.SocketWrite,
jdk.GarbageCollection,jdk.GCHeapSummary connect/control/performance.jfr`.
Socket event counts describe transport activity, not individual database calls.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
import statistics


def cpu_at(samples, at):
    for a, b in zip(samples, samples[1:]):
        if a['at_ms'] <= at <= b['at_ms']:
            gap = b['at_ms'] - a['at_ms']
            if gap <= 0 or gap > 1500 or b['cpu_seconds'] < a['cpu_seconds']:
                raise ValueError('Invalid or interrupted CPU sampling')
            return a['cpu_seconds'] + (b['cpu_seconds'] - a['cpu_seconds']) * (at - a['at_ms']) / gap
    raise ValueError('CPU samples do not bracket measured interval')


def summarize_run(directory):
    report = json.loads((directory / 'connect/report.json').read_text())
    native = json.loads((directory / 'native/report.json').read_text())
    if (report.get('status') != 'passed' or native.get('status') != 'passed' or not report.get('checks')
            or any(c['status'] != 'passed' for c in report['checks'])
            or report.get('cleanup', {}).get('status') != 'passed'
            or native.get('fixture_cleanup', {}).get('status') != 'passed'
            or native.get('connection_cleanup_errors')):
        raise ValueError('Only fully passed and cleaned-up runs may be summarized: ' + str(directory))
    measurements = [m for m in report['measurements'] if not m['warmup']]
    bursts = [m for m in measurements if m['scenario'] != 'backlog-resume']
    expected = Counter({'A-c': 5, 'M-c': 5, 'M-u': 5})
    if Counter(m['scenario'] for m in bursts) != expected:
        raise ValueError('Missing measured burst repetitions')
    start, end = bursts[0]['started_ms'], bursts[-1]['finished_ms']
    samples = [s for s in report['samples'] if start <= s['at_ms'] <= end]
    count = sum(m['rows'] for m in bursts)
    cpu = cpu_at(report['samples'], end) - cpu_at(report['samples'], start)
    events = json.loads((directory / 'jfr.json').read_text())['recording']['events']
    sockets = Counter()
    socket_bytes = Counter()
    gc_pauses = 0
    gc_count = 0
    for event in events:
        value = event['values']
        timestamp = datetime.fromisoformat(value['startTime']).timestamp() * 1000
        if not start <= timestamp <= end:
            continue
        kind = event['type']
        if kind in ('jdk.SocketRead', 'jdk.SocketWrite'):
            key = kind.removeprefix('jdk.Socket') + ':' + str(value['port'])
            sockets[key] += 1
            socket_bytes[key] += max(0, value['bytesRead'] if kind.endswith('Read') else value['bytesWritten'])
        elif kind == 'jdk.GarbageCollection':
            duration = value['sumOfPauses']
            if not duration.startswith('PT') or not duration.endswith('S') or 'M' in duration:
                raise ValueError('Unexpected GC duration format')
            gc_pauses += float(duration[2:-1]) * 1000
            gc_count += 1
    if not sockets:
        raise ValueError('No flight recorder socket observations in measured interval')
    return {'run': directory.name, 'version': report['version'], 'mode': report['mode'],
            'buffer_storage': report.get('buffer_storage', 'memory'),
            'distribution_sha256': report['distribution_sha256'], 'image': report['image'],
            'rows_per_burst': report['rows_per_burst'], 'measurements': measurements,
            'burst_resources': {'rows': count, 'seconds': (end-start)/1000,
                                'cpu_seconds': cpu, 'cpu_ms_per_row': cpu*1000/count,
                                'peak_rss_mib': max(s['rss_kib'] for s in samples)/1024,
                                'median_rss_mib': statistics.median(s['rss_kib'] for s in samples)/1024,
                                'gc_count': gc_count, 'gc_pause_ms': gc_pauses,
                                'socket_events_by_direction_and_port': dict(sockets),
                                'socket_bytes_by_direction_and_port': dict(socket_bytes)},
            'cleanup': 'passed'}


def summarize(directories):
    runs = [summarize_run(directory) for directory in directories]
    if len({r['image'] for r in runs}) != 1 or len({r['rows_per_burst'] for r in runs}) != 1:
        raise ValueError('Runtime image or workload differs across comparisons')
    for version in {r['version'] for r in runs}:
        variants = {(r['distribution_sha256'], r['mode'], r['buffer_storage']) for r in runs if r['version'] == version}
        if len(variants) != 1:
            raise ValueError('A version label mixes different artifacts or modes')
    groups = defaultdict(list)
    for run in runs:
        for measurement in run['measurements']:
            groups[(run['version'], measurement['scenario'])].append(measurement)
    results = []
    for (version, scenario), values in sorted(groups.items()):
        if len({v['rows'] for v in values}) != 1:
            raise ValueError('Scenario has different record counts')
        results.append({'version': version, 'scenario': scenario, 'trials': len(values), 'rows': values[0]['rows'],
                        **({'p99_ms': statistics.median(v['p99_ms'] for v in values)} if all('p99_ms' in v for v in values) else {}),
                        **{key: statistics.median(v[key] for v in values)
                           for key in ['elapsed_ms', 'rows_per_second', 'source_sql_ms', 'p50_ms', 'p95_ms']},
                        'sql_return_to_last_visible_ms': (statistics.median(v['elapsed_ms']-v['source_sql_ms'] for v in values)
                                                          if scenario != 'backlog-resume' else None),
                        'elapsed_min_ms': min(v['elapsed_ms'] for v in values),
                        'elapsed_max_ms': max(v['elapsed_ms'] for v in values)})
    return {'timing': 'Median finite-burst or resume-to-drained measurements, not sustained capacity; p95_ms is median trial p95',
            'resources': 'Whole Connect JVM; measured burst interval only, excluding warm-up and restart workloads',
            'runs': runs, 'results': results}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(summarize(args.directories), indent=2) + '\n')
