# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Isolated prospective-schema warm-up. Source origin remains a test candidate."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback

SPEC = importlib.util.spec_from_file_location('tail', Path(__file__).with_name('dcg-boundary-tail-probe.py'))
tail = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tail)
boundary, census, scope, fence = tail.boundary, tail.census, tail.scope, tail.fence


def prepare(exp, active=False, discover_history=False):
    """Acquire owned boundary/captures; the caller must drain workers before cleanup."""
    fence.stage_runtime(exp)
    exp.capture('preflight', 'preflight')
    exp.report['source_connected'] = True
    exp.fixture.setup()
    exp.deadline = time.monotonic() + 480
    metadata_session, observer = exp.connection(), exp.connection(read_only=True)
    query_file = exp.directory / 'queries.json'
    subprocess.run(exp.java_command('As400OnlineFenceEvidence', 'queries', query_file), check=True, timeout=30)
    queries = json.loads(query_file.read_text())
    scope.prepare_inventory(exp, metadata_session, queries)
    history_from = None if discover_history else boundary.point(observer[0], exp.library)
    older = exp.connection(transaction=True)
    exp.execute(older, 'savepoint', 'SAVEPOINT EARLY ON ROLLBACK RETAIN CURSORS')
    exp.execute(older, 'old-row-before-origin', f'INSERT INTO {exp.library}.U VALUES (90)')
    exp.execute(older, 'rollback-to-savepoint', 'ROLLBACK TO SAVEPOINT EARLY')
    # Rolling back the changes does not release this native cycle's locks.
    # Refuse this attempt, keep ordinary source writes moving, then retry
    # after the writer completes. Never promote the failed pulse's markers.
    exp.report['busy'] = boundary.attempt(exp, queries, metadata_session, observer, 'busy', busy=True, discover_history=discover_history)
    writer = exp.connection()
    exp.execute(writer, 'unrelated-while-old-open', f'INSERT INTO {exp.library}.A VALUES (9)')
    progress = boundary.point(observer[0], exp.library)
    tail.capture(exp, 'legacy-progress', progress, progress)
    exp.execute(older, 'commit')
    active_rows = []
    if active:
        writer_session = exp.connection()
        stop = threading.Event()
        errors = []
        def write():
            try:
                for value in range(1000, 1032):
                    if stop.wait(.5):
                        break
                    exp.execute(writer_session, 'concurrent-ordinary-row', f'INSERT INTO {exp.library}.A VALUES ({value})')
                    active_rows.append(value)
            except BaseException as error:
                errors.append(exp.fixture.redact(str(error)))
        exp.churn_thread = threading.Thread(target=write, name='owned-boundary-row-writer', daemon=True)
        exp.churn_thread.start()
        try:
            exp.report['candidate'] = boundary.attempt(exp, queries, metadata_session, observer, 'candidate', discover_history=discover_history)
        finally:
            stop.set()
            exp.churn_thread.join(timeout=20)
            exp.report['concurrent_writer'] = {'rows': active_rows, 'errors': errors, 'drained': not exp.churn_thread.is_alive()}
        if exp.churn_thread.is_alive() or errors or not active_rows:
            raise ValueError('Concurrent source writer failed or did not drain')
    else:
        exp.report['candidate'] = boundary.attempt(exp, queries, metadata_session, observer, 'candidate', discover_history=discover_history)
    if discover_history:
        history_from = exp.report['candidate']['discovered_history']['floor']
    n = exp.report['candidate']['sequences'][1]
    interval = json.loads((exp.directory / 'candidate/interval.json').read_text())
    origin = {**interval['from'], 'sequence': n}
    exp.report['origin'] = origin
    newer = exp.connection(transaction=True)
    exp.execute(writer, 'legacy-after-origin', f'INSERT INTO {exp.library}.A VALUES (10)')
    exp.execute(newer, 'newer-insert', f'INSERT INTO {exp.library}.M VALUES (21)')
    exp.execute(newer, 'commit')
    exp.execute(writer, 'completed-legacy-cut', f'INSERT INTO {exp.library}.A VALUES (12)')
    exp.report['cut'] = boundary.point(observer[0], exp.library)
    clear = exp.connection(transaction=True)
    exp.execute(clear, 'truncate', f'TRUNCATE TABLE {exp.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
    exp.execute(clear, 'reload', f'INSERT INTO {exp.library}.B VALUES (101)')
    exp.execute(writer, 'unrelated-while-clear-open', f'INSERT INTO {exp.library}.A VALUES (14)')
    exp.execute(clear, 'commit')
    end = boundary.point(observer[0], exp.library)
    exp.report['through'] = end
    tail.capture(exp, 'older-controls', history_from, end, 'controls')
    tail.capture(exp, 'prospective-rows', origin, end)
    exp.fixture.verify_rows({'A': [1, 2, 3, 9, 10, 12, 14, *active_rows], 'B': [101], 'M': [1, 2, 3, 21], 'U': [1, 2, 3]})
    exp.report['status'] = 'captured-unqualified'


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] != 'active':
        raise SystemExit('Usage: dcg-prospective-probe.py NEW_DIRECTORY DCNCdddd [active]; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Prospective schema with older control-only history', qualification='unqualified')
    for name in ('dcg-prospective-probe.py', 'dcg-boundary-tail-probe.py', 'dcg-short-boundary-probe.py', 'dcg-short-boundary.py'):
        (exp.directory / name).write_bytes(Path(__file__).with_name(name).read_bytes())
    try:
        prepare(exp, len(sys.argv) == 4)
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        exp.cleanup()
    print(exp.directory / 'report.json', flush=True)
    if exp.report['status'] != 'captured-unqualified' or exp.report.get('fixture_cleanup', {}).get('status') != 'passed' or exp.report.get('connection_cleanup_errors'):
        raise SystemExit('Native prospective experiment or cleanup failed; retained evidence identifies the gate')


if __name__ == '__main__':
    main()
