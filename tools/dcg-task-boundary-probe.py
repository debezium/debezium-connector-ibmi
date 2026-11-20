# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit isolated validation of the production native boundary collector.

No Kafka publication or automatic migration admission. Independent SQL and writer
observations check the owned Java result; captured raw bytes are retained.
"""
import importlib.util
import json
from pathlib import Path
import re
import sys
import threading
import time
import traceback

SPEC = importlib.util.spec_from_file_location('boundary', Path(__file__).with_name('dcg-short-boundary-probe.py'))
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)
census = boundary.census


def verify_candidate(exp, candidate, observer):
    if candidate.get('status') != 'captured' or candidate.get('startup_proof') is not False or not candidate.get('transport_disconnected'):
        raise ValueError('Completed candidate-only native collection required')
    if candidate.get('candidate_present') is not True:
        raise ValueError('Expected writable boundary candidate')
    window = candidate['native_identity_evidence']
    start, end = window['from'], window['through']
    params = [exp.library, 'QSQJRN', exp.library, start['receiver'], int(start['sequence']),
              exp.library, end['receiver'], int(end['sequence']), int(start['sequence'])]
    rows = census.rows(observer[0], boundary.MARKERS_SQL, params, 32)
    for row in rows:
        for key in ['SEQUENCE_NUMBER', 'COMMIT_CYCLE']:
            if row[key] is None or int(row[key]) != row[key]:
                raise ValueError('Non-integral independent marker coordinate')
            row[key] = int(row[key])
    program, completion = candidate['program'], candidate['completion']
    expected = [f"DCGB1 {program['nonce']} A", f"DCGB1 {program['nonce']} N {program['scope_digest']}",
                f"DCGB1 {program['nonce']} Z {program['scope_digest']}"]
    checked = boundary.check_markers(rows, expected, completion['job'], f"{int(completion['thread']):016X}",
                                     {**start, 'sequence': int(start['sequence'])}, {**end, 'sequence': int(end['sequence'])})
    if checked['sequences'][1] != int(candidate['origin']['sequence']):
        raise ValueError('Native collector and independent N coordinates differ')
    if program['targets'] != [{'library': exp.library, 'file': t, 'member': t} for t in ['A', 'B', 'M', 'U']]:
        raise ValueError('Native collector omitted owned fixture member scope')
    census.save(exp.directory / 'candidate-independent.json', {'sql': boundary.MARKERS_SQL, 'parameters': params, 'rows': rows, 'checked': checked})


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] != 'terminal':
        raise SystemExit('Usage: dcg-task-boundary-probe.py NEW_DIRECTORY DCNCdddd [terminal]; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Task-owned native boundary component', startup_proof=False)
    stop = threading.Event()
    writer_thread = None
    terminal_thread = None
    try:
        boundary.fence.stage_runtime(exp)
        exp.capture('preflight', 'preflight')
        exp.fixture.setup()
        exp.deadline = time.monotonic() + 240
        old = exp.connection(transaction=True)
        exp.execute(old, 'uncommitted-row', f'INSERT INTO {exp.library}.U VALUES (90)')
        busy = json.loads(exp.capture('busy', 'short-boundary').read_text())
        if busy.get('status') != 'captured' or busy.get('candidate_present') is not False or not busy.get('transport_disconnected'):
            raise ValueError('Open transaction did not safely refuse admission')
        exp.execute(old, 'rollback')
        writer = exp.connection()
        observer = exp.connection(read_only=True)
        errors, progress = [], []
        if len(sys.argv) == 4:
            exp.execute(old, 'old-cycle-before-observation', f'INSERT INTO {exp.library}.U VALUES (91)')
            terminal_observer = exp.connection(read_only=True)
            start = boundary.point(terminal_observer[0], exp.library)

            def complete_after_opening():
                try:
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline and not stop.wait(.2):
                        end = boundary.point(terminal_observer[0], exp.library)
                        if end['receiver'] != start['receiver']:
                            raise ValueError('Receiver changed before controlled terminal')
                        params = [exp.library, 'QSQJRN', exp.library, start['receiver'], start['sequence'],
                                  exp.library, end['receiver'], end['sequence'], start['sequence']]
                        rows = census.rows(terminal_observer[0], boundary.MARKERS_SQL, params, 32)
                        # Compare decoded CCSID 37 text; raw bytes remain retained.
                        openings = [row for row in rows if re.fullmatch(r'DCGB1 [A-F0-9]{32} A', boundary.marker_bytes(row).decode('cp037'))]
                        if len(openings) == 1:
                            exp.execute(old, 'commit')
                            exp.report['terminal_after_opening'] = {'opening_sequence': str(openings[0]['SEQUENCE_NUMBER']), 'job': old[1], 'completed': True}
                            return
                    raise TimeoutError('Opening marker not observed before terminal deadline')
                except BaseException as error:
                    errors.append(exp.fixture.redact(str(error)))

            terminal_thread = threading.Thread(target=complete_after_opening, name='owned-boundary-terminal', daemon=True)
            terminal_thread.start()

        def write():
            try:
                for value in range(1000, 1200):
                    if stop.wait(.2):
                        break
                    exp.execute(writer, 'concurrent-row', f'INSERT INTO {exp.library}.A VALUES ({value})')
                    progress.append(value)
            except BaseException as error:
                errors.append(exp.fixture.redact(str(error)))

        writer_thread = threading.Thread(target=write, name='owned-boundary-writer', daemon=True)
        writer_thread.start()
        started = time.monotonic()
        try:
            candidate = json.loads(exp.capture('candidate', 'short-boundary').read_text())
        finally:
            stop.set()
            writer_thread.join(timeout=20)
            if terminal_thread:
                terminal_thread.join(timeout=20)
            exp.report['concurrent_writer'] = {'rows': progress, 'errors': errors, 'drained': not writer_thread.is_alive()}
            exp.report['candidate_elapsed_seconds'] = time.monotonic() - started
        if writer_thread.is_alive() or errors or len(progress) < 2:
            raise ValueError('Concurrent writer failed, stalled or did not drain')
        verify_candidate(exp, candidate, observer)
        if terminal_thread and (terminal_thread.is_alive() or not exp.report.get('terminal_after_opening', {}).get('completed')
                                or candidate.get('control_inventory', {}).get('terminal_count', 0) < 1):
            raise ValueError('Native control history did not reconstruct the controlled terminal')
        exp.fixture.verify_rows({'A': [1, 2, 3, *progress], 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3, *([91] if terminal_thread else [])]})
        exp.report.update(status='passed', busy_refused=True, candidate_verified=True)
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        stop.set()
        if writer_thread:
            writer_thread.join(timeout=20)
        if terminal_thread:
            terminal_thread.join(timeout=20)
        if any(thread and thread.is_alive() for thread in [writer_thread, terminal_thread]):
            exp.report.update(status='failed', fixture_cleanup={'status': 'blocked', 'reason': 'Writer or terminal actor not drained'})
            exp.persist()
        else:
            exp.cleanup()
    if exp.report.get('status') != 'passed' or exp.report.get('fixture_cleanup', {}).get('status') != 'passed' or exp.report.get('connection_cleanup_errors'):
        raise SystemExit('Boundary qualification or cleanup failed; retained evidence identifies the gate')
    print(exp.directory / 'report.json', flush=True)


if __name__ == '__main__':
    main()
