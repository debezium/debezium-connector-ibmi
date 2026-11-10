# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Owned late-membership experiment. No startup admission or Kafka publication.

Retains both the complete maintenance trace and an explicitly named row-only
diagnostic projection to exercise the final unknown-binding guard independently.
The projection is never presented as complete journal coverage.
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import traceback

SPEC = importlib.util.spec_from_file_location('boundary', Path(__file__).with_name('dcg-short-boundary-probe.py'))
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)
census, scope, fence = boundary.census, boundary.scope, boundary.fence


def capture(exp, name, start, end, mode='capture'):
    if start['receiver'] != end['receiver'] or start['library'] != end['library']:
        raise ValueError('Exact same-receiver diagnostic required')
    lower, upper = exp.directory / (name + '-start.json'), exp.directory / (name + '-end.json')
    census.save(lower, {'from': start, 'coordinate_source': 'SQL diagnostic only', 'startup_proof': False})
    census.save(upper, {'through': end, 'coordinate_source': 'SQL diagnostic only', 'startup_proof': False})
    exp.capture(name, mode, lower, upper, 1048576)


def late_membership(exp, writer, observer, baseline):
    exp.execute(writer, 'create-after-z', f'CREATE TABLE {exp.library}.LATE(ID INTEGER NOT NULL PRIMARY KEY)')
    exp.fixture.TABLES = [*exp.fixture.TABLES, 'LATE']
    exp.fixture.owned_objects = exp.fixture.inventory('objects-with-late-file.json')
    after_ddl = boundary.point(observer[0], exp.library)
    exp.execute(writer, 'known-prefix', f'INSERT INTO {exp.library}.A VALUES (11)')
    first_row = boundary.point(observer[0], exp.library)
    exp.execute(writer, 'unknown-row', f'INSERT INTO {exp.library}.LATE VALUES (99)')
    exp.execute(writer, 'known-after-unknown', f'INSERT INTO {exp.library}.A VALUES (12)')
    end = boundary.point(observer[0], exp.library)
    exp.report['autocommit'] = {'baseline': baseline, 'after_ddl': after_ddl, 'from': first_row, 'through': end}
    capture(exp, 'autocommit-rows', first_row, end)
    transaction = exp.connection(transaction=True)
    try:
        exp.execute(transaction, 'buffered-known-prefix', f'INSERT INTO {exp.library}.A VALUES (13)')
        exp.execute(transaction, 'buffered-unknown-row', f'INSERT INTO {exp.library}.LATE VALUES (100)')
        exp.execute(transaction, 'commit')
    finally:
        exp.execute(transaction, 'rollback')
    tx_end = boundary.point(observer[0], exp.library)
    exp.report['transaction'] = {'baseline': end, 'through': tx_end}
    capture(exp, 'transaction-rows', end, tx_end)
    try:
        capture(exp, 'complete-tail', baseline, tx_end, 'audit')
        exp.report['complete_tail'] = 'captured; must separately reject maintenance'
    except RuntimeError:
        rejected = json.loads((exp.directory / 'complete-tail/capture.json').read_text())
        if rejected.get('status') != 'rejected' or 'Pointer-backed journal data requires a different retrieval profile' not in rejected.get('error', ''):
            raise
        exp.report['complete_tail'] = 'strict-pointer-rejection-preserved'
    exp.fixture.verify_rows({'A': [1, 2, 3, 10, 11, 12, 13], 'B': [1, 2, 3], 'M': [1, 2, 3], 'U': [1, 2, 3], 'LATE': [99, 100]})


def schema_change(exp, writer, observer, metadata_session, queries, baseline):
    """Equal-width schema change and revert; never omit maintenance to qualify it."""
    exp.report['experiment'] = 'Equal-width schema change and revert after Z'
    # Metadata services and SQL plans may retain native file handles. Release our
    # own observers before challenging DDL, never lengthen a held-lock budget.
    for connection in (metadata_session[0], writer[0]):
        connection.close()
        exp.connections.remove(connection)
    for phase, sql_type, row in [('changed', 'REAL', 101), ('restored', 'INTEGER', 102)]:
        exp.fixture.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{exp.library}','B','*FILE',0)) X", 'locks-before-' + phase + '.json')
        ddl = exp.connection()
        owned_options = False
        try:
            # IBM's job-local QAQQINI override permits this deliberately owned
            # conversion; no system reply-list or global message change is made.
            exp.execute(ddl, 'create-job-local-ddl-options', 'CALL QSYS2.OVERRIDE_QAQQINI(1)')
            owned_options = True
            exp.execute(ddl, 'owned-ddl-inquiry-policy', "CALL QSYS2.OVERRIDE_QAQQINI(2, 'SUPPRESS_INQUIRY_MESSAGES', '*YES')")
            exp.execute(ddl, 'alter-' + phase, f'ALTER TABLE {exp.library}.B ALTER COLUMN ID SET DATA TYPE {sql_type}')
        except Exception:
            try:
                messages = census.rows(observer[0], 'SELECT ORDINAL_POSITION,MESSAGE_ID,MESSAGE_TEXT,SEVERITY '
                                       'FROM TABLE(QSYS2.JOBLOG_INFO(?)) X ORDER BY ORDINAL_POSITION DESC FETCH FIRST 64 ROWS ONLY',
                                       (ddl[1],), 64)
                census.save(exp.directory / ('ddl-' + phase + '-joblog.json'), json.loads(exp.fixture.redact(json.dumps({'job': ddl[1], 'messages': messages}, default=str))))
            except Exception as diagnostic:
                census.save(exp.directory / ('ddl-' + phase + '-joblog-error.json'), {'error': exp.fixture.redact(str(diagnostic))})
            raise
        finally:
            try:
                if owned_options:
                    exp.execute(ddl, 'remove-job-local-ddl-options', 'CALL QSYS2.OVERRIDE_QAQQINI(3)')
            finally:
                ddl[0].close()
                exp.connections.remove(ddl[0])
        after_ddl = boundary.point(observer[0], exp.library)
        metadata_session = exp.connection()
        try:
            scope.prepare_inventory(exp, metadata_session, queries)
            boundary.metadata(exp, metadata_session, queries, exp.directory / ('metadata-' + phase))
        finally:
            metadata_session[0].close()
            exp.connections.remove(metadata_session[0])
        row_writer = exp.connection()
        try:
            exp.execute(row_writer, 'row-' + phase, f'INSERT INTO {exp.library}.B VALUES ({row})')
        finally:
            row_writer[0].close()
            exp.connections.remove(row_writer[0])
        point = boundary.point(observer[0], exp.library)
        capture(exp, 'row-' + phase, point, point)
        exp.report[phase] = {'after_ddl': after_ddl, 'row': point}
    writer = exp.connection()
    exp.execute(writer, 'known-after-schema-cycle', f'INSERT INTO {exp.library}.A VALUES (11)')
    end = boundary.point(observer[0], exp.library)
    params = [exp.library, 'QSQJRN', exp.library, baseline['receiver'], baseline['sequence'],
              exp.library, end['receiver'], end['sequence'], baseline['sequence']]
    audit = census.rows(observer[0], boundary.AUDIT_SQL, params, 2000)
    census.save(exp.directory / 'schema-audit-sql.json', {'sql': boundary.AUDIT_SQL, 'parameters': params, 'rows': audit,
                                                        'strict_rpc_replay': False, 'startup_proof': False})
    try:
        capture(exp, 'complete-schema-tail', baseline, end, 'audit')
        exp.report['complete_tail'] = 'captured; strict maintenance classification still required'
    except RuntimeError:
        rejected = json.loads((exp.directory / 'complete-schema-tail/capture.json').read_text())
        if rejected.get('status') != 'rejected' or 'Pointer-backed journal data requires a different retrieval profile' not in rejected.get('error', ''):
            raise
        exp.report['complete_tail'] = 'strict-pointer-rejection-preserved'
    exp.fixture.verify_rows({'A': [1, 2, 3, 10, 11], 'B': [1, 2, 3, 101, 102], 'M': [1, 2, 3], 'U': [1, 2, 3]})
    # A native ALTER may replace the target *FILE; only the explicitly operated
    # B identity may differ. Never enroll an unrelated or newly appearing object.
    current = exp.fixture.inventory('objects-after-owned-alter.json')
    key = lambda x: (x['OBJNAME'], x['OBJTYPE'], x['OBJATTRIBUTE'])
    old_by_key, new_by_key = {key(x): x for x in exp.fixture.owned_objects}, {key(x): x for x in current}
    if old_by_key.keys() != new_by_key.keys() or any(old_by_key[k] != new_by_key[k] for k in old_by_key if k[:2] != ('B', '*FILE')):
        raise ValueError('Unexpected object change during owned schema experiment')
    exp.fixture.owned_objects = current


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] != 'schema':
        raise SystemExit('Usage: dcg-boundary-tail-probe.py NEW_DIRECTORY DCNCdddd [schema]; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Late journal membership rejection', qualification='unqualified',
                      projection='row-only diagnostic deliberately excludes prior maintenance; never startup proof')
    for name in ('dcg-boundary-tail-probe.py', 'dcg-short-boundary-probe.py', 'dcg-short-boundary.py'):
        (exp.directory / name).write_bytes(Path(__file__).with_name(name).read_bytes())
    try:
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
        exp.report['candidate'] = boundary.attempt(exp, queries, metadata_session, observer, 'candidate')
        writer = exp.connection()
        exp.execute(writer, 'known-completed-baseline', f'INSERT INTO {exp.library}.A VALUES (10)')
        baseline = boundary.point(observer[0], exp.library)
        # The baseline is a controlled completed A insert, never marker N or Z.
        exp.report['baseline'] = baseline
        capture(exp, 'baseline-row', baseline, baseline)
        if len(sys.argv) == 4:
            schema_change(exp, writer, observer, metadata_session, queries, baseline)
        else:
            late_membership(exp, writer, observer, baseline)
        exp.report['status'] = 'captured-unqualified'
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        exp.cleanup()
    print(exp.directory / 'report.json', flush=True)
    if exp.report['status'] != 'captured-unqualified' or exp.report.get('fixture_cleanup', {}).get('status') != 'passed' or exp.report.get('connection_cleanup_errors'):
        raise SystemExit('Native tail experiment or cleanup failed; retained evidence identifies the gate')


if __name__ == '__main__':
    main()
