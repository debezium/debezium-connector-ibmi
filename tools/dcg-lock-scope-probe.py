# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit isolated native lock-scope qualification; never admits migration or publishes CDC."""
import importlib.util
import datetime
import json
import re
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback

SPEC = importlib.util.spec_from_file_location('fence', Path(__file__).with_name('dcg-online-fence-probe.py'))
fence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fence)
census = fence.census


def inventory_command(library):
    if not re.fullmatch(r'DCNC[0-9]{4}', library):
        raise ValueError('Exact isolated journal required')
    return f"WRKJRNA JRN({library}/QSQJRN) OUTPUT(*OUTFILE) OUTFILE(QTEMP/DCGSCOPE) OUTMBR(*FIRST *REPLACE) DETAIL(*OUTPUT)"


def prepare_inventory(exp, session, queries):
    counts = census.rows(session[0], queries['scope_counts']['sql'], (exp.library, 'QSQJRN'), 1)
    census.save(exp.directory / 'inventory-preflight-counts.json', counts)
    fields = ['JOURNALED_FILES', 'JOURNALED_MEMBERS', 'JOURNALED_LIBRARIES', 'JOURNALED_DATA_AREAS',
              'JOURNALED_DATA_QUEUES', 'JOURNALED_IFS_OBJECTS', 'JOURNALED_ACCESS_PATHS', 'NUMBER_REMOTE_JOURNALS']
    if len(counts) != 1 or any(type(counts[0].get(k)) is not int or not 0 <= counts[0][k] <= 50 for k in fields):
        raise ValueError('Unbounded native journal inventory')
    if sum(counts[0][k] for k in fields) > 128:
        raise ValueError('Native journal inventory exceeds fixture bound')
    # Only the owned test job and its temporary output file are changed. Default
    # inquiry replies prevent an overflow from waiting for an operator.
    for command in ['CHGJOB INQMSGRPY(*DFT)',
                    'CRTDUPOBJ OBJ(QAWRKJRNA) FROMLIB(QSYS) OBJTYPE(*FILE) TOLIB(QTEMP) NEWOBJ(DCGSCOPE) DATA(*NO)',
                    'CHGPF FILE(QTEMP/DCGSCOPE) SIZE(128 0 0)', inventory_command(exp.library)]:
        exp.execute(session, 'temporary-journal-inventory', "CALL QSYS2.QCMDEXC('" + command + "')")


class ScopedCollector(fence.Collector):
    def __init__(self, connection, library, targets, queries, discovered, members):
        self.discovered, self.discovered_members = discovered, members
        super().__init__(connection, library, [t['file'] for t in targets], queries,
                         members={t['file']: t['member'] for t in targets})

    def add_journal(self, phase):
        super().add_journal(phase)
        self.add('scope', [self.library, 'QSQJRN'], phase)
        self.add('scope_counts', [self.library, 'QSQJRN'], phase)

    def before_batch(self, batch):
        if batch['items'][0]['name'] == 'scope':
            return ["CALL QSYS2.QCMDEXC('" + inventory_command(self.library) + "');"]
        return []

    def complete_capture(self, result):
        scope = {phase: {('objects' if q['name'] == 'scope' else 'counts'): q['rows']
                         for q in result['queries'] if q['phase'] == phase and q['name'] in ['scope', 'scope_counts']}
                 for phase in ['before', 'after']}
        result['profile'] = 'online-native-scope-fence-candidate-v1'
        result['native_scope'] = {'discovered': self.discovered, 'members': self.discovered_members, **scope}
        result['queries'] = [q for q in result['queries'] if q['name'] not in ['scope', 'scope_counts']]
        if set(scope) != {'before', 'after'} or self.discovered != scope['before'] or scope['before'] != scope['after']:
            raise ValueError('Native scope changed across the lock interval')


def discover(exp, connection, queries, directory):
    directory.mkdir()
    definition = queries['scope']
    started = time.monotonic()
    raw = census.rows(connection, definition['sql'], (exp.library, 'QSQJRN'), definition['maximumRows'])
    census.save(directory / 'timing.json', {'discovery_query_seconds': time.monotonic() - started, 'outside_lock': True})
    census.save(directory / 'objects.json', raw)
    counts = census.rows(connection, queries['scope_counts']['sql'], (exp.library, 'QSQJRN'), 1)
    census.save(directory / 'counts.json', counts)
    snapshot = {'objects': raw, 'counts': counts}
    census.save(directory / 'query.json', {'sql': definition['sql'], 'parameters': [exp.library, 'QSQJRN'], 'maximum_rows': definition['maximumRows']})
    # Exact member lookups avoid an unbounded cross-catalog join. The Java
    # validator still rejects omitted resources, duplicates and wrong counts.
    members = []
    for row in raw:
        if row['OBJECT_TYPE'] == '*FILE':
            if row['OBJECT_LIBRARY'] != exp.library:
                raise ValueError('Foreign file outside the isolated library')
            member = census.rows(connection, queries['member']['sql'], (row['OBJECT_LIBRARY'], row['OBJECT_NAME']), 1)
            census.save(directory / ('member-' + str(len(members)) + '.json'), member)
            members.extend(member)
    census.save(directory / 'rows.json', {'scope': snapshot, 'members': members})
    process = subprocess.run(exp.java_command('As400OnlineLockScope', exp.library, directory / 'rows.json', directory / 'targets.json'),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
    (directory / 'validate.log').write_text(exp.fixture.redact(process.stdout))
    if process.returncode:
        raise ValueError('Native scope discovery rejected')
    return ScopedCollector(connection, exp.library, json.loads((directory / 'targets.json').read_text())['targets'], queries, snapshot, members)


def capture_valid(exp, collector, label):
    import pyodbc
    for attempt in range(5):
        directory = exp.directory / f'{label}-{attempt + 1}'
        try:
            result = collector.capture(directory)
            fence.validate_capture(exp, directory)
            return result, directory
        except pyodbc.Error as error:
            if 'CPF1002' not in str(error) or attempt == 4:
                raise
            time.sleep(.2)


def classify_mutation(event):
    if event['actor_status'] == 'completed':
        event['status'] = 'not-excluded' if event['holder_locks_after_actor'] else 'completion-order-unqualified'
        if event.get('capture_status') == 'candidate-validated':
            released = datetime.datetime.fromisoformat(str(next(r['OBSERVED'] for r in event['capture']['times'] if r['PHASE'] == 'released')))
            before = datetime.datetime.fromisoformat(event['actor_server_before'])
            after = datetime.datetime.fromisoformat(event['actor_server_after'])
            if before >= released:
                raise ValueError('Actor started after native release; no overlap established')
            event['status'] = 'not-excluded' if after < released else 'wait-observed' if event['waiting_locks'] else 'completion-order-unqualified'
    elif any(code in event.get('actor_error', '') for code in ['SQL0913', 'CPF3202', 'CPF3203', 'CPF7031', 'CPF9803']):
        event['status'] = 'conflict-observed'
    else:
        raise ValueError('Mutation failed for a reason other than the tested native lock')
    return event['status']


def capture_sql_journal(exp, connection):
    """Independent bounded SQL evidence; never substitutes for strict RPC replay."""
    start = json.loads((exp.directory / 'baseline/capture.json').read_text())['from']
    end = json.loads((exp.directory / 'final/capture.json').read_text())['through']
    if start['library'] != exp.library or end['library'] != exp.library or start['receiver'] != end['receiver']:
        raise ValueError('Exact single-receiver diagnostic interval required')
    sql = """SELECT SEQUENCE_NUMBER,JOURNAL_CODE,JOURNAL_ENTRY_TYPE,OBJECT,OBJECT_TYPE,
        COMMIT_CYCLE,COUNT_OR_RRN,THREAD,INDICATOR_FLAG,ENTRY_TIMESTAMP,
        RECEIVER_LIBRARY,RECEIVER_NAME,HEX(JOURNAL_IDENTIFIER) AS JID_HEX,
        LENGTH(ENTRY_DATA) AS DATA_LENGTH,HEX(SUBSTR(ENTRY_DATA,1,16000)) AS DATA_HEX,
        HEX(NULL_VALUE_INDICATORS) AS NULL_MAP_HEX
        FROM TABLE(QSYS2.DISPLAY_JOURNAL(?, ?,
            STARTING_RECEIVER_LIBRARY=>?, STARTING_RECEIVER_NAME=>?, STARTING_SEQUENCE=>?,
            ENDING_RECEIVER_LIBRARY=>?, ENDING_RECEIVER_NAME=>?, ENDING_SEQUENCE=>?)) X
        ORDER BY SEQUENCE_NUMBER FETCH FIRST 2001 ROWS ONLY"""
    parameters = [exp.library, 'QSQJRN', exp.library, start['receiver'], int(start['sequence']),
                  exp.library, end['receiver'], int(end['sequence'])]
    captured = census.rows(connection, sql, parameters, 2000)
    result = {'sql': sql, 'parameters': parameters, 'rows': captured, 'startup_proof': False,
              'rpc_replay_equivalence': False, 'maximum_rows': 2000, 'maximum_entry_data_bytes': 16000}
    census.save(exp.directory / 'sql-journal.json', result)
    if not captured or any(row['DATA_LENGTH'] is None or int(row['DATA_LENGTH']) > 16000 for row in captured):
        raise ValueError('Incomplete SQL journal payload evidence; preserve partial capture')
    return len(captured)


def main():
    if len(sys.argv) not in [3, 4] or len(sys.argv) == 4 and sys.argv[3] != 'discovery':
        raise SystemExit('Usage: dcg-lock-scope-probe.py NEW_DIRECTORY DCNCdddd [discovery]; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Native scope and lock exclusion', qualification='unqualified', observations=[])
    (exp.directory / 'harness.py').write_bytes(Path(__file__).read_bytes())
    (exp.directory / 'fence-harness.py').write_bytes(Path(fence.__file__).read_bytes())
    exp.persist()
    try:
        fence.stage_runtime(exp)
        exp.capture('preflight', 'preflight')
        exp.report['source_connected'] = True
        exp.fixture.setup()
        exp.deadline = time.monotonic() + 480
        holder = exp.connection()
        observer = exp.connection(read_only=True)
        # The native discovery must find an object absent from the old four-table
        # fixture definition and from any configured output table selection.
        exp.execute(holder, 'fixture-ddl', f'CREATE TABLE {exp.library}.EXTRA(ID INTEGER NOT NULL PRIMARY KEY)')
        exp.fixture.TABLES = [*exp.fixture.TABLES, 'EXTRA']
        exp.fixture.owned_objects = exp.fixture.inventory('objects-with-extra.json')
        exp.execute(holder, 'insert', f'INSERT INTO {exp.library}.EXTRA VALUES (1)')
        exp.execute(holder, 'native-member-name', f"CALL QSYS2.QCMDEXC('RNMM FILE({exp.library}/EXTRA) MBR(EXTRA) NEWMBR(DATA)')")
        query_file = exp.directory / 'queries.json'
        subprocess.run(exp.java_command('As400OnlineFenceEvidence', 'queries', query_file), check=True, timeout=30)
        queries = json.loads(query_file.read_text())
        prepare_inventory(exp, holder, queries)
        collector = discover(exp, holder[0], queries, exp.directory / 'discovery')
        if 'EXTRA' not in collector.tables or len(collector.tables) != 5:
            raise ValueError('Native discovery omitted the independent fifth file')
        collector.prepare()
        exp.capture('baseline', 'anchor')
        captured, directory = capture_valid(exp, collector, 'candidate')
        if fence.held_locks(observer[0], exp.library, collector.tables, holder[1]):
            raise ValueError('Native collector retained locks')
        exp.report['candidate'] = str(directory.relative_to(exp.directory) / 'capture.json')
        exp.report['observations'].append({'case': 'complete-native-scope', 'status': 'candidate-validated',
                                           'targets': captured['targets'], 'hold_seconds': captured['hold_seconds']})
        if len(sys.argv) == 3:
            run_matrix(exp, collector, holder, observer)
        exp.capture('final', 'anchor')
        exp.report['sql_journal_entries'] = capture_sql_journal(exp, observer[0])
        try:
            exp.capture('oracle', 'capture', exp.directory / 'baseline/capture.json', exp.directory / 'final/capture.json', 1048576)
            exp.report['strict_rpc_replay_input'] = 'captured'
        except RuntimeError:
            rejected = json.loads((exp.directory / 'oracle/capture.json').read_text())
            if rejected.get('status') != 'rejected' or 'Pointer-backed journal data requires a different retrieval profile' not in rejected.get('error', ''):
                raise
            exp.report['strict_rpc_replay_input'] = 'rejected-pointer-backed-data; SQL evidence is diagnostic only'

        exp.report['status'] = 'captured-unqualified'
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        exp.cleanup()
    print(str(exp.directory / 'report.json'), flush=True)
    if exp.report['status'] != 'captured-unqualified' or exp.report.get('fixture_cleanup', {}).get('status') != 'passed' or exp.report.get('connection_cleanup_errors'):
        raise SystemExit('Qualification experiment or cleanup failed')


def run_matrix(exp, collector, holder, observer):
    import pyodbc

    def server_time(connection):
        return str(census.rows(connection, 'SELECT CURRENT_TIMESTAMP AS OBSERVED FROM SYSIBM.SYSDUMMY1', maximum=1)[0]['OBSERVED'])

    def locks(job, states):
        return [r for r in census.rows(observer[0],
                "SELECT JOB_NAME, LOCK_STATE, LOCK_STATUS, MEMBER_NAME FROM TABLE(QSYS2.OBJECT_LOCK_INFO(?, ?, '*FILE', 0)) X WHERE JOB_NAME=?",
                (exp.library, 'B', job), 32) if r['LOCK_STATE'] in states and r['LOCK_STATUS'] == 'HELD']

    # A real autocommit UPDATE remains active in a bounded AFTER trigger. This
    # tests native writer admission; it does not manufacture an R/UB-only cut.
    trigger = f'{exp.library}.HOLDUP'
    exp.execute(holder, 'fixture-trigger', f"CREATE TRIGGER {trigger} AFTER UPDATE ON {exp.library}.B FOR EACH STATEMENT MODE DB2SQL "
                "BEGIN ATOMIC CALL QSYS2.QCMDEXC('DLYJOB DLY(10)'); END")
    exp.fixture.owned_objects = exp.fixture.inventory('objects-with-trigger.json')
    writer = exp.connection()
    writer[0].timeout = 20
    active = {'case': 'unfinished-autocommit-update', 'status': 'started', 'job': writer[1]}

    def update():
        try:
            exp.execute(writer, 'autocommit-update', f'UPDATE {exp.library}.B SET ID=11 WHERE ID=1')
            active['writer_status'] = 'completed'
        except Exception as error:
            active.update(writer_status='failed', writer_error=exp.fixture.redact(str(error)))
        finally:
            active['writer_finished'] = census.stamp()

    exp.churn_thread = threading.Thread(target=update, daemon=True)
    exp.churn_thread.start()
    try:
        until = time.monotonic() + 8
        observed = []
        while exp.churn_thread.is_alive() and time.monotonic() < until:
            observed = locks(writer[1], ['*SHRUPD', '*EXCLRD', '*EXCL'])
            if observed:
                break
            time.sleep(.05)
        if not observed or not exp.churn_thread.is_alive():
            raise ValueError('Native in-flight autocommit operation was not observed')
        active['writer_locks'] = observed
        exp.writer_thread = threading.Thread(target=exp.independent_writer, daemon=True)
        exp.writer_thread.start()
        try:
            collector.capture(exp.directory / 'unfinished-autocommit')
            raise ValueError('In-flight autocommit update was admitted')
        except pyodbc.Error as error:
            if 'CPF1002' not in str(error) or 'Cannot allocate object B.' not in str(error):
                raise
            active['rejected'] = census.stamp()
            if not exp.churn_thread.is_alive():
                raise ValueError('Busy rejection did not occur during the unfinished UPDATE')
            active['status'] = 'excluded'
        if fence.held_locks(observer[0], exp.library, collector.tables, holder[1]):
            raise ValueError('Busy autocommit rejection retained partial locks')
    finally:
        exp.churn_thread.join(timeout=25)
        exp.stop.set()
        if exp.writer_thread is not None:
            exp.writer_thread.join(timeout=10)
        exp.report['observations'].append(active)
        census.save(exp.directory / 'unrelated-progress.json', exp.progress)
        exp.persist()
    if exp.churn_thread.is_alive() or exp.writer_thread.is_alive() or active.get('writer_status') != 'completed' or exp.background_error or not exp.progress:
        raise ValueError('Controlled concurrent writers did not complete cleanly')
    writer[0].close()
    exp.connections.remove(writer[0])
    exp.execute(holder, 'fixture-trigger-drop', f'DROP TRIGGER {trigger}')
    exp.fixture.owned_objects = exp.fixture.inventory('objects-without-trigger.json')

    # Observe the actor while the server-owned collector releases independently.
    # Do not change job wait policy: the test account cannot change DFTWAIT.
    for label, statement in [
            ('ddl', f'ALTER TABLE {exp.library}.B ADD COLUMN NOTE INTEGER'),
            ('end-journaling', f"CALL QSYS2.QCMDEXC('ENDJRNPF FILE({exp.library}/B)')"),
            ('new-journaled-file', f'CREATE TABLE {exp.library}.NEWFILE(ID INTEGER NOT NULL PRIMARY KEY)')]:
        actor = exp.connection()
        event = {'case': label, 'actor_job': actor[1], 'status': 'unqualified'}
        exp.report['observations'].append(event)

        def collect():
            try:
                event['capture'] = collector.capture(exp.directory / label, delay=1)
                fence.validate_capture(exp, exp.directory / label)
                event['capture_status'] = 'candidate-validated'
            except Exception as error:
                event.update(capture_status='rejected', capture_error=exp.fixture.redact(str(error)))
            finally:
                event['capture_finished'] = census.stamp()

        exp.writer_thread = threading.Thread(target=collect, daemon=True)
        exp.writer_thread.start()
        try:
            until = time.monotonic() + 15
            observed = []
            while exp.writer_thread.is_alive() and time.monotonic() < until:
                observed = locks(holder[1], ['*EXCLRD'])
                if observed:
                    break
                time.sleep(.05)
            if not observed:
                raise ValueError('Concurrent mutation did not overlap an observed native lock')
            event['holder_locks'] = observed
            def mutate():
                event['actor_server_before'] = server_time(actor[0])
                try:
                    exp.execute(actor, label, statement)
                    event['actor_status'] = 'completed'
                except Exception as error:
                    event.update(actor_status='rejected', actor_error=exp.fixture.redact(str(error)))
                finally:
                    event['actor_server_after'] = server_time(actor[0])

            exp.churn_thread = threading.Thread(target=mutate, daemon=True)
            exp.churn_thread.start()
            event['waiting_locks'] = []
            while exp.churn_thread.is_alive() and time.monotonic() < until:
                current = census.rows(observer[0],
                    "SELECT JOB_NAME, LOCK_STATE, LOCK_STATUS, MEMBER_NAME FROM TABLE(QSYS2.OBJECT_LOCK_INFO(?, ?, '*FILE', 0)) X WHERE JOB_NAME=?",
                    (exp.library, 'B', actor[1]), 32)
                event['waiting_locks'].extend(r for r in current if r['LOCK_STATUS'] != 'HELD')
                time.sleep(.05)
            exp.churn_thread.join(timeout=40)
            if exp.churn_thread.is_alive():
                raise ValueError('Native actor did not drain')
            event['holder_locks_after_actor'] = locks(holder[1], ['*EXCLRD'])
        finally:
            exp.writer_thread.join(timeout=25)
        if exp.writer_thread.is_alive():
            raise ValueError('Server-owned capture did not drain')
        if fence.held_locks(observer[0], exp.library, collector.tables, holder[1]):
            raise ValueError('Concurrent mutation case retained source locks')
        partial = {'status': event.get('capture_status'), 'startup_proof': False, 'after_drain': True, 'queries': []}
        for batch in collector.batches:
            partial['queries'].append({'name': batch['items'][0]['name'], 'phase': batch['items'][0]['phase'],
                                       'rows': census.rows(holder[0], 'SELECT * FROM ' + batch['table'], maximum=batch['maximum_rows'])})
        partial['times'] = census.rows(holder[0], 'SELECT PHASE, OBSERVED FROM SESSION.DCGFTIME ORDER BY OBSERVED', maximum=len(collector.batches) + 3)
        census.save(exp.directory / label / 'retained-partial-results.json', partial)
        classify_mutation(event)

        # Only exact fixture objects are restored, after both jobs have drained.
        if event['actor_status'] == 'completed':
            if label == 'end-journaling':
                exp.execute(actor, 'restore-journaling', f"CALL QSYS2.QCMDEXC('STRJRNPF FILE({exp.library}/B) JRN({exp.library}/QSQJRN) IMAGES(*BOTH) OMTJRNE(*OPNCLO)')")
            elif label == 'new-journaled-file':
                exp.fixture.TABLES = [*exp.fixture.TABLES, 'NEWFILE']
            # Leave an added column until whole-fixture cleanup, after all owned
            # sessions close. A DROP COLUMN here can conflict with cached opens.
        actor[0].close()
        exp.connections.remove(actor[0])
        exp.fixture.owned_objects = exp.fixture.inventory('objects-after-' + label + '.json')
        exp.persist()
    exp.report['qualification'] = 'not-admitted: scope samples and native conflict observations do not prove a historical origin'


if __name__ == '__main__':
    main()
