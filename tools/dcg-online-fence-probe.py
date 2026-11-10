# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit isolated native experiment; server-owned brief locks, candidate evidence only."""
import importlib.util
import datetime
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('census', ROOT / 'tools/dcg-online-census-probe.py')
census = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(census)


def literal(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z0-9_#$@/.*-]{1,55}', value):
        raise ValueError('Exact controlled native identifier required')
    return "'" + value + "'"


def split_batch_rows(rows, items):
    """Retain exact per-request ownership and bounds when queries share a server call."""
    result = {item['id']: [] for item in items}
    limits = {item['id']: item['maximum_rows'] for item in items}
    for row in rows:
        owner = row.get('DCG_REQUEST_ID')
        if type(owner) is not int or owner not in result:
            raise ValueError('Foreign or missing batched metadata owner')
        result[owner].append({k: v for k, v in row.items() if k != 'DCG_REQUEST_ID'})
        if len(result[owner]) > limits[owner]:
            raise ValueError('Excessive native metadata for one request')
    return result


class Collector:
    """Prepare outside the lock. One server call acquires, collects and releases."""
    def __init__(self, connection, library, tables, queries, batched=True, members=None):
        if not re.fullmatch(r'DCNC[0-9]{4}', library) or not 1 <= len(tables) <= 50 or tables != sorted(set(tables)):
            raise ValueError('Distinct ordered bounded isolated targets required')
        if any(not re.fullmatch(r'[A-Z][A-Z0-9_]{0,9}', name) for name in tables):
            raise ValueError('Exact table/member names required')
        self.members = dict(members) if members is not None else {table: table for table in tables}
        if set(self.members) != set(tables) or any(not isinstance(m, str) or not re.fullmatch(r'[A-Z0-9_#$@]{1,10}', m) for m in self.members.values()):
            raise ValueError('Complete exact native member mapping required')
        self.connection, self.library, self.tables, self.queries = connection, library, tables, queries
        self.items = []
        # Only prepares the SQL types/identity request. Rotation after this read
        # must reject against the independent journal samples inside the lock.
        attachment = census.rows(connection, 'SELECT ATTACHED_JOURNAL_RECEIVER_LIBRARY AS LIBRARY, ATTACHED_JOURNAL_RECEIVER_NAME AS NAME '
                                 'FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?', (library, 'QSQJRN'), 1)
        if len(attachment) != 1:
            raise ValueError('Missing preparation attachment')
        self.receiver = attachment[0]
        self.add_journal('before')
        for table in tables:
            self.add_table(table, 'before')
        for table in tables:
            for name in ['columns', 'keys']:
                self.add(name, [library, table], 'layout')
        for table in tables:
            self.add_table(table, 'after')
        self.add_journal('after')
        groups = {}
        for item in self.items:
            key = (item['phase'], item['name']) if batched else (item['id'], item['name'])
            groups.setdefault(key, []).append(item)
        self.batches = []
        for items in groups.values():
            if items[0]['sql'].count('?') != 2 or any(len(item['parameters']) != 2 for item in items):
                raise ValueError('Unexpected native query parameters')
            table = f'SESSION.DCGB{len(self.batches):03}'
            requests = ', '.join(f"(CAST({literal(item['parameters'][0])} AS VARCHAR(55)), CAST({literal(item['parameters'][1])} AS VARCHAR(55)), {item['id']})" for item in items)
            sql = items[0]['sql'].replace('?', 'R.P1', 1).replace('?', 'R.P2', 1)
            maximum = items[0]['maximum_rows']
            combined = f'WITH REQUESTS(P1,P2,REQUEST_ID) AS (VALUES {requests}) SELECT R.REQUEST_ID AS DCG_REQUEST_ID, Q.* '
            combined += f'FROM REQUESTS R CROSS JOIN LATERAL ({sql} FETCH FIRST {maximum + 1} ROWS ONLY) Q'
            self.batches.append({'table': table, 'items': items, 'sql': combined, 'maximum_rows': len(items) * (maximum + 1)})
        objects = ' '.join(f'({library}/{table} *FILE *EXCLRD {self.members[table]})' for table in tables)
        self.allocate, self.release = f'ALCOBJ OBJ({objects}) WAIT(1) CONFLICT(*RQSRLS)', f'DLCOBJ OBJ({objects})'

    def add(self, name, parameters, phase):
        query = self.queries[name]
        self.items.append({'name': name, 'phase': phase, 'parameters': parameters, 'sql': query['sql'],
                           'maximum_rows': query['maximumRows'], 'id': len(self.items)})

    def add_journal(self, phase):
        for name in ['journal', 'receivers']:
            self.add(name, [self.library, 'QSQJRN'], phase)
        self.add('identity', [f'/QSYS.LIB/{self.library}.LIB/QSQJRN.JRN', '*JRN'], phase)
        self.add('identity', [f'/QSYS.LIB/{self.receiver["LIBRARY"]}.LIB/{self.receiver["NAME"]}.JRNRCV', '*JRNRCV'], phase)

    def add_table(self, table, phase):
        for name in ['member', 'format']:
            self.add(name, [self.library, table], phase)
        path = f'/QSYS.LIB/{self.library}.LIB/{table}.FILE'
        self.add('object', [path, '*FILE'], phase)
        self.add('object', [path + '/' + self.members[table] + '.MBR', '*MBR'], phase)

    def execute(self, sql):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(sql)
            if cursor.messages:
                raise ValueError('Native statement warning')

    def prepare(self):
        for item in self.batches:
            self.execute(f'DECLARE GLOBAL TEMPORARY TABLE {item["table"]} AS (' + item['sql']
                         + ') WITH NO DATA ON COMMIT PRESERVE ROWS NOT LOGGED')
        self.execute('DECLARE GLOBAL TEMPORARY TABLE SESSION.DCGFTIME (PHASE VARCHAR(16), OBSERVED TIMESTAMP(6)) '
                     'ON COMMIT PRESERVE ROWS NOT LOGGED')

    def statement(self, fault=False, delay=0, budget_ms=5000):
        if (type(delay) is not int or not 0 <= delay <= 2 or type(fault) is not bool
                or type(budget_ms) is not int or not 100 <= budget_ms <= 5000):
            raise ValueError('Bounded explicit fixture fault required')
        # No application row reads, network rendezvous or client-controlled release
        # occurs while held. Warning/error handlers prevent returning partial data.
        handler = f"BEGIN IF HELD=1 THEN CALL QSYS2.QCMDEXC('{self.release}'); END IF; RESIGNAL; END"
        # This is a between-statement budget, not a preemptive timeout for a
        # stalled native service. Release still runs on the source, not a client.
        budget = f"IF CURRENT_TIMESTAMP > ACQUIRED + {budget_ms * 1000} MICROSECONDS THEN SIGNAL SQLSTATE '75003' SET MESSAGE_TEXT='Native fence budget exceeded'; END IF;"
        lines = ['BEGIN NOT ATOMIC', 'DECLARE HELD SMALLINT DEFAULT 0;', 'DECLARE COPIED INTEGER;', 'DECLARE ACQUIRED TIMESTAMP(6);',
                 f'DECLARE EXIT HANDLER FOR SQLEXCEPTION {handler};', f'DECLARE EXIT HANDLER FOR SQLWARNING {handler};',
                 f"CALL QSYS2.QCMDEXC('{self.allocate}');", 'SET HELD=1;',
                 'SET ACQUIRED=CURRENT_TIMESTAMP;', "INSERT INTO SESSION.DCGFTIME VALUES('acquired',ACQUIRED);"]
        if delay:
            lines.append(f"CALL QSYS2.QCMDEXC('DLYJOB DLY({delay})');")
        for index, item in enumerate(self.batches):
            lines.append(budget)
            lines.extend(self.before_batch(item))
            lines.extend([budget, f'INSERT INTO {item["table"]} ' + item['sql'] + ';', budget,
                          'GET DIAGNOSTICS COPIED = ROW_COUNT;',
                          f"IF COPIED > {item['maximum_rows']} THEN SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT='Excessive native metadata'; END IF;",
                          f"INSERT INTO SESSION.DCGFTIME VALUES('q{index:03}',CURRENT_TIMESTAMP);"])
        if fault:
            lines.append("SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Owned fixture failure before release';")
        lines.extend(["INSERT INTO SESSION.DCGFTIME VALUES('collected',CURRENT_TIMESTAMP);", f"CALL QSYS2.QCMDEXC('{self.release}');", 'SET HELD=0;',
                      "INSERT INTO SESSION.DCGFTIME VALUES('released',CURRENT_TIMESTAMP);", 'END'])
        return '\n'.join(lines)

    def before_batch(self, batch):
        """Test-only same-job collection steps, inside the server release handlers."""
        return []

    def capture(self, directory, fault=False, delay=0, budget_ms=5000):
        directory = Path(directory)
        directory.mkdir()
        # Reuse this owned connection safely between attempts. These clears are
        # only session-temporary result tables, outside any source write lock.
        for name in [item['table'] for item in self.batches] + ['SESSION.DCGFTIME']:
            self.execute('DELETE FROM ' + name)
        sql = self.statement(fault, delay, budget_ms)
        (directory / 'statement.sql').write_text(sql)
        result = {'profile': 'online-file-fence-candidate-v1', 'status': 'collecting', 'startup_proof': False,
                  'library': self.library, 'targets': [{'library': self.library, 'file': name, 'member': self.members[name]} for name in self.tables],
                  'queries': [], 'started': census.stamp(), 'between_query_budget_ms': budget_ms}
        started = time.monotonic()
        try:
            self.execute(sql)
            result['call_seconds'] = time.monotonic() - started
            result['times'] = census.rows(self.connection, 'SELECT PHASE, OBSERVED FROM SESSION.DCGFTIME ORDER BY OBSERVED', maximum=len(self.batches) + 3)
            stages = [r for r in result['times'] if r['PHASE'] in ['acquired', 'collected', 'released']]
            if [r['PHASE'] for r in stages] != ['acquired', 'collected', 'released']:
                raise ValueError('Missing or ambiguous server timing stages')
            result['hold_seconds'] = (datetime.datetime.fromisoformat(str(stages[-1]['OBSERVED'])) - datetime.datetime.fromisoformat(str(stages[0]['OBSERVED']))).total_seconds()
            if result['hold_seconds'] < 0:
                raise ValueError('Server clock regressed')
            captured_rows = {}
            result['batches'] = []
            for batch in self.batches:
                batch_rows = census.rows(self.connection, 'SELECT * FROM ' + batch['table'], maximum=batch['maximum_rows'])
                captured_rows.update(split_batch_rows(batch_rows, batch['items']))
                result['batches'].append({'name': batch['items'][0]['name'], 'phase': batch['items'][0]['phase'], 'requests': [item['id'] for item in batch['items']]})
            for item in self.items:
                query = {k: item[k] for k in ['name', 'phase', 'parameters', 'sql']}
                query['rows'] = captured_rows[item['id']]
                if item['name'] in ['columns', 'keys']:
                    query['rows'].sort(key=lambda row: row['ORDINAL_POSITION'])
                result['queries'].append(query)
            self.complete_capture(result)
            result['status'] = 'released'
            return result
        except BaseException as error:
            result.update(status='rejected', error=str(error), call_seconds=time.monotonic() - started)
            raise
        finally:
            result['finished'] = census.stamp()
            census.save(directory / 'capture.json', result)

    def complete_capture(self, result):
        """Test collectors may add explicit evidence requirements before returning a candidate."""
        pass


def held_locks(observer, library, tables, job):
    result = []
    for table in tables:
        result.extend(census.rows(observer, "SELECT JOB_NAME, LOCK_STATE, LOCK_STATUS, MEMBER_NAME FROM TABLE(QSYS2.OBJECT_LOCK_INFO(?, ?, '*FILE', 0)) X "
                                 "WHERE JOB_NAME=? AND LOCK_STATE='*EXCLRD' AND LOCK_STATUS='HELD'", (library, table, job), 16))
    return result


def client_loss_child(directory, library, queries):
    """Owned test client only; parent verifies release without acting on its job."""
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(directory, library, 1)
    session = exp.connection()
    try:
        collector = Collector(session[0], library, ['A', 'B', 'M', 'U'], json.loads(Path(queries).read_text()))
        collector.prepare()
        census.save(exp.directory / 'ready.json', {'job': session[1]})
        collector.capture(exp.directory / 'capture', delay=1)
    finally:
        session[0].close()


def validate_capture(exp, directory):
    process = subprocess.run(exp.java_command('As400OnlineFenceEvidence', directory / 'capture.json', directory / 'validated'),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
    (directory / 'validate.log').write_text(exp.fixture.redact(process.stdout))
    if process.returncode:
        raise ValueError('Captured native binding validation failed')


def stage_runtime(exp):
    """Each run owns its classes and dependencies before the first source call."""
    command = exp.java_command('As400OnlineFenceEvidence')
    runtime = exp.directory / 'runtime'
    runtime.mkdir()
    paths = []
    files = {}
    for index, value in enumerate(command[2].split(':')):
        source = Path(value)
        if not source.exists() and source == ROOT / 'jt400/target/classes':
            continue  # This packaging-only module supplies its classes in the dependency jar.
        target = runtime / (str(index) + ('-' + source.name if source.is_file() else ''))
        if source.is_dir():
            shutil.copytree(source, target)
            copied = [p for p in target.rglob('*') if p.is_file()]
        elif source.is_file():
            shutil.copy2(source, target)
            copied = [target]
        else:
            raise ValueError('Missing runtime dependency before source access')
        paths.append(str(target))
        for path in copied:
            files[str(path.relative_to(runtime))] = hashlib.sha256(path.read_bytes()).hexdigest()
    census.save(runtime / 'manifest.json', {'files': files, 'java': command[0]})
    classpath = ':'.join(paths)
    exp.java_command = lambda entrypoint, *args: [command[0], '-cp', classpath, 'io.debezium.connector.db2as400.' + entrypoint, *map(str, args)]


def main():
    if len(sys.argv) != 3:
        raise SystemExit('Usage: dcg-online-fence-probe.py NEW_DIRECTORY DCNCdddd; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    import threading
    import traceback
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Server-captured binding metadata and boundary', startup_proof=False, observations=[])
    (exp.directory / 'harness.py').write_bytes(Path(__file__).read_bytes())
    exp.persist()
    holder = observer = collector = None
    try:
        stage_runtime(exp)
        exp.capture('preflight', 'preflight')
        exp.fixture.setup()
        exp.deadline = time.monotonic() + 480
        observer = exp.connection(read_only=True)
        holder = exp.connection()
        writer = exp.connection(True)
        # Native mixed layout, acquired under the lock; no integer-only output schema.
        exp.execute(holder, 'fixture-ddl', f'ALTER TABLE {exp.library}.M ADD COLUMN TXT VARCHAR(32) CCSID 37 '
                    'ADD COLUMN AMOUNT DECIMAL(10,2) ADD COLUMN FLAG SMALLINT')
        exp.capture('baseline', 'anchor')
        query_file = exp.directory / 'queries.json'
        subprocess.run(exp.java_command('As400OnlineFenceEvidence', 'queries', query_file), check=True, timeout=30)
        collector = Collector(holder[0], exp.library, ['A', 'B', 'M', 'U'], json.loads(query_file.read_text()))
        preparation = time.monotonic()
        collector.prepare()
        exp.report['prepare_seconds'] = time.monotonic() - preparation
        exp.writer_thread = threading.Thread(target=exp.independent_writer, name='owned-fence-writer', daemon=True)
        exp.writer_thread.start()
        exp.execute(writer, 'update', f'UPDATE {exp.library}.B SET ID=11 WHERE ID=1')

        def held():
            return held_locks(observer[0], exp.library, collector.tables, holder[1])

        def expect_busy(label):
            for attempt in range(5):
                try:
                    collector.capture(exp.directory / f'{label}-{attempt + 1}')
                    raise ValueError('Busy transaction incorrectly admitted')
                except pyodbc.Error as error:
                    if 'CPF1002' not in str(error) or held():
                        raise
                    if 'Cannot allocate object B.' in str(error):
                        exp.report['observations'].append({'case': label, 'status': 'rejected', 'remaining_locks': []})
                        return
                    # A's concurrent write may win first. Preserve that refusal,
                    # but it cannot satisfy the intended open-B regression.
                    if 'Cannot allocate object A.' not in str(error):
                        raise
                    exp.stop.wait(.2)
            raise ValueError('Open B conflict was not exercised within retry budget')

        def capture_released(label, **arguments):
            for attempt in range(5):
                try:
                    directory = exp.directory / f'{label}-{attempt + 1}'
                    return collector.capture(directory, **arguments), directory
                except pyodbc.Error as error:
                    if 'CPF1002' not in str(error) or held() or attempt == 4:
                        raise
                    exp.stop.wait(.2)

        expect_busy('busy')
        exp.execute(writer, 'rollback')
        for operation, sql in [('truncate', f'TRUNCATE TABLE {exp.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS'),
                               ('clear', f'DELETE FROM {exp.library}.B')]:
            exp.execute(writer, operation, sql)
            expect_busy('busy-' + operation)
            exp.execute(writer, 'rollback')
        captured = None
        for attempt in range(5):
            directory = exp.directory / f'candidate-{attempt + 1}'
            try:
                captured = collector.capture(directory)
                break
            except pyodbc.Error as error:
                if 'CPF1002' not in str(error) or held():
                    raise
                exp.stop.wait(.2)
        if captured is None or held():
            raise ValueError('No released candidate within retry budget')
        validate_capture(exp, directory)
        exp.report['candidate'] = str(directory.relative_to(exp.directory) / 'capture.json')
        exp.report['observations'].append({'case': 'metadata', 'status': 'validated-candidate', 'remaining_locks': []})
        for repeat in range(2):
            repeat_result, repeat_directory = capture_released(f'repeat-{repeat + 1}')
            if held():
                raise ValueError('Repeated capture retained a source lock')
            validate_capture(exp, repeat_directory)
            exp.report['observations'].append({'case': f'repeat-{repeat + 1}', 'status': 'released', 'hold_seconds': repeat_result['hold_seconds']})
        # The same full metadata path must release on an exception after collection.
        try:
            capture_released('injected-failure', fault=True)
            raise ValueError('Injected failure was hidden')
        except pyodbc.Error as error:
            if '75001' not in str(error):
                raise
        if held():
            raise ValueError('Failure left a source lock held')
        exp.report['observations'].append({'case': 'injected-failure', 'status': 'rejected', 'remaining_locks': []})
        try:
            capture_released('budget-expired', delay=1, budget_ms=100)
            raise ValueError('Expired collection budget was accepted')
        except pyodbc.Error as error:
            if '75003' not in str(error):
                raise
        if held():
            raise ValueError('Budget expiry left a source lock held')
        exp.report['observations'].append({'case': 'budget-expired', 'status': 'rejected', 'remaining_locks': []})

        # Kill only our local child after observing its source lock. Never ENDJOB
        # a disconnected server job; it may already be reused by another client.
        child_directory = exp.directory / 'client-loss'
        with (exp.directory / 'client-loss.log').open('w') as log:
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--client-loss-child',
                                      str(child_directory), exp.library, str(query_file)], stdout=log, stderr=subprocess.STDOUT)
            try:
                until = time.monotonic() + 30
                observed = []
                while time.monotonic() < until and child.poll() is None:
                    ready = child_directory / 'ready.json'
                    if ready.exists():
                        job = json.loads(ready.read_text())['job']
                        observed = held_locks(observer[0], exp.library, collector.tables, job)
                        if observed:
                            break
                    time.sleep(.05)
                if not observed:
                    raise ValueError('Client loss test never observed the owned lock')
                killed = time.monotonic()
                child.kill()
                child.wait(timeout=5)
                while time.monotonic() - killed < 12:
                    remaining = held_locks(observer[0], exp.library, collector.tables, job)
                    if not remaining:
                        break
                    time.sleep(.1)
                if remaining:
                    raise ValueError('Source retained client-loss locks beyond observation budget')
                exp.report['observations'].append({'case': 'client-loss', 'status': 'released', 'job': job,
                                                   'observed_locks': observed, 'remaining_locks': [],
                                                   'release_observed_seconds': time.monotonic() - killed, 'exit': child.returncode})
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
        exp.execute(writer, 'update', f'UPDATE {exp.library}.B SET ID=12 WHERE ID=1')
        exp.execute(writer, 'commit')
        exp.execute(writer, 'truncate', f'TRUNCATE TABLE {exp.library}.M REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(writer, 'mixed-insert', f"INSERT INTO {exp.library}.M VALUES(101,'fence',123.45,1)")
        exp.execute(writer, 'commit')
        exp.report['mixed_clear_committed'] = True
        exp.stop.set()
        exp.writer_thread.join(timeout=10)
        if exp.writer_thread.is_alive() or exp.background_error or not exp.progress:
            raise ValueError('Unrelated writer did not progress and drain')
        exp.capture('final', 'anchor')
        exp.capture('oracle', 'capture', exp.directory / 'baseline/capture.json', exp.directory / 'final/capture.json', 1048576)
        exp.report.update(status='captured-unqualified', unrelated_writes=len(exp.progress), network_failure_bound=False,
                          writer_calls=[item for item in exp.ledger if item['operation'] == 'unrelated-insert'])
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        exp.report['writer_calls'] = [item for item in exp.ledger if item['operation'] == 'unrelated-insert']
        # A client-side check is additional cleanup evidence, never the release protocol.
        if collector is not None:
            try:
                if held():
                    collector.execute("CALL QSYS2.QCMDEXC('" + collector.release + "')")
            except Exception as error:
                exp.report['release_error'] = exp.fixture.redact(str(error))
        exp.cleanup()
    print(str(exp.directory / 'report.json'), flush=True)
    if exp.report['status'] != 'captured-unqualified' or exp.report['fixture_cleanup']['status'] != 'passed' or exp.report['connection_cleanup_errors']:
        raise SystemExit('Candidate experiment or cleanup failed; inspect report')


if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1] == '--client-loss-child':
        client_loss_child(*sys.argv[2:])
    else:
        main()
