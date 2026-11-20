# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit isolated native pulse qualification; no migration or CDC publication.

Uses the existing fixture, inventory and native metadata validators. SQL journal
observations are diagnostics, never a substitute for strict RPC replay/audit.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback
import uuid


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


scope = load('scope', 'dcg-lock-scope-probe.py')
pulse = load('pulse', 'dcg-short-boundary.py')
fence, census = scope.fence, scope.census
OFFLOCK_PROFILE = 'offlock-native-metadata-candidate-v1'
MARKERS_SQL = """SELECT SEQUENCE_NUMBER,JOURNAL_CODE,JOURNAL_ENTRY_TYPE,OBJECT,OBJECT_TYPE,
 COMMIT_CYCLE,THREAD,JOB_NUMBER,JOB_USER,JOB_NAME,ENTRY_TIMESTAMP,RECEIVER_LIBRARY,RECEIVER_NAME,
 LENGTH(ENTRY_DATA) AS DATA_LENGTH,HEX(SUBSTR(ENTRY_DATA,1,512)) AS DATA_HEX
 FROM TABLE(QSYS2.DISPLAY_JOURNAL(?, ?,
 STARTING_RECEIVER_LIBRARY=>?, STARTING_RECEIVER_NAME=>?, STARTING_SEQUENCE=>?,
 ENDING_RECEIVER_LIBRARY=>?, ENDING_RECEIVER_NAME=>?, ENDING_SEQUENCE=>?, JOURNAL_CODES=>'U')) X
 WHERE SEQUENCE_NUMBER > ? ORDER BY SEQUENCE_NUMBER FETCH FIRST 33 ROWS ONLY"""
AUDIT_SQL = """SELECT SEQUENCE_NUMBER,JOURNAL_CODE,JOURNAL_ENTRY_TYPE,OBJECT,OBJECT_TYPE,
 COMMIT_CYCLE,THREAD,JOB_NUMBER,JOB_USER,JOB_NAME,ENTRY_TIMESTAMP,RECEIVER_LIBRARY,RECEIVER_NAME,
 LENGTH(ENTRY_DATA) AS DATA_LENGTH,HEX(SUBSTR(ENTRY_DATA,1,16000)) AS DATA_HEX
 FROM TABLE(QSYS2.DISPLAY_JOURNAL(?, ?,
 STARTING_RECEIVER_LIBRARY=>?, STARTING_RECEIVER_NAME=>?, STARTING_SEQUENCE=>?,
 ENDING_RECEIVER_LIBRARY=>?, ENDING_RECEIVER_NAME=>?, ENDING_SEQUENCE=>?,
 JOURNAL_CODES=>'*ALL', INCLUDE_INTERNAL=>'YES')) X
 WHERE SEQUENCE_NUMBER > ? ORDER BY SEQUENCE_NUMBER FETCH FIRST 2001 ROWS ONLY"""


def digest(value):
    return hashlib.sha256(json.dumps(value, default=str, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def qcommand(command):
    return 'CALL QSYS2.QCMDEXC(' + pulse.sql_literal(command) + ')'


def audit_marker(library, nonce, phase, scope_digest=None):
    pulse.exact(library, r'DCNC[0-9]{4}', 'Isolated library required')
    pulse.exact(nonce, r'[A-F0-9]{32}', 'Exact attempt required')
    if phase == 'A' and scope_digest is None:
        payload = f'DCGB1 {nonce} A'
    elif phase == 'Z':
        pulse.exact(scope_digest, r'[A-F0-9]{64}', 'Scope digest required')
        payload = f'DCGB1 {nonce} Z {scope_digest}'
    else:
        raise ValueError('Opening or closing audit marker required')
    force = '*YES' if phase == 'Z' else '*NO'
    command = (f'SNDJRNE JRN({library}/QSQJRN) TYPE(DN) ENTDTA({pulse.sql_literal(payload)}) '
               f'FILE(*NONE) FORCE({force}) OVRSTATE(*NONE) OVRRMTFTR(*NONE)')
    return payload, command


def point(connection, library):
    rows = census.rows(connection, 'SELECT ATTACHED_JOURNAL_RECEIVER_LIBRARY AS LIBRARY, '
                       'ATTACHED_JOURNAL_RECEIVER_NAME AS RECEIVER FROM QSYS2.JOURNAL_INFO '
                       'WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?', (library, 'QSQJRN'), 1)
    if len(rows) != 1 or rows[0]['LIBRARY'] != library:
        raise ValueError('Exact isolated attachment required')
    receiver = rows[0]['RECEIVER']
    pulse.exact(receiver, r'[A-Z][A-Z0-9_]{0,9}', 'Exact receiver required')
    ranges = census.rows(connection, 'SELECT LAST_SEQUENCE_NUMBER FROM QSYS2.JOURNAL_RECEIVER_INFO '
                         'WHERE JOURNAL_RECEIVER_LIBRARY=? AND JOURNAL_RECEIVER_NAME=? AND STATUS=\'ATTACHED\'',
                         (library, receiver), 1)
    if len(ranges) != 1 or ranges[0]['LAST_SEQUENCE_NUMBER'] is None:
        raise ValueError('Exact attached range required')
    return {'library': library, 'receiver': receiver, 'sequence': int(ranges[0]['LAST_SEQUENCE_NUMBER'])}


def metadata(exp, session, queries, directory):
    """All native queries run before admission or after independent pulse drain."""
    directory.mkdir()
    exp.execute(session, 'offlock-inventory', qcommand(scope.inventory_command(exp.library)))
    collector = scope.discover(exp, session[0], queries, directory / 'discovery')
    result = {'profile': OFFLOCK_PROFILE, 'outside_lock': True, 'startup_proof': False,
              'library': exp.library, 'targets': [{'library': exp.library, 'file': f, 'member': collector.members[f]}
                                                for f in collector.tables], 'queries': [], 'started': census.stamp()}
    observations = {}
    try:
        for item in collector.items:
            if exp.deadline is not None and time.monotonic() >= exp.deadline:
                raise TimeoutError('Off-lock metadata exceeded experiment budget')
            if item['name'] == 'scope':
                exp.execute(session, 'offlock-inventory', qcommand(scope.inventory_command(exp.library)))
            entry = {k: item[k] for k in ('name', 'phase', 'parameters', 'sql')}
            entry['rows'] = census.rows(session[0], item['sql'], item['parameters'], item['maximum_rows'])
            if item['name'] in ('columns', 'keys'):
                entry['rows'].sort(key=lambda row: row['ORDINAL_POSITION'])
            if item['name'] in ('scope', 'scope_counts'):
                observations.setdefault(item['phase'], {})['objects' if item['name'] == 'scope' else 'counts'] = entry['rows']
            else:
                result['queries'].append(entry)
        result['native_scope'] = {'discovered': collector.discovered, 'members': collector.discovered_members, **observations}
        result['status'] = 'observed'
    except BaseException:
        result['status'] = 'rejected'
        raise
    finally:
        result['finished'] = census.stamp()
        census.save(directory / 'capture.json', result)
    process = subprocess.run(exp.java_command('As400OnlineFenceEvidence', 'offlock', directory / 'capture.json', directory / 'validated'),
                             capture_output=True, text=True, timeout=30)
    (directory / 'validate.log').write_text(exp.fixture.redact(process.stdout + process.stderr))
    if process.returncode:
        raise ValueError('Off-lock metadata rejected by native validators')
    return result


def same_metadata(before, after):
    # The Java validator has independently checked each full bundle, including
    # recording settings and ranges. Compare bindings, identities and scope across
    # the pulse; current attached LAST_SEQUENCE_NUMBER is expected to advance.
    def stable(bundle):
        return {'targets': bundle['targets'], 'native_scope': bundle['native_scope'],
                'queries': [q for q in bundle['queries'] if q['name'] not in ('journal', 'receivers')],
                'journal': [{k: v for k, v in q['rows'][0].items() if k != 'CURRENT_SEQUENCE_NUMBER'}
                            for q in bundle['queries'] if q['name'] == 'journal'],
                'receivers': [[{k: v for k, v in row.items() if k != 'LAST_SEQUENCE_NUMBER'} for row in q['rows']]
                              for q in bundle['queries'] if q['name'] == 'receivers']}
    if stable(before) != stable(after):
        raise ValueError('Scope, schema, identity or recording profile changed across pulse')


def audit_diagnostic(rows, marker_sequences, start, end):
    """Conservative invalidation, not a journal visibility/completeness proof.

    Recognized row/transaction records are not interpreted or replayed here.
    Every other non-marker entry invalidates this first candidate profile. In
    particular, equal end states cannot erase an observed maintenance transition.
    Raw pointer/continuation validation still belongs to the strict RPC reader.
    """
    if (not 0 < len(rows) <= 2000 or not marker_sequences
            or start['library'] != end['library'] or start['receiver'] != end['receiver']):
        raise ValueError('Bounded same-receiver SQL audit required')
    invalidating = []
    seen, markers = [], []
    for row in rows:
        sequence = row['SEQUENCE_NUMBER']
        if (sequence is None or isinstance(sequence, bool) or int(sequence) != sequence
                or not start['sequence'] < int(sequence) <= end['sequence']
                or row['RECEIVER_LIBRARY'].rstrip(' ') != start['library']
                or row['RECEIVER_NAME'].rstrip(' ') != start['receiver']):
            raise ValueError('Malformed audit coordinate')
        sequence = int(sequence)
        seen.append(sequence)
        length = row['DATA_LENGTH']
        if type(length) is not int or not 0 <= length <= 16000:
            raise ValueError('Incomplete SQL audit payload; not strict replay')
        encoded = row['DATA_HEX']
        if not isinstance(encoded, str) or len(encoded) != 32000:
            raise ValueError('Truncated SQL audit bytes')
        raw = bytes.fromhex(encoded)
        if len(raw) != 16000 or any(raw[length:]):
            raise ValueError('Incomplete or nonzero-padded SQL audit bytes')
        kind = row['JOURNAL_CODE'] + '/' + row['JOURNAL_ENTRY_TYPE']
        if kind == 'U/DN':
            markers.append(sequence)
        elif kind not in {'R/PT', 'R/PX', 'R/UB', 'R/UP', 'R/DL', 'C/SC', 'C/CM', 'C/RB'}:
            invalidating.append({'sequence': sequence, 'type': kind})
    if seen != sorted(set(seen)) or markers != marker_sequences:
        raise ValueError('Missing, duplicate, reordered or foreign marker in SQL audit')
    return {'status': 'invalidated' if invalidating else 'no-invalidation-observed-unqualified',
            'invalidating': invalidating, 'startup_proof': False, 'maintenance_visibility_proven': False,
            'strict_rpc_replay': False, 'entry_count': len(rows)}


def check_rpc_markers(directory, expected, sequences, job, thread_hex, start, end):
    capture = json.loads((directory / 'capture.json').read_text())
    if capture.get('status') != 'audit-captured-unqualified' or capture.get('startup_proof') is not False:
        raise ValueError('Test-only raw audit required')
    markers = [entry for entry in capture['entries']
               if int(entry['position']['sequence']) > start['sequence'] and entry['type'].startswith('U/')]
    if [int(e['position']['sequence']) for e in markers] != sequences or len(markers) != len(expected):
        raise ValueError('SQL and raw RPC marker coordinates disagree')
    for entry, payload in zip(markers, expected):
        coordinate, producer = entry['position'], entry['record']
        if (coordinate['library'] != start['library'] or coordinate['receiver'] != start['receiver']
                or not start['sequence'] < int(coordinate['sequence']) <= end['sequence']
                or entry['type'] != 'U/DN' or entry['cycle'] != '0'
                or any(entry.get(k) != '' for k in ['table', 'library', 'member'])
                or '/'.join(producer[k] for k in ['jobNumber', 'jobUser', 'jobName']) != job
                or f"{int(producer['threadId']):016X}" != thread_hex):
            raise ValueError('Raw marker identity, producer or cycle differs')
        name = entry['file']
        pulse.exact(name, r'entry-[0-9]+\.bin', 'Owned capture filename required')
        raw = (directory / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry['sha256']:
            raise ValueError('Raw marker bytes changed after capture')
        offset, limit = entry['entry_specific_data_offset'], entry['entry_end_offset']
        if type(offset) is not int or type(limit) is not int or not 236 <= offset <= limit == len(raw):
            raise ValueError('Raw marker payload bounds disagree')
        if rpc_payload(raw, offset, limit) != payload.encode('cp037'):
            raise ValueError('Raw marker payload differs from SQL/native command')
    return {'status': 'raw-markers-matched-unqualified', 'startup_proof': False,
            'maintenance_visibility_proven': False, 'sequences': sequences}


def rpc_payload(raw, offset, limit):
    # RJNE0200's entry-specific data starts with a Zoned(5,0) length and 11
    # reserved bytes; data follows at +16. The next entry can add alignment bytes.
    # This bounded marker profile accepts only the observed positive F-zone digits,
    # zero reserved/alignment bytes, and no incomplete/minimized-data flags.
    if offset + 16 > limit or raw[218] & 0x2A:
        raise ValueError('Incomplete/minimized raw marker or missing data prefix')
    digits = raw[offset:offset + 5]
    if any(not 0xF0 <= digit <= 0xF9 for digit in digits) or any(raw[offset + 5:offset + 16]):
        raise ValueError('Unsupported raw marker length prefix')
    length = int(digits.decode('cp037'))
    end = offset + 16 + length
    if not 0 < length <= 512 or not end <= limit <= end + 15 or any(raw[end:limit]):
        raise ValueError('Raw marker length or alignment is inconsistent')
    return raw[offset + 16:end]


def marker_bytes(row):
    length = row.get('DATA_LENGTH')
    if type(length) is not int or not 0 < length <= 512:
        raise ValueError('Excessive or missing marker bytes')
    raw = bytes.fromhex(row['DATA_HEX'])
    # DISPLAY_JOURNAL SUBSTR of a BLOB pads to the fixed requested length on the
    # qualified server. Retain the original field; accept only exact zero padding.
    if len(raw) != 512 or any(raw[length:]):
        raise ValueError('Truncated or nonzero-padded marker bytes')
    return raw[:length]


def check_markers(rows, expected, job, thread_hex, start, end):
    """SQL diagnostic only. Do not invent raw pointer/header fields for this data."""
    if start['library'] != end['library'] or start['receiver'] != end['receiver']:
        raise ValueError('Receiver rotation rejects this pulse profile')
    if len(rows) != len(expected):
        raise ValueError('Missing, duplicate or foreign U entry')
    sequences = []
    for row, payload in zip(rows, expected):
        def name(key, maximum):
            value = row.get(key)
            if not isinstance(value, str) or not 0 < len(value) <= maximum:
                raise ValueError('Missing or excessive native marker name')
            return value.rstrip(' ')
        if (row.get('JOURNAL_CODE') != 'U' or row.get('JOURNAL_ENTRY_TYPE') != 'DN'
                or name('RECEIVER_LIBRARY', 10) != start['library'] or name('RECEIVER_NAME', 10) != start['receiver']
                or '/'.join(name(k, n) for k, n in [('JOB_NUMBER', 6), ('JOB_USER', 10), ('JOB_NAME', 10)]) != job
                or type(row.get('COMMIT_CYCLE')) is not int or row['COMMIT_CYCLE'] != 0
                or any(k not in row or row[k] is not None and (not isinstance(row[k], str) or row[k].strip(' '))
                       for k in ('OBJECT', 'OBJECT_TYPE'))):
            raise ValueError('Foreign or associated marker')
        thread = row.get('THREAD')
        if type(thread) is not int or thread <= 0 or f'{thread:016X}' != thread_hex:
            raise ValueError('Marker thread differs from independently read holder thread')
        sequence = row.get('SEQUENCE_NUMBER')
        if type(sequence) is not int or not start['sequence'] < sequence <= end['sequence']:
            raise ValueError('Marker outside the bounded interval')
        sequences.append(sequence)
        if marker_bytes(row) != payload.encode('cp037'):
            raise ValueError('Native marker bytes differ from explicit CCSID 37 fixture encoding')
    if sequences != sorted(set(sequences)):
        raise ValueError('Marker order is ambiguous')
    return {'status': 'sql-marker-diagnostic-matched', 'startup_proof': False, 'strict_rpc_replay': False,
            'maintenance_visibility_proven': False, 'sequences': sequences}


def run_pulse(exp, holder, observer, program, directory, fault=False, wait_seconds=20, contention_delay=False):
    """Only the worker invokes the server-owned pulse; no host work inside it.

    Timeout is an observation deadline, not cancellation. No metadata/result read
    or connection close is allowed while the worker is alive. The fixture cleanup
    helper also refuses deletion while this registered worker remains active.
    """
    if type(wait_seconds) not in (int, float) or not 0 < wait_seconds <= 20:
        raise ValueError('Bounded pulse observation deadline required')
    if type(contention_delay) is not bool or contention_delay and fault:
        raise ValueError('Separate explicit contention-only instrumentation required')
    directory.mkdir()
    table_names = [file for file, member in program.targets]
    if fence.held_locks(observer[0], exp.library, table_names, holder[1]):
        raise ValueError('Holder already has target allocations')
    exp.execute(holder, 'prepare-pulse-results', pulse.PREPARE_SQL)
    sql = program.statement()
    if fault:
        marker_call = 'CALL QSYS2.QCMDEXC(' + pulse.sql_literal(program.marker) + ');'
        if sql.count(marker_call) != 1:
            raise ValueError('Cannot locate the single marker fault site')
        sql = sql.replace(marker_call, marker_call + "\nSIGNAL SQLSTATE '75011' SET MESSAGE_TEXT='Owned post-marker fault';")
    if contention_delay:
        # NOT the minimal program or healthy latency measurement. This owned
        # one-second test delay permits deterministic observation of a held lock
        # before starting the competing maintenance operation. No metadata is added.
        marker_call = 'CALL QSYS2.QCMDEXC(' + pulse.sql_literal(program.marker) + ');'
        sql = sql.replace(marker_call, marker_call + "\nCALL QSYS2.QCMDEXC('DLYJOB DLY(1)');")
    (directory / 'statement.sql').write_text(sql + '\n')
    report = {'status': 'running', 'startup_proof': False, 'program_sha256': hashlib.sha256(sql.encode()).hexdigest(),
              'fault_after_marker': fault, 'contention_only_delay_seconds': int(contention_delay), 'job': holder[1], 'started': census.stamp()}

    def execute():
        try:
            exp.execute(holder, 'pulse', sql)
            report['response'] = 'success'
        except BaseException as error:
            report.update(response='failed', error=exp.fixture.redact(str(error)))
        finally:
            report['finished'] = census.stamp()

    worker = threading.Thread(target=execute, name='owned-short-pulse', daemon=True)
    exp.writer_thread = worker
    worker.start()
    worker.join(timeout=wait_seconds)
    if worker.is_alive():
        report.update(status='rejected', reason='Pulse response deadline exceeded; drain unknown', holder_drained=False)
        census.save(directory / 'pulse.json', report)
        raise TimeoutError(report['reason'])
    report['call_seconds'] = (report['finished']['monotonic_ns'] - report['started']['monotonic_ns']) / 1e9
    try:
        remaining = fence.held_locks(observer[0], exp.library, table_names, holder[1])
        report['remaining_locks'] = remaining
        report['holder_drained'] = not remaining
        if remaining:
            raise ValueError('Returned pulse still holds source allocations')
        report['result_rows'] = census.rows(holder[0], 'SELECT ATTEMPT,SCOPE_DIGEST,ACQUIRED,RELEASED FROM SESSION.DCGPULSE', maximum=1)
        if report['response'] == 'success':
            if len(report['result_rows']) != 1:
                raise ValueError('Missing or duplicate pulse timing')
            row = report['result_rows'][0]
            if row['ATTEMPT'] != program.nonce or row['SCOPE_DIGEST'] != program.scope_digest:
                raise ValueError('Foreign pulse timing')
            report['hold_seconds'] = (row['RELEASED'] - row['ACQUIRED']).total_seconds()
            if not 0 <= report['hold_seconds'] - int(contention_delay) <= 1:
                raise ValueError('Pulse exceeds the experimental one-second healthy-hold target')
        elif report['result_rows']:
            raise ValueError('Failed pulse left a success timing row')
        report['status'] = 'returned-and-locks-absent'
        return report
    except BaseException as error:
        report.update(status='rejected', error=exp.fixture.redact(str(error)))
        raise
    finally:
        census.save(directory / 'pulse.json', report)


def run_contention(exp, queries, metadata_session, observer):
    """Reproduce the maintenance conflict with a labeled, non-candidate delay variant."""
    for label, command in [('alter', f'ALTER TABLE {exp.library}.B ADD COLUMN NOTE INTEGER'),
                           ('end-journaling', qcommand(f'ENDJRNPF FILE({exp.library}/B)'))]:
        directory = exp.directory / label
        directory.mkdir()
        before = metadata(exp, metadata_session, queries, directory / 'before')
        program = pulse.Pulse(exp.library, [(t['file'], t['member']) for t in before['targets']], uuid.uuid4().hex.upper(), digest(before))
        holder, actor, pulse_observer = exp.connection(), exp.connection(), exp.connection(read_only=True)
        result = {'case': label, 'startup_proof': False, 'contention_only_delay_seconds': 1}

        def collect():
            try:
                result['pulse'] = run_pulse(exp, holder, pulse_observer, program, directory / 'pulse', contention_delay=True)
            except BaseException as error:
                result['pulse_error'] = exp.fixture.redact(str(error))

        controller = threading.Thread(target=collect, name='contention-controller', daemon=True)
        exp.churn_thread = controller  # Existing cleanup refuses close/drop while it remains active.
        controller.start()
        try:
            deadline = time.monotonic() + 12
            observed = []
            while controller.is_alive() and time.monotonic() < deadline:
                observed = fence.held_locks(observer[0], exp.library, ['B'], holder[1])
                if observed:
                    break
                time.sleep(.02)
            if not observed:
                raise ValueError('No observed lock overlap; contention case is unqualified')
            result['observed_holder_locks'] = observed
            result['actor_before'] = str(census.rows(actor[0], 'SELECT CURRENT_TIMESTAMP AS T FROM SYSIBM.SYSDUMMY1', maximum=1)[0]['T'])
            try:
                exp.execute(actor, label, command)
                result['actor_status'] = 'completed'
            except Exception as error:
                result.update(actor_status='rejected', actor_error=exp.fixture.redact(str(error)))
            result['actor_after'] = str(census.rows(actor[0], 'SELECT CURRENT_TIMESTAMP AS T FROM SYSIBM.SYSDUMMY1', maximum=1)[0]['T'])
        finally:
            controller.join(timeout=25)
            census.save(directory / 'contention.json', result)
        if controller.is_alive() or 'pulse_error' in result or result.get('pulse', {}).get('response') != 'success':
            raise ValueError('Contention pulse did not complete and drain successfully')
        times = result['pulse']['result_rows'][0]
        start, end = str(times['ACQUIRED']), str(times['RELEASED'])
        if not start <= result['actor_before'] < end:
            raise ValueError('Actor did not start during the observed held interval')
        if result['actor_status'] == 'completed':
            if result['actor_after'] < end:
                raise ValueError('Maintenance completed before allocation release')
            result['status'] = 'completion-after-release-observed'
        elif any(code in result.get('actor_error', '') for code in ('CPF7031', 'SQL0913', 'CPF3202', 'CPF3203', 'CPF9803')):
            result['status'] = 'native-conflict-observed'
        else:
            raise ValueError('Maintenance failed for an unrelated reason')
        census.save(directory / 'contention.json', result)
        exp.report['observations'].append(result)
        exp.persist()
        holder[0].close()
        exp.connections.remove(holder[0])
        if label == 'end-journaling' and result['actor_status'] == 'completed':
            exp.execute(actor, 'restore-owned-journaling', qcommand(f'STRJRNPF FILE({exp.library}/B) JRN({exp.library}/QSQJRN) IMAGES(*BOTH) OMTJRNE(*OPNCLO)'))


def attempt(exp, queries, metadata_session, observer, label, *, fault=False, busy=False, after_pulse=None, expect_invalid=False, discover_history=False):
    if exp.deadline is not None and time.monotonic() >= exp.deadline:
        raise TimeoutError('Native attempt budget exhausted')
    directory = exp.directory / label
    directory.mkdir()
    holder = exp.connection()
    job = census.rows(holder[0], 'SELECT QSYS2.JOB_NAME AS JOB,HEX(QSYS2.THREAD_ID) AS THREAD_HEX FROM SYSIBM.SYSDUMMY1', maximum=1)
    if len(job) != 1 or job[0]['JOB'] != holder[1]:
        raise ValueError('Holder identity changed before pulse')
    start = point(observer[0], exp.library)
    nonce = uuid.uuid4().hex.upper()
    opening, command = audit_marker(exp.library, nonce, 'A')
    exp.execute(holder, 'opening-marker', qcommand(command))
    if discover_history:
        exp.capture(label + '/pending', 'pending')
        exp.capture(label + '/native-scope-before', 'scope')
    before = metadata(exp, metadata_session, queries, directory / 'before')
    if discover_history:
        native_scope = json.loads((directory / 'native-scope-before/capture.json').read_text())
        if native_scope.get('status') != 'captured' or native_scope.get('targets') != before['targets']:
            raise ValueError('Production native discovery differs from independent complete lock scope')
    program = pulse.Pulse(exp.library, [(t['file'], t['member']) for t in before['targets']], nonce, digest(before))
    census.save(directory / 'request.json', {'library': exp.library, 'targets': program.targets,
                                            'nonce': nonce, 'evidence_sha256': program.evidence_sha256})
    outcome = run_pulse(exp, holder, observer, program, directory / 'pulse', fault)
    if after_pulse is not None:
        # This hook is deliberately AFTER the registered worker returned and
        # independent lock absence was established, never inside the pulse.
        after_pulse()
    after = metadata(exp, metadata_session, queries, directory / 'after')
    if discover_history:
        exp.capture(label + '/native-scope-after', 'scope')
        native_after = json.loads((directory / 'native-scope-after/capture.json').read_text())
        if (native_after.get('status') != 'captured' or native_after.get('targets') != after['targets']
                or any(native_scope.get(k) != native_after.get(k) for k in ['scope', 'journaled_libraries', 'targets', 'acquired_binding_count'])):
            raise ValueError('Production native scope changed across pulse')
    closing, command = audit_marker(exp.library, nonce, 'Z', program.scope_digest)
    exp.execute(holder, 'closing-force-marker', qcommand(command))
    end = point(observer[0], exp.library)
    census.save(directory / 'interval.json', {'from': start, 'through': end, 'holder': job})
    if start['receiver'] != end['receiver']:
        raise ValueError('Receiver rotated during pulse attempt')
    # The API start is inclusive and must name an existing entry; our attempt
    # interval is (start,end]. Exclude exactly that already-existing anchor in SQL,
    # including a previous attempt's Z, without filtering any later foreign U entry.
    params = [exp.library, 'QSQJRN', exp.library, start['receiver'], start['sequence'], exp.library, end['receiver'], end['sequence'], start['sequence']]
    rows = census.rows(observer[0], MARKERS_SQL, params, 32)
    # Normalize exact integral DECIMAL coordinates only; never substitute IDs.
    for row in rows:
        for key in ('SEQUENCE_NUMBER', 'COMMIT_CYCLE'):
            value = row[key]
            if value is None or int(value) != value:
                raise ValueError('Missing or non-integral marker coordinate')
            row[key] = int(value)
    census.save(directory / 'markers-sql.json', {'sql': MARKERS_SQL, 'parameters': params, 'rows': rows,
                                               'strict_rpc_replay': False, 'startup_proof': False})
    expected = [opening, *([] if busy else [program.payload]), closing]
    checked = check_markers(rows, expected, holder[1], job[0]['THREAD_HEX'], start, end)
    audit_rows = census.rows(observer[0], AUDIT_SQL, params, 2000)
    census.save(directory / 'audit-sql.json', {'sql': AUDIT_SQL, 'parameters': params, 'rows': audit_rows,
                                            'strict_rpc_replay': False, 'startup_proof': False})
    audit = audit_diagnostic(audit_rows, checked['sequences'], start, end)
    reasons = []
    try:
        same_metadata(before, after)
    except ValueError as error:
        reasons.append(str(error))
    if audit['invalidating']:
        reasons.append('Maintenance or unsupported journal entries in audit interval')
    checked.update(audit=audit, invalidation_reasons=reasons)
    if bool(reasons) != expect_invalid:
        census.save(directory / 'result.json', checked)
        raise ValueError('Unexpected boundary invalidation result: ' + str(reasons))
    if (outcome['response'] == 'success') != (not fault and not busy):
        raise ValueError('Unexpected pulse command outcome')
    # These are explicit SQL-observed bounds, not forged raw anchor certificates.
    census.save(directory / 'raw-start.json', {'from': start, 'coordinate_source': 'QSYS2.JOURNAL_RECEIVER_INFO', 'startup_proof': False})
    census.save(directory / 'raw-end.json', {'through': end, 'coordinate_source': 'QSYS2.JOURNAL_RECEIVER_INFO', 'startup_proof': False})
    try:
        exp.capture(label + '/raw', 'audit', directory / 'raw-start.json', directory / 'raw-end.json', 1048576)
        checked['raw_markers'] = check_rpc_markers(directory / 'raw', expected, checked['sequences'], holder[1], job[0]['THREAD_HEX'], start, end)
    except RuntimeError:
        rejected = json.loads((directory / 'raw/capture.json').read_text())
        if not expect_invalid or rejected.get('status') != 'rejected' or 'Pointer-backed journal data requires a different retrieval profile' not in rejected.get('error', ''):
            raise
        checked['raw_audit'] = 'rejected-pointer-backed-data; SQL diagnostic cannot replace it'
    checked.update(pulse=outcome, case=label)
    if discover_history:
        floor_file = directory / 'discovered-floor.json'
        subprocess.run(exp.java_command('As400NativeHistoryDiscoveryProbe', directory / 'pending/capture.json', directory / 'raw', floor_file),
                       check=True, timeout=30)
        checked['discovered_history'] = json.loads(floor_file.read_text())
    census.save(directory / 'result.json', checked)
    holder[0].close()
    exp.connections.remove(holder[0])
    return checked


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] != 'contention':
        raise SystemExit('Usage: dcg-short-boundary-probe.py NEW_DIRECTORY DCNCdddd [contention]; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    exp = census.Experiment(sys.argv[1], sys.argv[2], 1)
    exp.report.update(experiment='Minimal native lock-marker-release pulse', qualification='unqualified', observations=[])
    for filename in ('dcg-short-boundary-probe.py', 'dcg-short-boundary.py'):
        (exp.directory / filename).write_bytes(Path(__file__).with_name(filename).read_bytes())
    try:
        fence.stage_runtime(exp)
        exp.capture('preflight', 'preflight')
        exp.report['source_connected'] = True
        exp.fixture.setup()
        exp.deadline = time.monotonic() + 480
        metadata_session = exp.connection()
        observer = exp.connection(read_only=True)
        exp.execute(metadata_session, 'extra-file', f'CREATE TABLE {exp.library}.EXTRA(ID INTEGER NOT NULL PRIMARY KEY)')
        exp.fixture.TABLES = [*exp.fixture.TABLES, 'EXTRA']
        exp.fixture.owned_objects = exp.fixture.inventory('objects-with-extra.json')
        exp.execute(metadata_session, 'extra-row', f'INSERT INTO {exp.library}.EXTRA VALUES (1)')
        exp.execute(metadata_session, 'actual-member', qcommand(f'RNMM FILE({exp.library}/EXTRA) MBR(EXTRA) NEWMBR(DATA)'))
        query_file = exp.directory / 'queries.json'
        subprocess.run(exp.java_command('As400OnlineFenceEvidence', 'queries', query_file), check=True, timeout=30)
        queries = json.loads(query_file.read_text())
        scope.prepare_inventory(exp, metadata_session, queries)
        if len(sys.argv) == 4:
            run_contention(exp, queries, metadata_session, observer)
        else:
            for number in range(3):
                exp.report['observations'].append(attempt(exp, queries, metadata_session, observer, f'healthy-{number + 1}'))
                exp.persist()
            exp.report['observations'].append(attempt(exp, queries, metadata_session, observer, 'post-marker-error', fault=True))
            writer = exp.connection(transaction=True)
            exp.execute(writer, 'open-update', f'UPDATE {exp.library}.B SET ID=11 WHERE ID=1')
            try:
                exp.report['observations'].append(attempt(exp, queries, metadata_session, observer, 'busy-update', busy=True))
            finally:
                exp.execute(writer, 'rollback')
            def new_file(transient):
                exp.execute(metadata_session, 'race-new-file', f'CREATE TABLE {exp.library}.NEWFILE(ID INTEGER NOT NULL PRIMARY KEY)')
                exp.fixture.TABLES = [*exp.fixture.TABLES, 'NEWFILE']
                exp.fixture.owned_objects = exp.fixture.inventory('objects-with-race-file.json')
                if transient:
                    exp.execute(metadata_session, 'race-drop-file', f'DROP TABLE {exp.library}.NEWFILE')
                    exp.fixture.TABLES = [t for t in exp.fixture.TABLES if t != 'NEWFILE']
                    exp.fixture.owned_objects = exp.fixture.inventory('objects-after-race-drop.json')
            exp.report['observations'].append(attempt(exp, queries, metadata_session, observer, 'transient-create-drop',
                                                     after_pulse=lambda: new_file(True), expect_invalid=True))
            exp.report['observations'].append(attempt(exp, queries, metadata_session, observer, 'new-file-membership',
                                                     after_pulse=lambda: new_file(False), expect_invalid=True))
        exp.report['status'] = 'captured-unqualified'
    except BaseException as error:
        (exp.directory / 'failure.txt').write_text(exp.fixture.redact(traceback.format_exc()))
        exp.report.update(status='failed', error=exp.fixture.redact(str(error)))
    finally:
        exp.cleanup()
    print(exp.directory / 'report.json', flush=True)
    if exp.report['status'] != 'captured-unqualified' or exp.report.get('fixture_cleanup', {}).get('status') != 'passed' or exp.report.get('connection_cleanup_errors'):
        raise SystemExit('Native pulse experiment or cleanup failed; retained evidence identifies the gate')


if __name__ == '__main__':
    main()
