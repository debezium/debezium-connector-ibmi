# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Execute the emitted pulse's restricted control flow locally, never native SQL.

This deliberately small independent reader rejects any statement outside the
generator's grammar. Fault schedules exercise the emitted handlers and ordering,
not a second handwritten allocate/release loop. It is not a Db2 SQL emulator or
evidence that IBM i accepts this program or implements the modeled conditions.
"""
import copy
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('short_boundary', Path(__file__).with_name('dcg-short-boundary.py'))
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)


def pulse(**changes):
    return boundary.Pulse(**({'library': 'DCNC9999', 'targets': [('A', 'A'), ('EXTRA', 'DATA')],
                             'nonce': '0123456789ABCDEF' * 2, 'evidence_sha256': 'a' * 64} | changes))


class Condition(Exception):
    def __init__(self, category, operation):
        super().__init__(category, operation)
        self.category = category


class ProgramModel:
    """Read the generated statements, including nested EXIT handler scope."""
    def __init__(self, program, faults=None, sql=None):
        self.program = program
        self.lines = iter((sql if sql is not None else program.statement()).splitlines())
        self.state, self.trace, self.result_rows = {}, [], []
        self.faults, self.held, self.marker_present = faults or {}, False, False
        self.release_calls = 0
        if next(self.lines) != 'BEGIN NOT ATOMIC':
            raise AssertionError('Expected dynamic compound')
        self.tree = self.parse('END')
        if next(self.lines, None) is not None:
            raise AssertionError('Trailing statements')

    def parse(self, end):
        body, handlers = [], {}
        for line in self.lines:
            if line == end:
                return handlers, body
            if line == 'BEGIN':
                body.append(('block', self.parse('END;')))
            elif line.startswith('IF ') and line.endswith(' THEN'):
                body.append(('if', line[3:-5], self.parse('END IF;')))
            elif match := re.fullmatch(r'DECLARE EXIT HANDLER FOR (SQLEXCEPTION|SQLWARNING|NOT FOUND) (SET \w+=1;)', line):
                if match[1] in handlers:
                    raise AssertionError('Duplicate handler')
                handlers[match[1]] = match[2]
            elif match := re.fullmatch(r'DECLARE (\w+) (SMALLINT DEFAULT 0|TIMESTAMP\(6\));', line):
                self.state[match[1]] = 0 if match[2].startswith('SMALLINT') else None
            else:
                body.append(('statement', line))
        raise AssertionError('Unterminated block')

    def step(self, operation, effect):
        self.trace.append((operation, self.held))
        fault = self.faults.get(operation)
        if fault and not fault[1]:
            raise Condition(fault[0], operation)
        effect()
        if fault:
            raise Condition(fault[0], operation)

    def native(self, command):
        if command == self.program.allocate:
            self.step('allocate', lambda: setattr(self, 'held', True))
        elif command == self.program.marker:
            if not self.held:
                raise AssertionError('Marker without allocations')
            self.step('marker', lambda: setattr(self, 'marker_present', True))
        elif command == self.program.release:
            self.release_calls += 1
            self.step('release', lambda: setattr(self, 'held', False))
        else:
            raise AssertionError('Unapproved native command')

    def statement(self, line):
        if match := re.fullmatch(r'SET (\w+)=([01]);', line):
            self.state[match[1]] = int(match[2])
        elif match := re.fullmatch(r'SET (ACQUIRED|RELEASED)=CURRENT_TIMESTAMP;', line):
            key = match[1]
            value = '2026-11-09 12:00:00.000001' if key == 'ACQUIRED' else '2026-11-09 12:00:00.000002'
            self.step(key.lower(), lambda: self.state.__setitem__(key, value))
        elif match := re.fullmatch(r"CALL QSYS2.QCMDEXC\('((?:''|[^'])*)'\);", line):
            self.native(match[1].replace("''", "'"))
        elif match := re.fullmatch(r"INSERT INTO SESSION.DCGPULSE VALUES\('([A-F0-9]{32})', '([A-F0-9]{64})', ACQUIRED, RELEASED\);", line):
            if self.held:
                raise AssertionError('Result bookkeeping while allocations held')
            self.step('result', lambda: self.result_rows.append({'ATTEMPT': match[1], 'SCOPE_DIGEST': match[2],
                                                               'ACQUIRED': self.state['ACQUIRED'], 'RELEASED': self.state['RELEASED']}))
        elif line in ("SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Pulse failed; independent drain required';",
                      "SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT='Pulse release failed; independent drain required';"):
            raise Condition('SQLEXCEPTION', 'pulse rejected')
        else:
            raise AssertionError('Statement outside the minimal pulse grammar: ' + line)

    def run_block(self, node):
        handlers, body = node
        try:
            for item in body:
                if item[0] == 'block':
                    self.run_block(item[1])
                elif item[0] == 'if':
                    operator = ' OR ' if ' OR ' in item[1] else ' AND '
                    clauses = item[1].split(operator)
                    if any(not re.fullmatch(r'\w+=[01]', clause) for clause in clauses):
                        raise AssertionError('Unsupported condition')
                    values = [self.state[clause[:-2]] == int(clause[-1]) for clause in clauses]
                    if (any(values) if operator == ' OR ' else all(values)):
                        self.run_block(item[2])
                else:
                    self.statement(item[1])
        except Condition as error:
            if error.category not in handlers:
                raise
            # EXIT: handler actions are outside their own declaration's scope.
            self.statement(handlers[error.category])

    def run(self):
        self.run_block(self.tree)
        return self


def marker_fixture(program):
    context = {'journal_identity': b'owned-journal-native-identity', 'receiver_identity': b'owned-receiver-native-identity',
               'receiver': 'QSQJRN0001', 'job': '123456/TESTUSR/QZDASOINIT', 'thread': b'\x00' * 7 + b'\x01',
               'lower_sequence': 100, 'upper_sequence': 105}
    record = {'journal_library': program.library, 'journal': 'QSQJRN', 'receiver_library': program.library,
              **{key: value for key, value in context.items() if key not in ('lower_sequence', 'upper_sequence')},
              'code': 'U', 'type': 'DN', 'file': None, 'member': None, 'commit_cycle': 0, 'ccsid': 37,
              'sequence': 102, 'payload': program.payload.encode('cp037'), 'data_length': len(program.payload),
              'truncated': False, 'pointer_backed': False, 'warnings': []}
    completion = {'nonce': program.nonce, 'scope_digest': program.scope_digest, 'program_sha256': program.program_sha256,
                  'response': 'success', 'warnings': [], 'holder_drained': True, 'locks_absent': True}
    return [record], context, completion, ProgramModel(program).run().result_rows


class ProgramTest(unittest.TestCase):
    def test_offline_cli_generates_inspectable_sql_without_native_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            request = Path(directory) / 'request.json'
            request.write_text(json.dumps({'library': 'DCNC9999', 'targets': [['A', 'DATA']],
                                           'nonce': 'F' * 32, 'evidence_sha256': 'a' * 64}))
            result = subprocess.run([sys.executable, str(Path(boundary.__file__).resolve()), str(request)],
                                    cwd=directory, check=True, capture_output=True, text=True)
            artifact = json.loads(result.stdout)
            self.assertEqual('generated-only', artifact['status'])
            self.assertIs(artifact['source_connected'], False)
            self.assertIs(artifact['startup_proof'], False)
            ProgramModel(pulse(targets=[('A', 'DATA')], nonce='F' * 32), sql=artifact['pulse_sql']).run()

    def test_success_has_one_exact_all_or_none_request_and_only_marker_before_release(self):
        program = pulse()
        self.assertEqual('ALCOBJ OBJ((DCNC9999/A *FILE *EXCLRD A) (DCNC9999/EXTRA *FILE *EXCLRD DATA)) '
                         'WAIT(1) CONFLICT(*RQSRLS) SCOPE(*JOB)', program.allocate)
        self.assertEqual('DLCOBJ OBJ((DCNC9999/A *FILE *EXCLRD A) (DCNC9999/EXTRA *FILE *EXCLRD DATA)) SCOPE(*JOB)', program.release)
        self.assertEqual(5, program.statement().count('CALL QSYS2.QCMDEXC('))
        model = ProgramModel(program).run()
        self.assertEqual([('allocate', False), ('acquired', True), ('marker', True), ('release', True),
                          ('released', False), ('result', False)], model.trace)
        self.assertFalse(model.held)
        self.assertEqual(1, model.release_calls)
        self.assertEqual(1, len(model.result_rows))
        self.assertNotIn(boundary.PREPARE_SQL, program.statement())
        for forbidden in ('SELECT ', 'WRKJRNA', 'IFS_OBJECT_STATISTICS', 'JOURNAL_INFO', 'SYSPARTITIONSTAT',
                          'SYSFILES', 'DLYJOB', 'COMMIT', 'ROLLBACK', 'WHILE', 'LOOP', 'FORCE(*YES)'):
            self.assertNotIn(forbidden, program.statement())

    def test_shape_checks_detect_metadata_and_early_bookkeeping_mutations(self):
        program = pulse()
        for extra in ('SELECT * FROM QSYS2.SYSFILES;',
                      next(line for line in program.statement().splitlines() if line.startswith('INSERT '))):
            sql = program.statement().replace('SET ACQUIRED=CURRENT_TIMESTAMP;', 'SET ACQUIRED=CURRENT_TIMESTAMP;\n' + extra)
            with self.subTest(extra=extra), self.assertRaises(AssertionError):
                ProgramModel(program, sql=sql).run()

    def test_marker_command_is_bounded_and_does_not_force_override_or_associate_a_file(self):
        program = pulse()
        self.assertRegex(program.payload, r'^DCGB1 [A-F0-9]{32} N [A-F0-9]{64}$')
        self.assertLessEqual(len(program.payload.encode('cp037')), 512)
        self.assertEqual(f"SNDJRNE JRN(DCNC9999/QSQJRN) TYPE(DN) ENTDTA('{program.payload}') "
                         'FILE(*NONE) FORCE(*NO) OVRSTATE(*NONE) OVRRMTFTR(*NONE)', program.marker)

    def test_inputs_cannot_inject_or_silently_reduce_the_scope(self):
        invalid = [{'library': 'APPDATA'}, {'nonce': 'x'}, {'evidence_sha256': ''}, {'targets': []},
                   {'targets': [('A', 'A'), ('A', 'DATA')]}, {'targets': [('B', 'B'), ('A', 'A')]},
                   {'targets': [('A', '*FIRST')]}, {'targets': [('A', "A'); DROP")]},
                   {'targets': [('A;DROP', 'A')]}, {'targets': [('A',)]},
                   {'targets': [(f'A{i:02}', 'DATA') for i in range(51)]}]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                pulse(**changes)
        self.assertEqual(50, len(pulse(targets=[(f'A{i:02}', 'DATA') for i in range(50)]).targets))

    def test_scope_is_owned_and_digest_binds_exact_member_and_offlock_evidence(self):
        targets = [['A', 'DATA']]
        program = pulse(targets=targets)
        digest, sql = program.scope_digest, program.statement()
        targets[0][1] = 'OTHER'
        self.assertEqual(sql, program.statement())
        self.assertNotEqual(digest, pulse(targets=targets).scope_digest)
        self.assertNotEqual(digest, pulse(targets=[('A', 'DATA')], evidence_sha256='b' * 64).scope_digest)
        self.assertNotEqual(digest, pulse(targets=[('A', 'DATA')], library='DCNC9998').scope_digest)

    def test_error_warning_and_not_found_after_confirmed_allocation_release_once(self):
        for operation in ('acquired', 'marker'):
            for category in ('SQLEXCEPTION', 'SQLWARNING', 'NOT FOUND'):
                for after in (False, True):
                    with self.subTest(operation=operation, category=category, after=after):
                        model = ProgramModel(pulse(), {operation: (category, after)})
                        with self.assertRaises(Condition):
                            model.run()
                        self.assertEqual(1, model.release_calls)
                        self.assertFalse(model.held)
                        self.assertEqual([], model.result_rows)

    def test_failed_or_partial_allocation_runs_release_without_marker_or_success(self):
        for category in ('SQLEXCEPTION', 'SQLWARNING', 'NOT FOUND'):
            for after in (False, True):
                with self.subTest(category=category, after=after):
                    model = ProgramModel(pulse(), {'allocate': (category, after)})
                    with self.assertRaises(Condition):
                        model.run()
                    self.assertEqual([('allocate', False), ('release', after), ('released', False)], model.trace)
                    self.assertEqual(1, model.release_calls)
                    self.assertFalse(model.marker_present)
                    self.assertEqual([], model.result_rows)
                    self.assertFalse(model.held)

    def test_release_failure_has_a_bounded_retry_without_returning_success(self):
        for category in ('SQLEXCEPTION', 'SQLWARNING', 'NOT FOUND'):
            for after in (False, True):
                for marker_failure in (False, True):
                    with self.subTest(category=category, after=after, marker_failure=marker_failure):
                        faults = {'release': (category, after)}
                        if marker_failure:
                            faults['marker'] = ('SQLWARNING', True)
                        model = ProgramModel(pulse(), faults)
                        with self.assertRaises(Condition):
                            model.run()
                        self.assertEqual(3, model.release_calls)
                        self.assertEqual([], model.result_rows)
                        self.assertEqual(not after, model.held)

    def test_post_release_failure_does_not_reacquire_or_release_twice(self):
        for operation in ('released', 'result'):
            for category in ('SQLEXCEPTION', 'SQLWARNING', 'NOT FOUND'):
                for after in (False, True):
                    model = ProgramModel(pulse(), {operation: (category, after)})
                    with self.subTest(operation=operation, category=category, after=after), self.assertRaises(Condition):
                        model.run()
                    self.assertFalse(model.held)
                    self.assertEqual(1, model.release_calls)
                    # Even a warning after result insertion must become an error.
                    self.assertEqual(int(operation == 'result' and after), len(model.result_rows))


class MarkerTest(unittest.TestCase):
    def test_success_returns_owned_candidate_only_and_no_recovery_or_output_position(self):
        program = pulse()
        args = marker_fixture(program)
        args[0][0]['payload'] = bytearray(args[0][0]['payload'])
        candidate = boundary.validate_marker(program, *args)
        before = copy.deepcopy(candidate)
        args[0][0]['payload'][:] = b'x' * len(args[0][0]['payload'])
        self.assertEqual(before, candidate)
        self.assertEqual(boundary.PROFILE, candidate['profile'])
        self.assertEqual('marker-consistent-unqualified', candidate['status'])
        self.assertIs(candidate['startup_proof'], False)
        self.assertIs(candidate['maintenance_visibility_proven'], False)
        self.assertTrue({'R', 'F', 'W', 'L', 'checkpoint', 'offset', 'history', 'output'}.isdisjoint(candidate))

    def test_successful_command_with_missing_or_duplicate_marker_rejects(self):
        program = pulse()
        records, *rest = marker_fixture(program)
        for bad_records in ([], records * 2):
            with self.subTest(count=len(bad_records)), self.assertRaisesRegex(ValueError, 'Exactly one marker'):
                boundary.validate_marker(program, bad_records, *rest)

    def test_record_must_match_exact_native_identity_context_and_owned_bytes(self):
        changes = {'journal_library': 'DCNC9998', 'journal': 'OTHER', 'journal_identity': b'replaced',
                   'receiver_library': 'DCNC9998', 'receiver': 'QSQJRN0002', 'receiver_identity': b'replaced',
                   'job': '123457/TESTUSR/QZDASOINIT', 'thread': b'\x00' * 8, 'code': 'R', 'type': 'XX',
                   'file': 'A', 'member': 'DATA', 'commit_cycle': 1, 'ccsid': 1208, 'sequence': 105,
                   'pointer_backed': True, 'truncated': True, 'warnings': ['warning'], 'data_length': 1,
                   'payload': b'x' * 513}
        for key, value in changes.items():
            args = marker_fixture(pulse())
            args[0][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args)
        for key in marker_fixture(pulse())[0][0]:
            args = marker_fixture(pulse())
            del args[0][0][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args)

    def test_wrong_attempt_phase_digest_version_or_padding_cannot_match(self):
        program = pulse()
        for payload in (program.payload.replace('DCGB1', 'DCGB2'), program.payload.replace(' N ', ' A '),
                        pulse(nonce='F' * 32).payload, pulse(evidence_sha256='b' * 64).payload, program.payload + ' '):
            args = marker_fixture(program)
            args[0][0].update(payload=payload.encode('cp037'), data_length=len(payload))
            with self.subTest(payload=payload), self.assertRaisesRegex(ValueError, 'bytes differ'):
                boundary.validate_marker(program, *args)

    def test_marker_and_even_saved_result_do_not_rescue_failed_or_lost_response(self):
        changes = {'response': 'lost', 'warnings': ['warning'], 'holder_drained': False, 'locks_absent': False,
                   'nonce': 'F' * 32, 'scope_digest': 'F' * 64, 'program_sha256': 'f' * 64}
        for key, value in changes.items():
            args = marker_fixture(pulse())
            args[2][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args)
        for operation in ('marker', 'result'):
            model = ProgramModel(pulse(), {operation: ('SQLEXCEPTION', True)})
            with self.assertRaises(Condition):
                model.run()
            self.assertTrue(model.marker_present)
            args = marker_fixture(pulse())
            args[2]['response'] = 'failed'
            with self.assertRaisesRegex(ValueError, 'Failed'):
                boundary.validate_marker(pulse(), *args[:3], model.result_rows)

    def test_orphan_marker_without_completion_or_from_an_earlier_attempt_is_rejected(self):
        args = marker_fixture(pulse())
        args[2].clear()
        with self.assertRaises(ValueError):
            boundary.validate_marker(pulse(), *args)
        args = marker_fixture(pulse())
        with self.assertRaises(ValueError):
            boundary.validate_marker(pulse(nonce='E' * 32), *args)

    def test_incomplete_context_or_bookkeeping_rejects(self):
        for key in marker_fixture(pulse())[1]:
            args = marker_fixture(pulse())
            del args[1][key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args)
        for rows in ([], marker_fixture(pulse())[3] * 2, [{}]):
            args = marker_fixture(pulse())
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args[:3], rows)
        for key, value in (('ATTEMPT', 'F' * 32), ('SCOPE_DIGEST', 'F' * 64), ('ACQUIRED', None),
                           ('RELEASED', '2026-11-09 11:59:59.000000')):
            args = marker_fixture(pulse())
            args[3][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                boundary.validate_marker(pulse(), *args)


if __name__ == '__main__':
    unittest.main()
