# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Runner scheduling and diagnostic rejection tests; never import a native driver."""
import copy
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('short_probe', Path(__file__).with_name('dcg-short-boundary-probe.py'))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def program():
    return probe.pulse.Pulse('DCNC9999', [('A', 'DATA')], 'F' * 32, 'a' * 64)


def marker(payload, sequence):
    raw = payload.encode('cp037')
    return {'JOURNAL_CODE': 'U', 'JOURNAL_ENTRY_TYPE': 'DN', 'RECEIVER_LIBRARY': 'DCNC9999',
            'RECEIVER_NAME': 'QSQJRN0002', 'JOB_NUMBER': '123456', 'JOB_USER': 'TESTUSR', 'JOB_NAME': 'QZDASOINIT',
            'THREAD': 7, 'COMMIT_CYCLE': 0, 'OBJECT': '', 'OBJECT_TYPE': '', 'SEQUENCE_NUMBER': sequence,
            'DATA_LENGTH': len(raw), 'DATA_HEX': (raw + bytes(512 - len(raw))).hex()}


class DiagnosticTest(unittest.TestCase):
    def test_actual_edgeqa_marker_bytes_and_padded_names(self):
        path = Path(__file__).resolve().parents[1] / 'debezium-connector-ibmi/src/test/resources/native-transactions/short-boundary-markers.json'
        captured = json.loads(path.read_text())
        request = probe.pulse.Pulse(**captured['request'])
        payloads = [probe.audit_marker(request.library, request.nonce, 'A')[0], request.payload,
                    probe.audit_marker(request.library, request.nonce, 'Z', request.scope_digest)[0]]
        interval = captured['interval']
        checked = probe.check_markers(captured['diagnostic']['rows'], payloads, interval['holder'][0]['JOB'],
                                      interval['holder'][0]['THREAD_HEX'], interval['from'], interval['through'])
        self.assertEqual([54, 55, 56], checked['sequences'])
        self.assertIs(checked['startup_proof'], False)

    def test_repeated_attempt_query_excludes_only_its_inclusive_api_anchor(self):
        path = Path(__file__).resolve().parents[1] / 'debezium-connector-ibmi/src/test/resources/native-transactions/short-boundary-markers.json'
        captured = json.loads(path.read_text())['repeated_attempt']
        request = probe.pulse.Pulse(**captured['request'])
        interval = captured['interval']
        rows = captured['diagnostic']['rows']
        self.assertEqual(4, len(rows))
        self.assertEqual(interval['from']['sequence'], rows[0]['SEQUENCE_NUMBER'])
        self.assertIn('WHERE SEQUENCE_NUMBER > ?', probe.MARKERS_SQL)
        self.assertEqual(9, probe.MARKERS_SQL.count('?'))
        payloads = [probe.audit_marker(request.library, request.nonce, 'A')[0], request.payload,
                    probe.audit_marker(request.library, request.nonce, 'Z', request.scope_digest)[0]]
        selected = [r for r in rows if r['SEQUENCE_NUMBER'] > interval['from']['sequence']]
        result = probe.check_markers(selected, payloads, interval['holder'][0]['JOB'],
                                     interval['holder'][0]['THREAD_HEX'], interval['from'], interval['through'])
        self.assertEqual(3, len(result['sequences']))
        with self.assertRaisesRegex(ValueError, 'foreign U'):
            probe.check_markers(rows, payloads, interval['holder'][0]['JOB'],
                                interval['holder'][0]['THREAD_HEX'], interval['from'], interval['through'])

    def setUp(self):
        self.program = program()
        self.payloads = [probe.audit_marker('DCNC9999', self.program.nonce, 'A')[0], self.program.payload,
                         probe.audit_marker('DCNC9999', self.program.nonce, 'Z', self.program.scope_digest)[0]]
        self.rows = [marker(payload, i + 101) for i, payload in enumerate(self.payloads)]
        self.start = {'library': 'DCNC9999', 'receiver': 'QSQJRN0002', 'sequence': 100}
        self.end = {**self.start, 'sequence': 103}
        self.audit_start = {**self.start, 'sequence': 0}
        self.audit_end = {**self.end, 'sequence': 3}

    def audit_row(self, sequence):
        return {**marker('', sequence), 'DATA_HEX': '00' * 16000}

    def check(self):
        return probe.check_markers(self.rows, self.payloads, '123456/TESTUSR/QZDASOINIT', '0000000000000007', self.start, self.end)

    def test_sql_diagnostic_never_claims_strict_rpc_or_startup_proof(self):
        checked = self.check()
        self.assertIs(checked['startup_proof'], False)
        self.assertIs(checked['maintenance_visibility_proven'], False)
        self.assertIs(checked['strict_rpc_replay'], False)
        self.assertEqual([101, 102, 103], checked['sequences'])

    def test_missing_duplicate_out_of_order_or_foreign_marker_rejects(self):
        original = copy.deepcopy(self.rows)
        for rows in (original[:-1], original + [original[-1]], list(reversed(original))):
            self.rows = rows
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.check()
        for key, value in {'THREAD': 8, 'JOB_NUMBER': '999999', 'RECEIVER_NAME': 'OTHER', 'OBJECT': 'A',
                           'COMMIT_CYCLE': 1, 'SEQUENCE_NUMBER': 99, 'JOURNAL_CODE': 'R'}.items():
            self.rows = copy.deepcopy(original)
            self.rows[1][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check()

    def test_native_byte_bounds_padding_and_encoding_are_not_guessed(self):
        original = copy.deepcopy(self.rows)
        for key, value in [('DATA_LENGTH', 513), ('DATA_LENGTH', None), ('DATA_HEX', 'FF' * 512),
                           ('DATA_HEX', original[1]['DATA_HEX'][:-2]), ('DATA_HEX', '00' * 512)]:
            self.rows = copy.deepcopy(original)
            self.rows[1][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.check()

    def test_rotation_and_bad_marker_commands_reject(self):
        self.end['receiver'] = 'QSQJRN0003'
        with self.assertRaisesRegex(ValueError, 'rotation'):
            self.check()
        for library, nonce, phase, digest in [('APPDATA', 'F' * 32, 'A', None),
                                            ('DCNC9999', "';DROP", 'A', None),
                                            ('DCNC9999', 'F' * 32, 'N', None),
                                            ('DCNC9999', 'F' * 32, 'Z', None)]:
            with self.subTest(phase=phase), self.assertRaises(ValueError):
                probe.audit_marker(library, nonce, phase, digest)

    def test_equal_metadata_is_only_a_consistency_check_and_changed_scope_rejects(self):
        before = {'targets': ['A'], 'native_scope': {'objects': ['A']}, 'queries': []}
        self.assertIsNone(probe.same_metadata(before, copy.deepcopy(before)))
        after = copy.deepcopy(before)
        after['native_scope']['objects'].append('NEW')
        with self.assertRaisesRegex(ValueError, 'changed'):
            probe.same_metadata(before, after)

    def test_equal_end_states_cannot_erase_observed_create_drop_or_journaling_changes(self):
        for kind in ('D/CT', 'D/TD', 'F/EJ', 'D/JF', 'J/XP', 'C/BA', 'Y/YO'):
            rows = [{**self.audit_row(i + 1), 'JOURNAL_CODE': k[0], 'JOURNAL_ENTRY_TYPE': k[2:]}
                    for i, k in enumerate(['U/DN', kind, 'U/DN'])]
            checked = probe.audit_diagnostic(rows, [1, 3], self.audit_start, self.audit_end)
            with self.subTest(kind=kind):
                self.assertEqual('invalidated', checked['status'])
                self.assertEqual(kind, checked['invalidating'][0]['type'])
                self.assertIs(checked['startup_proof'], False)

    def test_clean_sql_audit_is_unqualified_and_incomplete_marker_or_payload_rejects(self):
        row = self.audit_row(1)
        self.assertEqual('no-invalidation-observed-unqualified', probe.audit_diagnostic([row], [1], self.audit_start, self.audit_end)['status'])
        for rows, markers in [([row], []), ([row, row], [1]), ([{**row, 'DATA_LENGTH': 16001}], [1])]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                probe.audit_diagnostic(rows, markers, self.audit_start, self.audit_end)


    def test_sql_audit_rejects_foreign_coordinates_truncated_bytes_and_padding(self):
        row = self.audit_row(1)
        for key, value in [('RECEIVER_LIBRARY', 'FOREIGN'), ('RECEIVER_NAME', 'OTHER'),
                           ('SEQUENCE_NUMBER', 4), ('SEQUENCE_NUMBER', True),
                           ('DATA_HEX', '00' * 15999), ('DATA_HEX', 'FF' + '00' * 15999)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                probe.audit_diagnostic([{**row, key: value}], [1], self.audit_start, self.audit_end)


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.directory = Path(self.folder.name) / 'pulse'
        self.program = program()
        self.exp = SimpleNamespace(library='DCNC9999', fixture=SimpleNamespace(redact=lambda text: text),
                                   execute=Mock(), writer_thread=None)
        self.holder, self.observer = (object(), '123456/TESTUSR/QZDASOINIT'), (object(), '999999/TESTUSR/QZDASOINIT')
        self.rows = [{'ATTEMPT': self.program.nonce, 'SCOPE_DIGEST': self.program.scope_digest,
                      'ACQUIRED': datetime.datetime(2026, 11, 9, 12), 'RELEASED': datetime.datetime(2026, 11, 9, 12, 0, 0, 1000)}]

    def test_success_reads_results_only_after_return_and_independent_lock_absence(self):
        order = []
        def execute(holder, operation, sql):
            order.append(operation)
        def locks(*args):
            order.append('locks')
            return []
        def rows(*args, **kwargs):
            order.append('results')
            return self.rows
        self.exp.execute.side_effect = execute
        with patch.object(probe.fence, 'held_locks', side_effect=locks), patch.object(probe.census, 'rows', side_effect=rows):
            result = probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory)
        self.assertEqual(['locks', 'prepare-pulse-results', 'pulse', 'locks', 'results'], order)
        self.assertTrue(result['holder_drained'])
        self.assertEqual('success', result['response'])
        self.assertEqual(self.program.statement() + '\n', (self.directory / 'statement.sql').read_text())
        self.assertEqual(0, result['contention_only_delay_seconds'])

    def test_contention_delay_is_explicit_and_separate_from_healthy_latency(self):
        self.rows[0]['RELEASED'] += datetime.timedelta(seconds=1)
        with patch.object(probe.fence, 'held_locks', return_value=[]), patch.object(probe.census, 'rows', return_value=self.rows):
            result = probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory, contention_delay=True)
        statement = (self.directory / 'statement.sql').read_text()
        self.assertEqual(1, statement.count("CALL QSYS2.QCMDEXC('DLYJOB DLY(1)');"))
        self.assertEqual(1, result['contention_only_delay_seconds'])
        self.assertEqual(1.001, result['hold_seconds'])
        self.assertLess(statement.index(probe.pulse.sql_literal(self.program.marker)), statement.index('DLYJOB'))
        self.assertLess(statement.index('DLYJOB'), statement.index('DLCOBJ'))

    def test_ambiguous_contention_instrumentation_is_rejected_before_native_work(self):
        for arguments in ({'contention_delay': 1}, {'contention_delay': True, 'fault': True}):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory, **arguments)
        self.exp.execute.assert_not_called()

    def test_error_after_marker_is_retained_with_drain_and_zero_success_rows(self):
        def execute(holder, operation, sql):
            if operation == 'pulse':
                self.assertIn("SIGNAL SQLSTATE '75011'", sql)
                raise RuntimeError('native test failure')
        self.exp.execute.side_effect = execute
        with patch.object(probe.fence, 'held_locks', return_value=[]), patch.object(probe.census, 'rows', return_value=[]):
            result = probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory, fault=True)
        self.assertEqual('failed', result['response'])
        self.assertTrue(result['holder_drained'])
        self.assertEqual([], result['result_rows'])

    def test_remaining_locks_prevent_result_and_metadata_work(self):
        with patch.object(probe.fence, 'held_locks', side_effect=[[], [{'LOCK_STATE': '*EXCLRD'}]]), patch.object(probe.census, 'rows') as read:
            with self.assertRaisesRegex(ValueError, 'still holds'):
                probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory)
            read.assert_not_called()

    def test_timeout_keeps_registered_worker_and_never_reads_or_closes_its_session(self):
        entered, release = threading.Event(), threading.Event()
        def execute(holder, operation, sql):
            if operation == 'pulse':
                entered.set()
                release.wait(2)
        self.exp.execute.side_effect = execute
        try:
            with patch.object(probe.fence, 'held_locks', return_value=[]) as locks, patch.object(probe.census, 'rows') as read:
                with self.assertRaisesRegex(TimeoutError, 'drain unknown'):
                    probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory, wait_seconds=.02)
                self.assertTrue(entered.is_set())
                self.assertTrue(self.exp.writer_thread.is_alive())
                self.assertEqual(1, locks.call_count)
                read.assert_not_called()
        finally:
            release.set()
            self.exp.writer_thread.join(2)
        self.assertFalse(self.exp.writer_thread.is_alive())

    def test_existing_allocations_prevent_program_admission(self):
        with patch.object(probe.fence, 'held_locks', return_value=[{'LOCK_STATE': '*EXCLRD'}]):
            with self.assertRaisesRegex(ValueError, 'already has'):
                probe.run_pulse(self.exp, self.holder, self.observer, self.program, self.directory)
        self.exp.execute.assert_not_called()


class RawMarkerTest(unittest.TestCase):
    def test_actual_rpc_marker_prefix_and_alignment_match_native_sql_payloads(self):
        fixture = Path(__file__).resolve().parents[1] / 'debezium-connector-ibmi/src/test/resources/native-transactions/short-boundary-markers.json'
        captured = json.loads(fixture.read_text())['raw_attempt']
        request = probe.pulse.Pulse(**captured['request'])
        interval = captured['interval']
        expected = [probe.audit_marker(request.library, request.nonce, 'A')[0], request.payload,
                    probe.audit_marker(request.library, request.nonce, 'Z', request.scope_digest)[0]]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'capture.json').write_text(json.dumps(captured['capture']))
            for name, raw in captured['owned_bytes'].items():
                (root / name).write_bytes(bytes.fromhex(raw))
            result = probe.check_rpc_markers(root, expected, [54, 55, 56], interval['holder'][0]['JOB'],
                                             interval['holder'][0]['THREAD_HEX'], interval['from'], interval['through'])
            self.assertEqual('raw-markers-matched-unqualified', result['status'])
        entry = next(e for e in captured['capture']['entries'] if e['type'] == 'U/DN')
        original = bytes.fromhex(captured['owned_bytes'][entry['file']])
        offset, limit = entry['entry_specific_data_offset'], entry['entry_end_offset']
        for at, value in [(offset, 0), (offset + 5, 1), (218, 0x20), (limit - 1, 1)]:
            bad = bytearray(original)
            bad[at] = value
            with self.subTest(at=at), self.assertRaises(ValueError):
                probe.rpc_payload(bytes(bad), offset, limit)

    def test_raw_owned_bytes_coordinates_and_producer_must_all_match(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            raw = bytes(240) + '00001'.encode('cp037') + bytes(11) + b'\xc1'  # Synthetic RJNE0200 prefix/frame.
            (root / 'entry-0.bin').write_bytes(raw)
            start = {'library': 'DCNC9999', 'receiver': 'QSQJRN0002', 'sequence': 10}
            end = {**start, 'sequence': 12}
            entry = {'type': 'U/DN', 'cycle': '0', 'position': {**start, 'sequence': 11}, 'file': 'entry-0.bin',
                     'table': '', 'library': '', 'member': '',
                     'record': {'jobNumber': '123456', 'jobUser': 'TESTUSR', 'jobName': 'JOB', 'threadId': 7},
                     'sha256': hashlib.sha256(raw).hexdigest(), 'entry_specific_data_offset': 240, 'entry_end_offset': len(raw)}
            capture = {'status': 'audit-captured-unqualified', 'startup_proof': False, 'entries': [entry]}
            (root / 'capture.json').write_text(json.dumps(capture))
            def check():
                return probe.check_rpc_markers(root, ['A'], [11], '123456/TESTUSR/JOB', '0000000000000007', start, end)
            self.assertIs(check()['startup_proof'], False)
            for key, value in [('sha256', '0' * 64), ('entry_end_offset', len(raw) - 1), ('file', '../foreign.bin'), ('cycle', '1')]:
                bad = copy.deepcopy(capture)
                bad['entries'][0][key] = value
                (root / 'capture.json').write_text(json.dumps(bad))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    check()
            capture['entries'][0]['record']['threadId'] = 8
            (root / 'capture.json').write_text(json.dumps(capture))
            with self.assertRaises(ValueError):
                check()


if __name__ == '__main__':
    unittest.main()
