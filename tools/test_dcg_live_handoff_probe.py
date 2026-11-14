# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Independent live-fixture ledger checks; no network or native credentials."""
import base64
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from probe_test_support import load_probe

live = load_probe('dcg-live-handoff-probe.py', 'live_handoff')


class LiveLedgerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source, self.replay = (Path(self.temp.name) / n for n in ['bundle.json', 'replay.json'])
        entries = []
        for index, (table, value) in enumerate([('A', 10), ('M', 21), ('A', 12), ('B', None), ('B', 101), ('A', 14)]):
            raw = bytearray(52)
            struct.pack_into('>i', raw, 8, 32)
            if value is not None:
                struct.pack_into('>i', raw, 48, value)
            entries.append({'type': 'D/CG' if value is None else 'R/PT', 'table': table,
                            'position': {'sequence': str(500 + index)}, 'raw': base64.b64encode(raw).decode()})
        self.bundle = {'profile': 'prospective-native-boundary-v1', 'startup_proof': False, 'cut': {'sequence': 502},
                       'prospective-rows': {'library': 'DCNC9999', 'entries': entries, 'native_identity_evidence': {}}}
        self.replay.write_text(json.dumps({'status': 'verified-offline', 'recovery': {'cases': [{'status': 'passed'}]}}))

    def export(self):
        self.source.write_text(json.dumps(self.bundle))
        return live.export_live(self.source, self.replay, 502)

    def test_actual_coordinates_need_not_match_old_66_entry_fixture(self):
        bundle, expected = self.export()
        self.assertFalse(bundle['startup_proof'])
        self.assertEqual(['501', '502', '505', '503', '504'], [r['sequence'] for r in expected['all']])
        self.assertEqual(['c', 'c', 'c', 't', 'c'], [r['op'] for r in expected['all']])

    def test_different_writer_values_cannot_redefine_expected_output(self):
        entry = self.bundle['prospective-rows']['entries'][1]
        raw = bytearray(base64.b64decode(entry['raw']))
        struct.pack_into('>i', raw, 48, 999)
        entry['raw'] = base64.b64encode(raw).decode()
        with self.assertRaisesRegex(ValueError, 'writer ledger'):
            self.export()

    def test_no_success_without_strict_replay_or_with_promoted_proof(self):
        self.bundle['startup_proof'] = True
        with self.assertRaisesRegex(ValueError, 'candidate replay'):
            self.export()
        self.bundle['startup_proof'] = False
        self.replay.write_text(json.dumps({'status': 'verified-offline', 'recovery': {'cases': [{'status': 'failed'}]}}))
        with self.assertRaisesRegex(ValueError, 'candidate replay'):
            self.export()


class NativeCompletionTest(unittest.TestCase):
    def test_completed_output_does_not_require_scanned_metadata_tail(self):
        terminal = {'JOURNAL_CODE': 'C', 'JOURNAL_ENTRY_TYPE': 'RB', 'JOB_NUMBER': '123456', 'JOB_USER': 'TEST',
                    'JOB_NAME': 'WRITER', 'COMMIT_CYCLE': 81, 'SEQUENCE_NUMBER': 94}
        metadata = {**terminal, 'JOURNAL_CODE': 'F', 'JOURNAL_ENTRY_TYPE': 'JM', 'COMMIT_CYCLE': 0, 'SEQUENCE_NUMBER': 98}
        foreign = {**terminal, 'JOB_NUMBER': '654321', 'SEQUENCE_NUMBER': 97}
        self.assertEqual(94, live.terminal_position([terminal, foreign, metadata], '123456/TEST/WRITER', 'RB'))
        padded = {key: value.ljust(10) if isinstance(value, str) else value for key, value in terminal.items()}
        self.assertEqual(94, live.terminal_position([padded], '123456/TEST/WRITER', 'RB'))
        for rows in [[metadata], [foreign], [terminal, terminal], [{**terminal, 'COMMIT_CYCLE': 0}]]:
            with self.assertRaisesRegex(ValueError, 'Exactly one'):
                live.terminal_position(rows, '123456/TEST/WRITER', 'RB')

    def test_failed_rollback_still_closes_connection_and_reports_failure(self):
        connection = Mock(autocommit=False)
        connection.rollback.side_effect = RuntimeError('disconnected')
        capture = Mock(connections=[connection], report={})
        capture.fixture.redact.side_effect = lambda value: value
        with self.assertRaisesRegex(RuntimeError, 'did not drain'):
            live.drain_capture_sessions(capture)
        connection.close.assert_called_once()
        self.assertEqual([], capture.connections)
        self.assertEqual('failed', capture.report['boundary_session_drain']['status'])


class CleanupTest(unittest.TestCase):
    def test_worker_failure_drains_writers_but_does_not_drop_fixture(self):
        for raises in [True, False]:
            with self.subTest(raises=raises):
                connection = Mock(autocommit=False)
                capture = Mock(connections=[connection], report={})
                capture.fixture.redact.side_effect = lambda value: value
                experiment = Mock(report={'cleanup': {'status': 'failed'}})
                if raises:
                    experiment.cleanup.side_effect = subprocess.TimeoutExpired('docker kill', 10)
                with self.assertRaises((RuntimeError, subprocess.TimeoutExpired)):
                    live.cleanup_owned(experiment, capture)
                connection.rollback.assert_called_once()
                connection.close.assert_called_once()
                capture.cleanup.assert_not_called()
                self.assertEqual('blocked', capture.report['fixture_cleanup']['status'])
                self.assertEqual('failed', experiment.report['status'])
                experiment.save.assert_called_once()

    def test_docker_timeout_aggregates_failure_and_continues_worker_cleanup(self):
        experiment = live.LiveExperiment.__new__(live.LiveExperiment)
        experiment.logs, experiment.children, experiment.log_threads = {'owned-worker': Path('/unused')}, [], []
        experiment.egress, experiment.prefix = 'owned-network', 'owned'
        experiment.report = {'status': 'passed', 'cleanup': {'status': 'passed'}}
        experiment.fixture, experiment.save = Mock(), Mock()
        experiment.fixture.redact.side_effect = lambda value: value
        experiment.command = Mock(side_effect=[subprocess.TimeoutExpired('docker kill', 10), '', '', '', '', ''])
        with patch.object(live.handoff.HandoffExperiment, 'cleanup') as parent:
            experiment.cleanup()
        parent.assert_called_once()
        self.assertEqual(6, experiment.command.call_count)
        self.assertEqual('failed', experiment.report['status'])
        self.assertEqual('failed', experiment.report['native_worker_cleanup']['status'])
        self.assertIn('timed out', experiment.report['native_worker_cleanup']['errors'][0])
        experiment.save.assert_called_once()

    def test_successful_drain_allows_strict_native_cleanup(self):
        capture = Mock()
        experiment = Mock(report={name: {'status': 'passed'} for name in ['cleanup', 'native_worker_cleanup']})
        live.cleanup_owned(experiment, capture)
        experiment.cleanup.assert_called_once()
        capture.cleanup.assert_called_once()

    def test_failure_before_worker_creation_still_cleans_native_fixture(self):
        capture = Mock()
        live.cleanup_owned(None, capture)
        capture.cleanup.assert_called_once()


if __name__ == '__main__':
    unittest.main()
