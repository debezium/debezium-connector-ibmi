# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Evidence safety and failure classification for the public plugin experiment."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('public_online', Path(__file__).with_name('dcg-public-online-probe.py'))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class PublicEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.exp = probe.Experiment.__new__(probe.Experiment)
        self.exp.directory = Path(self.temp.name)
        self.exp.fixture = SimpleNamespace(redact=lambda text: text.replace('private-target', '<redacted>'))

    def test_command_retains_redacted_evidence_without_changing_live_response(self):
        result = subprocess.CompletedProcess([], 0, '{"result":"private-target"}\n')
        with patch.object(probe.subprocess, 'run', return_value=result):
            self.assertEqual('{"result":"private-target"}', self.exp.command('inspect', 'private-target'))
        log = (self.exp.directory / 'commands.log').read_text()
        self.assertNotIn('private-target', log)
        self.assertEqual(2, log.count('<redacted>'))

    def test_unrelated_task_failure_cannot_pass_missing_offset_assertion(self):
        self.exp.rest = Mock(return_value={'tasks': [{'state': 'FAILED', 'trace': 'Network failure private-target'}]})
        with self.assertRaisesRegex(RuntimeError, 'unexpected reason'):
            self.exp.failed('worker', 'missing', 'Missing offsets')
        self.assertNotIn('private-target', (self.exp.directory / 'missing-failed-status.json').read_text())
        self.exp.rest.return_value = {'tasks': [{'state': 'RUNNING'}]}
        self.assertFalse(self.exp.failed('worker', 'missing', 'Missing offsets'))
        self.exp.rest.return_value = {'tasks': [{'state': 'FAILED', 'trace': 'Missing offsets cannot select automatic migration'}]}
        self.assertTrue(self.exp.failed('worker', 'missing', 'Missing offsets'))

    def test_timeout_does_not_expose_private_arguments_or_output(self):
        failure = subprocess.TimeoutExpired(['inspect', 'private-target'], 1, output='private-target')
        with patch.object(probe.subprocess, 'run', side_effect=failure):
            with self.assertRaises(RuntimeError) as caught:
                self.exp.command('inspect', 'private-target', timeout=1)
        self.assertNotIn('private-target', str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == '__main__':
    unittest.main()
