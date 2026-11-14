# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""A failed Engine cleanup must never authorize deletion of its IBM i fixture."""
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from probe_test_support import load_probe

probe = load_probe('dcg-file-history-probe.py', 'file_history_probe')


class CleanupTest(unittest.TestCase):
    def test_live_engine_blocks_fixture_deletion_and_still_removes_temporary_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            exp = probe.Experiment.__new__(probe.Experiment)
            exp.directory = Path(directory)
            exp.configs = [exp.directory / 'credentials.properties']
            exp.configs[0].write_text('fixture-only')
            exp.logs = []
            process = Mock()
            process.poll.return_value = None
            process.wait.side_effect = subprocess.TimeoutExpired('owned-engine', 1)
            exp.children = [('running', process, exp.configs[0])]
            exp.report = {'status': 'passed'}
            exp.capture = SimpleNamespace(report={}, cleanup=Mock(), persist=Mock())
            with patch.object(probe.public.live, 'drain_capture_sessions') as drain:
                exp.cleanup()
                drain.assert_called_once_with(exp.capture)
            exp.capture.cleanup.assert_not_called()
            self.assertEqual('blocked', exp.capture.report['fixture_cleanup']['status'])
            self.assertEqual('failed', exp.report['process_cleanup']['status'])
            self.assertFalse(exp.configs[0].exists())


if __name__ == '__main__':
    unittest.main()
