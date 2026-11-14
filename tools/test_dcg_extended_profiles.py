# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Extended workloads must retain explicit bounds before any native side effect."""
import unittest
from unittest.mock import patch

from probe_test_support import load_probe


soak = load_probe('dcg-soak-probe.py')


class ExtendedProfilesTest(unittest.TestCase):
    def test_should_reject_unsafe_hour_profile_before_fixture_or_evidence_creation(self):
        # Distribution is an existing file; profile admission must precede importing
        # the optional native driver, creating evidence or connecting to the source.
        for rows, restarts in [(100, 0), (50, 1), (500, 3)]:
            with self.subTest(rows=rows, restarts=restarts):
                args = ['probe', '/not-created-by-this-test', 'DCNC9999', '--distribution', __file__,
                        '--label', 'test', '--duration', '3600', '--rows', str(rows), '--restart-count', str(restarts)]
                with patch('sys.argv', args), patch.object(soak.performance.live.prospective.census, 'Experiment') as fixture:
                    with self.assertRaises(SystemExit) as result:
                        soak.performance.main(soak.Experiment, soak.configure)
                    self.assertEqual(2, result.exception.code)
                    fixture.assert_not_called()

    def test_should_reject_legacy_delay_override_on_transactional_path_before_native_setup(self):
        args = ['probe', '/not-created-by-this-test', 'DCNC9999', '--distribution', __file__,
                '--label', 'test', '--mode', 'transactional', '--journal-additional-delay', '0']
        with patch('sys.argv', args), patch.object(soak.performance.live.prospective.census, 'Experiment') as fixture:
            with self.assertRaises(SystemExit) as result:
                soak.performance.main()
            self.assertEqual(2, result.exception.code)
            fixture.assert_not_called()


if __name__ == '__main__':
    unittest.main()
