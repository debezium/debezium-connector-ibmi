# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Native race classification and safe scope boundaries; never connects to IBM i."""
import datetime
import unittest

from probe_test_support import load_probe

scope = load_probe('dcg-lock-scope-probe.py', 'scope')


class ScopeProbeTest(unittest.TestCase):
    def event(self):
        return {'actor_status': 'completed', 'actor_server_before': '2026-11-09 14:00:01',
                'actor_server_after': '2026-11-09 14:00:05', 'holder_locks_after_actor': [],
                'waiting_locks': [{'LOCK_STATUS': 'WAITING'}], 'capture_status': 'candidate-validated',
                'capture': {'times': [{'PHASE': 'released', 'OBSERVED': datetime.datetime(2026, 11, 9, 14, 0, 4)}]}}

    def test_native_datetime_and_saved_string_agree(self):
        event = self.event()
        self.assertEqual('wait-observed', scope.classify_mutation(event))
        event['capture']['times'][0]['OBSERVED'] = '2026-11-09 14:00:04'
        self.assertEqual('wait-observed', scope.classify_mutation(event))

    def test_completed_inside_hold_is_not_excluded_even_if_observer_arrives_late(self):
        event = self.event()
        event['actor_server_after'] = '2026-11-09 14:00:03'
        self.assertEqual('not-excluded', scope.classify_mutation(event))

    def test_no_overlap_and_unrelated_failure_cannot_pass(self):
        event = self.event()
        event['actor_server_before'] = '2026-11-09 14:00:04'
        with self.assertRaisesRegex(ValueError, 'no overlap'):
            scope.classify_mutation(event)
        event.update(actor_status='rejected', actor_error='CPF1337: not authorized')
        with self.assertRaisesRegex(ValueError, 'other than'):
            scope.classify_mutation(event)

    def test_missing_wait_or_failed_capture_does_not_prove_exclusion(self):
        event = self.event()
        event['waiting_locks'] = []
        self.assertEqual('completion-order-unqualified', scope.classify_mutation(event))
        event = self.event()
        event['capture_status'] = 'rejected'
        self.assertEqual('completion-order-unqualified', scope.classify_mutation(event))
        event['holder_locks_after_actor'] = [{'LOCK_STATE': '*EXCLRD'}]
        self.assertEqual('not-excluded', scope.classify_mutation(event))

    def test_inventory_targets_one_journal_and_only_owned_temporary_output(self):
        command = scope.inventory_command('DCNC9999')
        self.assertIn('JRN(DCNC9999/QSQJRN)', command)
        self.assertIn('OUTFILE(QTEMP/DCGSCOPE)', command)
        self.assertIn('DETAIL(*OUTPUT)', command)
        for library in ['APPDATA', 'DCNC9999/QSQJRN', "DCNC9999')"]:
            with self.subTest(library=library), self.assertRaises(ValueError):
                scope.inventory_command(library)


if __name__ == '__main__':
    unittest.main()
