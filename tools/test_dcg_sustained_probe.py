# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
import unittest

from probe_test_support import load_probe

sustained = load_probe('dcg-sustained-probe.py', 'sustained')


class ObservationTest(unittest.TestCase):
    def test_matches_each_operation_to_its_submission_and_rejects_false_success(self):
        batches = [{'table': 'M', 'first': 1, 'rows': 2, 'operation': 'c', 'started_ms': 100},
                   {'table': 'M', 'first': 1, 'rows': 2, 'operation': 'u', 'started_ms': 200}]
        events = [{'topic': 'test.DB.LIB.M', 'key': {'ID': key}, 'value': {'op': op}, 'observed_ms': at}
                  for key, op, at in [(1, 'c', 150), (2, 'c', 170), (1, 'u', 280), (2, 'u', 300)]]
        self.assertEqual(sustained.latency_summary(batches, events),
                         {'rows': 4, 'p50_ms': 75, 'p95_ms': 100, 'p99_ms': 100, 'max_ms': 100})
        for invalid in [events[:-1], events + events[:1], [dict(events[0], observed_ms=99)] + events[1:]]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                sustained.latency_summary(batches, invalid)
        with self.assertRaises(ValueError):
            sustained.latency_summary(batches + batches[:1], events)


if __name__ == '__main__':
    unittest.main()
