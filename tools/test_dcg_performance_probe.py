# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Prevent incomplete or incorrect output from being reported as fast delivery."""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

SPEC = importlib.util.spec_from_file_location('performance', Path(__file__).with_name('dcg-performance-probe.py'))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class PerformanceEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.exp = probe.Experiment.__new__(probe.Experiment)
        self.exp.control = Path(self.temp.name)
        self.exp.events = []
        self.exp.event_position = 0
        self.exp.observer_process = SimpleNamespace(poll=lambda: 1)

    def row(self, key, after=None):
        return {'topic': 'test.DB.DCNC9999.A', 'key': {'ID': key},
                'value': {'op': 'c', 'after': {'ID': key if after is None else after}}, 'observed_ms': 10}

    def test_should_wait_for_complete_observer_lines_without_repeating_records(self):
        path = self.exp.control / 'performance-events.jsonl'
        first, second = json.dumps(self.row(1)), json.dumps(self.row(2))
        path.write_text(first + '\n' + second[:20])
        self.assertEqual([self.row(1)], self.exp.new_events())
        with path.open('a') as stream:
            stream.write(second[20:] + '\n')
        self.assertEqual([self.row(1), self.row(2)], self.exp.new_events())
        self.assertEqual([self.row(1), self.row(2)], self.exp.new_events())

    def test_should_reject_missing_duplicate_or_corrupt_output_before_recording_timing(self):
        for rows in [[self.row(1)], [self.row(1), self.row(1)],
                     [self.row(1), self.row(2, 99)], [self.row(1), self.row(2), self.row(2)]]:
            with self.subTest(rows=rows):
                self.exp.events = rows
                with self.assertRaises((RuntimeError, AssertionError)):
                    self.exp.await_rows('A', 1, 2, 'c')
        self.exp.events = [self.row(2), self.row(1)]
        self.assertEqual(2, len(self.exp.await_rows('A', 1, 2, 'c')))

    def test_should_reject_records_observed_before_the_measurement_started(self):
        with self.assertRaisesRegex(AssertionError, 'clock or record selection'):
            self.exp.record([self.row(1)], 11, 12, 'A-c', 2)

    def test_should_compact_only_offset_observations_when_explicitly_enabled(self):
        self.exp.latest_offsets = True
        self.exp.command = Mock()
        offset = {'key': '["automatic", {"server": "test"}]', 'value': None}
        (self.exp.control / 'automatic-latest-offsets-read_committed.json').write_text(json.dumps([offset]))
        rows = self.exp.snapshot('observer', 'automatic', 'latest', offsets=True)
        self.assertEqual([{'key': ['automatic', {'server': 'test'}], 'value': None}], rows)
        self.assertEqual('latest-per-key', self.exp.command.call_args.args[-1])

        event = {'key': '{"ID": 1}', 'value': '{"op": "c"}'}
        (self.exp.control / 'automatic-all-records-read_committed.json').write_text(json.dumps([event, event]))
        rows = self.exp.snapshot('observer', 'automatic', 'all')
        self.assertEqual(2, len(rows))
        self.assertNotIn('latest-per-key', self.exp.command.call_args.args)


if __name__ == '__main__':
    unittest.main()
