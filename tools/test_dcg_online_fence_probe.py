# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Ownership and bound checks for native metadata batching; no source connection."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('fence', Path(__file__).with_name('dcg-online-fence-probe.py'))
fence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fence)


class BatchingTest(unittest.TestCase):
    def test_exact_request_ownership_survives_interleaved_results_and_empty_request(self):
        items = [{'id': 1, 'maximum_rows': 2}, {'id': 4, 'maximum_rows': 2}, {'id': 9, 'maximum_rows': 1}]
        rows = [{'DCG_REQUEST_ID': 4, 'VALUE': 'B'}, {'DCG_REQUEST_ID': 1, 'VALUE': 'A'}, {'DCG_REQUEST_ID': 4, 'VALUE': 'C'}]
        result = fence.split_batch_rows(rows, items)
        self.assertEqual({1: [{'VALUE': 'A'}], 4: [{'VALUE': 'B'}, {'VALUE': 'C'}], 9: []}, result)
        rows[0]['VALUE'] = 'changed'
        self.assertEqual('B', result[4][0]['VALUE'])

    def test_one_request_cannot_consume_anothers_unused_bound(self):
        items = [{'id': 1, 'maximum_rows': 1}, {'id': 2, 'maximum_rows': 10}]
        with self.assertRaisesRegex(ValueError, 'Excessive native metadata'):
            fence.split_batch_rows([{'DCG_REQUEST_ID': 1}, {'DCG_REQUEST_ID': 1}], items)

    def test_unknown_or_missing_owner_rejects(self):
        for owner in [None, 2, True, '1']:
            with self.subTest(owner=owner), self.assertRaisesRegex(ValueError, 'metadata owner'):
                fence.split_batch_rows([{'DCG_REQUEST_ID': owner}], [{'id': 1, 'maximum_rows': 1}])

    def test_unsafe_targets_reject_before_any_source_query(self):
        with patch.object(fence.census, 'rows') as rows:
            for library, tables in [('APPDATA', ['A']), ('DCNC9999', ['A;DROP']), ('DCNC9999', ['A', 'A']), ('DCNC9999', [])]:
                with self.subTest(library=library, tables=tables), self.assertRaises(ValueError):
                    fence.Collector(object(), library, tables, {})
            rows.assert_not_called()

    def test_incomplete_or_unsafe_native_member_mapping_rejects_before_queries(self):
        with patch.object(fence.census, 'rows') as rows:
            for members in [{}, {'A': 'DATA', 'B': 'B'}, {'A': "DATA');DROP"}, {'A': None}]:
                with self.subTest(members=members), self.assertRaises(ValueError):
                    fence.Collector(object(), 'DCNC9999', ['A'], {}, members=members)
            rows.assert_not_called()

    def test_parameter_literals_cannot_escape_the_read_queries(self):
        for value in ["A' OR 1=1", 'A;DROP', 'A --', 1]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                fence.literal(value)


if __name__ == '__main__':
    unittest.main()
