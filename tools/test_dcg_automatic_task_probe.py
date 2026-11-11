# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Local lifecycle checks for the isolated native task harness; no connection."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location('automatic_task', Path(__file__).with_name('dcg-automatic-task-probe.py'))
automatic = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(automatic)


class TaskLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.experiment = automatic.Experiment.__new__(automatic.Experiment)
        self.experiment.control = Path(self.temp.name)
        self.experiment.active_role = 'automatic-w2'
        self.current = self.experiment.control / 'automatic-automatic-w2-current'
        self.current.mkdir()
        previous = self.experiment.control / 'automatic-automatic-w1-previous'
        previous.mkdir()
        (previous / 'drained.json').write_text('{}')

    def test_completed_drain_fails_wait_for_unreached_phase_without_timeout(self):
        (self.current / 'drained.json').write_text('{}')
        with self.assertRaisesRegex(RuntimeError, 'drained before busy'):
            self.experiment.task_marker('busy-*')

    def test_previous_generation_drain_does_not_fail_replacement(self):
        with patch.object(automatic.base, 'wait_for', side_effect=lambda label, predicate, timeout: predicate()):
            self.assertIsNone(self.experiment.task_marker('recovered'))
        recovered = self.current / 'recovered.json'
        recovered.write_text('{}')
        self.assertEqual(recovered, self.experiment.task_marker('recovered'))
        drained = self.current / 'drained.json'
        drained.write_text('{}')
        self.assertEqual(drained, self.experiment.task_marker('drained'))

    def test_observations_use_distinct_read_only_sessions_and_close_each(self):
        first, second = Mock(), Mock()
        capture = self.experiment.capture = Mock(connections=[first, second])
        capture.connection.side_effect = [(first, 'job-1'), (second, 'job-2')]
        action = Mock(side_effect=['point-1', 'point-2'])
        self.assertEqual('point-1', self.experiment.read_native(action))
        self.assertEqual('point-2', self.experiment.read_native(action))
        self.assertEqual([((first, 'job-1'),), ((second, 'job-2'),)], [call.args for call in action.call_args_list])
        self.assertEqual([{'read_only': True}, {'read_only': True}], [call.kwargs for call in capture.connection.call_args_list])
        first.close.assert_called_once_with()
        second.close.assert_called_once_with()
        self.assertEqual([], capture.connections)

    def test_disconnected_observation_fails_without_retry_and_closes(self):
        connection = Mock()
        capture = self.experiment.capture = Mock(connections=[connection])
        capture.connection.return_value = (connection, 'job')
        action = Mock(side_effect=RuntimeError('SQLPrepare disconnected'))
        with self.assertRaisesRegex(RuntimeError, 'SQLPrepare disconnected'):
            self.experiment.read_native(action)
        action.assert_called_once_with((connection, 'job'))
        capture.connection.assert_called_once_with(read_only=True)
        connection.close.assert_called_once_with()
        self.assertEqual([], capture.connections)

    def test_failed_close_remains_registered_for_owned_cleanup(self):
        connection = Mock()
        connection.close.side_effect = RuntimeError('close incomplete')
        capture = self.experiment.capture = Mock(connections=[connection])
        with self.assertRaisesRegex(RuntimeError, 'close incomplete'):
            self.experiment.close_session((connection, 'job'))
        self.assertEqual([connection], capture.connections)

    def test_progress_uses_observed_row_coordinate_not_receiver_tail(self):
        self.experiment.library = 'DCNC1002'
        self.experiment.point = Mock(return_value={'sequence': 663})
        self.experiment.progressed = Mock(return_value={'completed_through.sequence': '662'})
        record = {'value': {'source': {'sequence': '662', 'receiver': 'QSQJRN0002', 'receiver_library': 'DCNC1002'}}}
        self.assertEqual({'completed_through.sequence': '662'}, self.experiment.row_progress(record, 'writer-complete'))
        self.experiment.progressed.assert_called_once_with('662', 'writer-complete')
        self.experiment.point.assert_not_called()

    def test_insert_lookup_handles_prior_truncates_and_tombstones_without_hiding_duplicates(self):
        inserted = {'key': {'ID': 224}, 'value': {'op': 'c'}}
        records = [{'key': None, 'value': {'op': 't'}},
                   {'key': {'ID': 224}, 'value': None},
                   {'key': {'ID': 224}, 'value': {'op': 'd'}}, inserted]
        self.experiment.table_records = Mock(return_value=records)
        self.assertEqual(inserted, self.experiment.inserted('M', 224, 'reload-after-clear'))
        records.append(inserted.copy())
        with self.assertRaisesRegex(ValueError, 'Duplicate fixture insert output'):
            self.experiment.inserted('M', 224, 'duplicate-reload')

    def test_ambiguous_row_coordinate_cannot_choose_a_checkpoint_target(self):
        self.experiment.library = 'DCNC1002'
        self.experiment.progressed = Mock()
        for changes in [{'receiver_library': 'OTHER'}, {'receiver': ''}, {'sequence': '-1'}, {'sequence': '0'}, {'sequence': 662}]:
            with self.subTest(changes=changes):
                source = {'sequence': '662', 'receiver': 'QSQJRN0002', 'receiver_library': 'DCNC1002', **changes}
                with self.assertRaises(ValueError):
                    self.experiment.row_progress({'value': {'source': source}}, 'writer-complete')
        self.experiment.progressed.assert_not_called()


if __name__ == '__main__':
    unittest.main()
