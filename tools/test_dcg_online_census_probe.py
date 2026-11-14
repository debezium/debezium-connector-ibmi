# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
import copy
import itertools
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from probe_test_support import load_probe

probe = load_probe("dcg-online-census-probe.py", "probe")


def entry(sequence, cycle, kind):
    return {"position": {"sequence": str(sequence), "receiver": "RCV", "library": "DCNC0911"},
            "cycle": str(cycle), "type": kind, "sha256": str(sequence)}


class MembershipObservationTest(unittest.TestCase):
    LIBRARY = "DCNC0911"
    SPACE = "UDB_0000000000000001"

    def observation(self, count, start, end):
        return {"started": {"monotonic_ns": start}, "finished": {"monotonic_ns": end}, "rows": [{
            "JOURNAL_LIBRARY": self.LIBRARY, "JOURNAL_NAME": "QSQJRN", "JOURNAL_TYPE": "*LOCAL",
            "JOURNAL_STATE": "*ACTIVE", "JOURNALED_COMMITMENT_DEFINITIONS": count}]}

    def setUp(self):
        self.before, self.after = self.observation(1, 1, 2), self.observation(1, 5, 6)
        self.sample = {"status": "observed", "started": {"monotonic_ns": 3}, "finished": {"monotonic_ns": 4},
                       "definitions": [{"LOCK_SPACE_ID": self.SPACE}], "resources": [{"lock_space": self.SPACE,
                           "JOURNAL_LIBRARY": self.LIBRARY, "JOURNAL_NAME": "QSQJRN", "COMMIT_CYCLE": 5, "PENDING_CHANGES": 0}]}
        self.bracket = [entry(10, 0, "R/PT"), entry(11, 0, "R/PT")]

    def audit(self):
        return probe.audit_membership(self.sample, self.before, self.after, self.bracket, self.LIBRARY)

    def test_open_or_idle_resource_is_counted_without_proof_promotion(self):
        for cycle in [5, None]:
            with self.subTest(cycle=cycle):
                self.sample["resources"][0]["COMMIT_CYCLE"] = cycle
                result = self.audit()
                self.assertEqual(result["status"], "membership-consistent-unqualified")
                self.assertEqual(result["observed_resources"], 1)
                self.assertFalse(result["startup_proof"])
                self.assertFalse(result["visibility_proof"])

    def test_persistently_missing_resource_rejects_even_when_bracket_only_has_unrelated_rows(self):
        self.sample["resources"] = []
        self.assertIn("disagrees", self.audit()["reason"])

    def test_balanced_membership_replacement_rejects_despite_equal_counts(self):
        self.bracket = [entry(10, 0, "C/EC"), entry(11, 0, "C/BC")]
        self.assertIn("membership changed", self.audit()["reason"])

    def test_begin_end_and_abnormal_recovery_each_reject(self):
        for kind in ["C/BC", "C/EC", "C/BA"]:
            with self.subTest(kind=kind):
                self.bracket = [entry(10, 0, kind)]
                self.assertEqual(self.audit()["status"], "rejected")

    def test_changed_count_rejects(self):
        self.after["rows"][0]["JOURNALED_COMMITMENT_DEFINITIONS"] = 2
        self.assertEqual(self.audit()["status"], "rejected")

    def test_unknown_partial_remote_and_excessive_observations_reject(self):
        changes = [{"JOURNALED_COMMITMENT_DEFINITIONS": count} for count in [None, -1, True, "1", 1.0, 257]]
        changes += [{"JOURNAL_NAME": "OTHER"}, {"JOURNAL_LIBRARY": "OTHER"},
                    {"JOURNAL_TYPE": "*REMOTE"}, {"JOURNAL_STATE": "*STANDBY"}]
        for change in changes:
            with self.subTest(change=change):
                self.before = self.observation(1, 1, 2)
                self.before["rows"][0].update(change)
                self.assertEqual(self.audit()["status"], "rejected")
        for values in [[], self.after["rows"] * 2]:
            self.before["rows"] = values
            self.assertEqual(self.audit()["status"], "rejected")

    def test_duplicate_or_unlisted_resource_cannot_mask_missing_definition(self):
        original = copy.deepcopy(self.sample)
        for fault in ["duplicate-resource", "duplicate-definition", "unlisted", "wrong-journal"]:
            with self.subTest(fault=fault):
                self.sample = copy.deepcopy(original)
                if fault == "duplicate-resource":
                    self.sample["resources"] *= 2
                elif fault == "duplicate-definition":
                    self.sample["definitions"] *= 2
                elif fault == "unlisted":
                    self.sample["definitions"] = []
                else:
                    self.sample["resources"][0]["JOURNAL_NAME"] = "OTHER"
                self.assertEqual(self.audit()["status"], "rejected")

    def test_empty_inventory_needs_both_zero_counts_and_raw_bracket(self):
        self.sample.update(definitions=[], resources=[])
        self.before, self.after = self.observation(0, 1, 2), self.observation(0, 5, 6)
        self.assertEqual(self.audit()["status"], "membership-consistent-unqualified")
        self.bracket = []
        self.assertEqual(self.audit()["status"], "rejected")

    def test_incomplete_or_unordered_census_rejects(self):
        self.sample["status"] = "rejected"
        self.assertEqual(self.audit()["status"], "rejected")
        self.sample["status"] = "observed"
        self.after["started"]["monotonic_ns"] = 3
        self.assertEqual(self.audit()["status"], "rejected")

    def test_exact_parameterized_bounded_read_and_failed_read_never_become_zero(self):
        with patch.object(probe, "rows", return_value=self.before["rows"]) as read:
            observed = probe.membership(object(), self.LIBRARY)
        self.assertEqual(read.call_args.args[1:], (probe.MEMBERSHIP, (self.LIBRARY, "QSQJRN"), 1))
        self.assertFalse(observed["startup_proof"])
        self.assertLess(observed["started"]["monotonic_ns"], observed["finished"]["monotonic_ns"])
        for error in [TimeoutError("timeout"), ValueError("SQL warning"), ValueError("partial response")]:
            with self.subTest(error=error), patch.object(probe, "rows", side_effect=error):
                with self.assertRaises(type(error)):
                    probe.membership(object(), self.LIBRARY)


class ReceiverHistoryTest(unittest.TestCase):
    LIBRARY = "DCNC0911"

    def setUp(self):
        journal = {"JOURNAL_LIBRARY": self.LIBRARY, "JOURNAL_NAME": "QSQJRN", "JOURNAL_TYPE": "*LOCAL", "JOURNAL_STATE": "*ACTIVE",
                   "NUMBER_JOURNAL_RECEIVERS": 2, "ATTACHED_JOURNAL_RECEIVER_LIBRARY": self.LIBRARY, "ATTACHED_JOURNAL_RECEIVER_NAME": "RCV",
                   "JOURNAL_CACHE": "NO", "REMOVE_INTERNAL_ENTRIES": "NO", "REMOVE_FIXED_LENGTH_DETAIL": "NO"}
        old = {"JOURNAL_LIBRARY": self.LIBRARY, "JOURNAL_NAME": "QSQJRN", "JOURNAL_RECEIVER_LIBRARY": self.LIBRARY,
               "JOURNAL_RECEIVER_NAME": "OLD", "STATUS": "ONLINE", "FIRST_SEQUENCE_NUMBER": 1, "LAST_SEQUENCE_NUMBER": 9,
               "PREVIOUS_JOURNAL_RECEIVER_LIBRARY": None, "PREVIOUS_JOURNAL_RECEIVER": None,
               "NEXT_JOURNAL_RECEIVER_LIBRARY": self.LIBRARY, "NEXT_JOURNAL_RECEIVER": "RCV", "PENDING_TRANSACTIONS": "YES"}
        current = dict(old, JOURNAL_RECEIVER_NAME="RCV", STATUS="ATTACHED", FIRST_SEQUENCE_NUMBER=10, LAST_SEQUENCE_NUMBER=20,
                       PREVIOUS_JOURNAL_RECEIVER_LIBRARY=self.LIBRARY, PREVIOUS_JOURNAL_RECEIVER="OLD",
                       NEXT_JOURNAL_RECEIVER_LIBRARY=None, NEXT_JOURNAL_RECEIVER=None, PENDING_TRANSACTIONS="NO")
        self.sample = {"status": "observed", "before": [journal], "after": [copy.deepcopy(journal)], "receivers": [old, current]}
        self.bracket = [entry(15, 0, "R/PT"), entry(21, 0, "R/PT")]

    def candidate(self):
        return probe.receiver_history_floor(self.sample, self.bracket, self.LIBRARY)

    def test_silent_cycle_in_old_receiver_requires_its_history_even_without_census(self):
        result = self.candidate()
        self.assertEqual(result["status"], "receiver-floor-candidate")
        self.assertEqual(result["floor"], {"library": self.LIBRARY, "receiver": "OLD", "sequence": "1"})
        self.assertFalse(result["startup_proof"])
        self.assertFalse(result["visibility_proof"])

    def test_cycle_ending_during_scan_cannot_hide_start_behind_cleared_flag(self):
        self.sample["receivers"][0]["PENDING_TRANSACTIONS"] = "NO"
        self.bracket = [entry(15, 0, "R/PT"), entry(21, 5, "C/RB")]
        self.assertEqual(self.candidate()["floor"]["sequence"], "5")
        self.sample["receivers"][0]["FIRST_SEQUENCE_NUMBER"] = 6
        self.assertIn("history is unavailable", self.candidate()["reason"])

    def test_new_cycle_and_zero_pending_count_are_not_treated_as_closed(self):
        self.sample["receivers"][0]["PENDING_TRANSACTIONS"] = "NO"
        self.bracket = [entry(15, 15, "C/SC"), entry(21, 15, "C/SU")]
        result = self.candidate()
        self.assertEqual(result["bracket_cycles"], [15])
        self.assertEqual(result["floor"]["sequence"], "15")
        self.assertFalse(result["startup_proof"])

    def test_all_no_is_only_an_observation_floor_not_a_checkpoint_or_old_offset(self):
        self.sample["receivers"][0]["PENDING_TRANSACTIONS"] = "NO"
        result = self.candidate()
        self.assertEqual(result["floor"]["sequence"], "15")
        self.assertNotIn("completed", result)
        self.assertNotIn("checkpoint", result)
        self.assertFalse(result["startup_proof"])

    def test_missing_or_unreadable_old_receiver_and_partial_links_reject(self):
        original = copy.deepcopy(self.sample)
        for mutation in [lambda s: s["receivers"].pop(0),
                         lambda s: s["receivers"][0].update(STATUS="FREED"),
                         lambda s: s["receivers"][0].update(PENDING_TRANSACTIONS=None),
                         lambda s: s["receivers"][0].update(NEXT_JOURNAL_RECEIVER=None),
                         lambda s: s["receivers"][1].update(FIRST_SEQUENCE_NUMBER=2),
                         lambda s: s["receivers"][1].update(PREVIOUS_JOURNAL_RECEIVER="MISSING")]:
            self.sample = copy.deepcopy(original)
            mutation(self.sample)
            self.assertEqual(self.candidate()["status"], "rejected")

    def test_rotation_abnormal_recovery_cache_filter_and_directory_races_reject(self):
        original = copy.deepcopy(self.sample)
        for kind in ["J/PR", "J/NR", "J/RD", "J/RR", "C/BA", "C/CN"]:
            with self.subTest(kind=kind):
                self.bracket = [entry(15, 0, kind)]
                self.assertEqual(self.candidate()["status"], "rejected")
        self.bracket = [entry(15, 0, "R/PT")]
        for field, value in [("NUMBER_JOURNAL_RECEIVERS", 3), ("JOURNAL_TYPE", "*REMOTE"), ("JOURNAL_CACHE", "YES"),
                             ("REMOVE_INTERNAL_ENTRIES", "YES"), ("REMOVE_FIXED_LENGTH_DETAIL", "YES")]:
            self.sample = copy.deepcopy(original)
            for which in ["before", "after"]:
                self.sample[which][0][field] = value
            self.assertEqual(self.candidate()["status"], "rejected")
        self.sample = original
        self.sample["after"][0]["ATTACHED_JOURNAL_RECEIVER_NAME"] = "NEW"
        self.assertEqual(self.candidate()["status"], "rejected")

    def test_collector_is_bounded_parameterized_and_propagates_partial_observation(self):
        with patch.object(probe, "rows", side_effect=[self.sample["before"], self.sample["receivers"], self.sample["after"]]) as read:
            result = probe.receiver_history(object(), self.LIBRARY)
        self.assertEqual(result["status"], "observed")
        self.assertEqual([call.args[2:] for call in read.call_args_list],
                         [((self.LIBRARY, "QSQJRN"), 1), ((self.LIBRARY, "QSQJRN"), 16), ((self.LIBRARY, "QSQJRN"), 1)])
        for error in [TimeoutError("timeout"), ValueError("SQL warning"), ValueError("overflow")]:
            with patch.object(probe, "rows", side_effect=error):
                result = probe.receiver_history(object(), self.LIBRARY)
            self.assertEqual(result["status"], "rejected")
            self.assertEqual(probe.receiver_history_floor(result, self.bracket, self.LIBRARY)["status"], "rejected")

    def test_nonatomic_receiver_scan_retains_every_cycle_spanning_the_observation_bracket(self):
        # Independent schedule model: old cycles can finish before, during or
        # after either receiver read. A new cycle can start inside the bracket.
        # Pending flags are evaluated at separate read instants, not at an
        # invented atomic snapshot. Terminal entries appear before flags clear.
        original = copy.deepcopy(self.sample)
        for end_a, end_b, old_read, new_read in itertools.product([12, 16, 19, 23], [13, 18, 24], [15, 17], [18, 20]):
            self.sample = copy.deepcopy(original)
            cycles = [(5, end_a), (11, end_b), (17, 24)]
            self.sample["receivers"][0]["PENDING_TRANSACTIONS"] = "YES" if end_a > old_read else "NO"
            self.sample["receivers"][1]["PENDING_TRANSACTIONS"] = "YES" if 11 <= new_read < end_b or 17 <= new_read < 24 else "NO"
            events = [(14, 0, "R/PT"), (17, 17, "C/SC"), (22, 0, "R/PT")]
            events += [(end, start, "C/CM") for start, end in cycles if 14 <= end <= 22]
            # A receiver's observation time and journal order are separate
            # coordinates here; duplicate model event coordinates are invalid.
            if len({event[0] for event in events}) != len(events):
                continue
            self.bracket = [entry(*event) for event in sorted(events)]
            result = self.candidate()
            self.assertEqual(result["status"], "receiver-floor-candidate")
            floor = int(result["floor"]["sequence"])
            self.assertTrue(all(floor <= start for start, end in cycles if start <= 22 and end >= 14),
                            (end_a, end_b, old_read, new_read, result))


class CensusProofTest(unittest.TestCase):
    def setUp(self):
        self.old = [entry(5, 5, "C/SC"), entry(6, 5, "R/PT")]
        self.new = [entry(12, 12, "C/SC"), entry(13, 12, "R/PT"), entry(14, 12, "C/CM")]

    def sample(self, *cycles):
        return {"status": "observed", "resources": [{"COMMIT_CYCLE": cycle} for cycle in cycles]}

    def test_silent_cycle_requires_inventory_even_with_complete_new_journal_work(self):
        oracle = self.old + self.new
        complete = probe.audit_inventory(self.sample(5), self.new, oracle, 10, 15)
        self.assertEqual(complete["status"], "capture-consistent")
        self.assertEqual(complete["required_cycles"], [5, 12])
        self.assertFalse(complete["startup_proof"])
        missed = probe.audit_inventory(self.sample(), self.new, oracle, 10, 15)
        self.assertEqual(missed["status"], "rejected")
        self.assertEqual(missed["missed_cycles"], [5])

    def test_terminal_record_recovers_cycle_which_disappeared_before_lookup(self):
        terminal = entry(11, 5, "C/CM")
        result = probe.audit_inventory(self.sample(), [terminal], self.old + [terminal], 10, 11)
        self.assertEqual(result["status"], "capture-consistent")
        self.assertEqual(result["observed_cycles"], [5])

    def test_definition_reuse_does_not_merge_two_cycles(self):
        terminal = entry(11, 5, "C/RB")
        bracket = [terminal] + self.new[:2]
        result = probe.audit_inventory(self.sample(12), bracket, self.old + bracket, 10, 13)
        self.assertEqual(result["required_cycles"], [5, 12])
        self.assertEqual(result["observed_cycles"], [5, 12])

    def test_matching_captures_do_not_prove_work_absent_from_both_was_not_lost(self):
        result = probe.audit_inventory(self.sample(), self.new, self.new, 10, 15)
        self.assertEqual(result["status"], "capture-consistent")
        self.assertEqual(result["oracle_completeness"], "unqualified-pending-writer-ledger-replay")
        self.assertFalse(result["startup_proof"])

    def test_missing_retained_start_rejects_instead_of_inventing_closed_floor(self):
        result = probe.audit_inventory(self.sample(5), self.new, self.old[1:] + self.new, 10, 15)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(result["reason"], "Incomplete oracle history")

    def test_filtered_or_late_visible_terminal_rejects_even_if_inventory_has_cycle(self):
        terminal = entry(11, 5, "C/CM")
        result = probe.audit_inventory(self.sample(5), [], self.old + [terminal], 10, 11)
        self.assertEqual(result["status"], "rejected")
        self.assertIn("bracket disagrees", result["reason"])

    def test_changed_anchor_bytes_reject(self):
        changed = {**self.new[0], "sha256": "changed"}
        result = probe.audit_inventory(self.sample(5), [changed] + self.new[1:], self.old + self.new, 10, 15)
        self.assertEqual(result["status"], "rejected")

    def test_native_content_comparison_preserves_mismatch_and_representation_guards(self):
        oracle = [{**item, "content_sha256": "content-" + item["sha256"]} for item in self.old + self.new]
        bracket = [{**item, "sha256": "different-transport"} for item in oracle[2:]]
        self.assertEqual(probe.audit_inventory(self.sample(5), bracket, oracle, 10, 15)["status"], "capture-consistent")
        for fault in ["content", "position", "mixed-representation"]:
            with self.subTest(fault=fault):
                changed = copy.deepcopy(bracket)
                if fault == "content":
                    changed[0]["content_sha256"] = "changed"
                elif fault == "position":
                    changed[0]["position"]["receiver"] = "OTHER"
                else:
                    del changed[0]["content_sha256"]
                self.assertEqual(probe.audit_inventory(self.sample(5), changed, oracle, 10, 15)["status"], "rejected")

    def test_incomplete_census_cannot_pass_on_matching_journal(self):
        result = probe.audit_inventory({"status": "rejected"}, self.new, self.old + self.new, 10, 15)
        self.assertEqual(result["reason"], "Incomplete census")

    def test_zero_pending_and_complete_rollback_percentage_keep_positive_cycle(self):
        definition = {"LOCK_SPACE_ID": "UDB_0000000000000001", "JOB_NAME": "000001/TEST/JOB", "ASPGRP": "*SYSBAS",
                      "RESOURCE_LOCATION": "LOCAL", "XA_TRANSACTION_BRANCH_STATE": None}
        resource = {"JOURNAL_LIBRARY": "DCNC0911", "JOURNAL_NAME": "QSQJRN", "COMMIT_CYCLE": 5,
                    "PENDING_CHANGES": 0, "RECORD_ROLLBACK_PERCENT": 100}
        authority = [{"JOBCTL": 1, "SQLADM": 0, "SYSMON": 0}]
        with patch.object(probe, "rows", side_effect=[authority, [definition], [resource], authority]) as read:
            result = probe.census(object(), "DCNC0911")
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["resources"][0]["COMMIT_CYCLE"], 5)
        self.assertEqual(read.call_args_list[2].args[2], (definition["LOCK_SPACE_ID"], "DCNC0911", "QSQJRN"))

    def test_authority_denial_precedes_enumeration_and_workload_hook(self):
        with patch.object(probe, "rows", return_value=[{"JOBCTL": 0, "SQLADM": 0, "SYSMON": 0}]) as read:
            result = probe.census(object(), "DCNC0911", lambda definitions: self.fail("hook must not run"))
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(read.call_count, 1)

    def test_warning_timeout_and_partial_read_are_rejections(self):
        for error in [ValueError("SQL warning"), TimeoutError("deadline"), ValueError("Bounded SQL response overflow")]:
            with self.subTest(error=error), patch.object(probe, "rows", side_effect=error):
                result = probe.census(object(), "DCNC0911")
                self.assertEqual(result["status"], "rejected")
                self.assertFalse(result["startup_proof"])

    def test_signon_preflight_failure_cannot_create_fixture(self):
        experiment = probe.Experiment.__new__(probe.Experiment)
        experiment.fixture = Mock()
        experiment.capture = Mock(side_effect=RuntimeError("sign-on dropped"))
        with self.assertRaisesRegex(RuntimeError, "sign-on dropped"):
            experiment.run()
        experiment.fixture.setup.assert_not_called()

    def test_denied_jdbc_authority_cannot_create_fixture(self):
        experiment = probe.Experiment.__new__(probe.Experiment)
        experiment.report = {}
        experiment.fixture = Mock()
        preflight = Mock()
        preflight.read_text.return_value = '{"authority":{"JOBCTL":0,"SQLADM":0,"SYSMON":0}}'
        experiment.capture = Mock(return_value=preflight)
        with self.assertRaisesRegex(ValueError, "visibility"):
            experiment.run()
        experiment.fixture.setup.assert_not_called()

    def test_clear_preparation_starts_cycle_without_changing_the_clear_target(self):
        for scenario in ["clear-commit", "clear-rollback", "optimized-delete", "savepoint"]:
            with self.subTest(scenario=scenario):
                experiment = probe.Experiment.__new__(probe.Experiment)
                experiment.library = "DCNC0911"
                experiment.insert = Mock()
                experiment.execute = Mock()
                experiment.capture = Mock(side_effect=RuntimeError("end of preparation"))
                with self.assertRaisesRegex(RuntimeError, "end of preparation"):
                    experiment.attempt(0, scenario, "silent", "changing", "observer")
                self.assertEqual(experiment.insert.call_args_list[0].args, ("changing", "M"))
                clears = [call.args[2] for call in experiment.execute.call_args_list if call.args[1] in ["clear", "truncate"]]
                self.assertEqual(len(clears), 1)
                self.assertIn("DCNC0911.B", clears[0])


class EnumerationOverlapTest(unittest.TestCase):
    def setUp(self):
        self.job = "123456/TEST/CONCURRENT"
        point = lambda n: {"monotonic_ns": n}
        self.sample = {"status": "observed", "queries": [{"name": "definitions", "started": point(10), "finished": point(20)}]}
        self.ledger = [{"operation": "connection", "job": self.job, "transaction": True, "read_only": False},
                       {"operation": "insert", "job": self.job, "status": "completed", "started": point(5), "finished": point(12)},
                       {"operation": "commit", "job": self.job, "status": "completed", "started": point(13), "finished": point(18)}]

    def audit(self):
        return probe.audit_enumeration_overlap(self.sample, self.ledger, self.job)

    def test_successful_terminal_call_must_fit_inside_actual_definition_call(self):
        result = self.audit()
        self.assertEqual(result["status"], "client-overlap-observed")
        self.assertFalse(result["startup_proof"])
        self.assertFalse(result["contained_terminals"][0]["whole_cycle_contained"])
        self.assertEqual(result["contained_terminals"][0]["terminal_ledger_index"], 2)
        self.ledger[-1]["operation"] = "rollback"
        self.assertEqual(self.audit()["contained_terminals"][0]["operation"], "rollback")

    def test_partial_overlap_or_completion_after_enumeration_does_not_count(self):
        for start, end in [(8, 15), (15, 21), (21, 23)]:
            with self.subTest(start=start, end=end):
                self.ledger[1]["started"]["monotonic_ns"] = 1
                self.ledger[1]["finished"]["monotonic_ns"] = 2
                self.ledger[-1]["started"]["monotonic_ns"] = start
                self.ledger[-1]["finished"]["monotonic_ns"] = end
                self.assertEqual(self.audit()["status"], "not-observed")
        self.ledger = self.ledger[:1]  # Query can return before the writer is scheduled.
        self.assertEqual(self.audit()["status"], "not-observed")
        self.assertEqual(self.audit()["completed_cycles"], 0)

    def test_bad_timing_incomplete_queries_and_writer_failures_reject(self):
        mutations = [lambda: self.sample.update(status="rejected"),
                     lambda: self.sample["queries"].append(copy.deepcopy(self.sample["queries"][0])),
                     lambda: self.sample["queries"][0]["started"].update(monotonic_ns=True),
                     lambda: self.ledger[-1].update(status="failed"),
                     lambda: self.ledger[-1]["started"].update(monotonic_ns=11),
                     lambda: self.ledger.pop(),
                     lambda: self.ledger[0].update(transaction=False),
                     lambda: self.ledger.append(copy.deepcopy(self.ledger[0]))]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                sample, ledger = copy.deepcopy(self.sample), copy.deepcopy(self.ledger)
                mutate()
                self.assertEqual(self.audit()["status"], "rejected")
                self.sample, self.ledger = sample, ledger

    def test_run_cannot_pass_when_every_race_missed_or_any_timing_evidence_rejected(self):
        cases = [([], False), (["not-observed"], False), (["client-overlap-observed", "not-observed"], True),
                 (["client-overlap-observed", "rejected"], False), (["client-overlap-observed", "unknown"], False)]
        for statuses, expected in cases:
            with self.subTest(statuses=statuses):
                self.assertEqual(probe.enumeration_covered([{"enumeration_overlap": {"status": status}} for status in statuses]), expected)


class ConcurrentWriterTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.fixture = Mock(created=False)
        self.fixture.redact.side_effect = str
        self.fixture.cleanup.return_value = {"status": "passed"}
        with patch.object(probe.native, "NativeFixture", return_value=self.fixture):
            self.experiment = probe.Experiment(Path(directory.name) / "evidence", "DCNC0911", 1)
        self.experiment.deadline = time.monotonic() + 60
        self.job = "123456/TEST/CONCURRENT"
        self.definition_entered = threading.Event()
        self.terminal_finished = threading.Event()
        self.wait_for_query = False
        self.fail_terminal = False
        self.stop_during_insert = False
        self.sql = []

        def connection(transaction):
            self.assertTrue(transaction)
            self.experiment.ledger.append({"operation": "connection", "job": self.job, "transaction": True, "read_only": False})
            return object(), self.job

        def execute(session, operation, sql=None):
            self.sql.append((operation, sql))
            item = {"operation": operation, "job": session[1], "started": probe.stamp(), "sql": sql}
            self.experiment.ledger.append(item)
            try:
                if operation == "insert":
                    if self.wait_for_query and not self.definition_entered.wait(2):
                        raise TimeoutError("Test query did not start")
                    if self.stop_during_insert:
                        self.experiment.churn_stop.set()
                elif self.fail_terminal:
                    raise RuntimeError("controlled writer failure")
                item["status"] = "completed"
            except Exception:
                item["status"] = "failed"
                raise
            finally:
                item["finished"] = probe.stamp()
                if operation in ["commit", "rollback"]:
                    self.terminal_finished.set()

        self.experiment.connection = connection
        self.experiment.execute = execute
        self.addCleanup(self.stop_worker)

    def stop_worker(self):
        self.experiment.stop.set()
        self.experiment.churn_stop.set()
        self.experiment.churn_go.set()
        self.definition_entered.set()
        if self.experiment.churn_thread is not None and isinstance(self.experiment.churn_thread, threading.Thread):
            self.experiment.churn_thread.join(3)
            self.assertFalse(self.experiment.churn_thread.is_alive())

    def query(self, connection, sql, parameters=(), maximum=128):
        if sql == probe.AUTHORITY:
            return [{"JOBCTL": 1, "SQLADM": 0, "SYSMON": 0}]
        self.assertEqual(sql, probe.DEFINITIONS)
        self.definition_entered.set()
        if not self.terminal_finished.wait(2):
            raise TimeoutError("Test writer did not complete")
        return []

    def test_real_thread_turnover_is_measured_inside_query_and_drains(self):
        self.wait_for_query = True
        with patch.object(probe, "rows", side_effect=self.query):
            sample, overlap = self.experiment.concurrent_census(object())
        self.assertEqual(sample["status"], "observed")
        self.assertEqual(overlap["status"], "client-overlap-observed")
        self.assertFalse(self.experiment.churn_thread.is_alive())
        self.assertTrue(self.experiment.report["concurrent_writers"][0]["drained"])
        self.assertFalse(self.experiment.background_error)

    def test_authority_denial_never_releases_writer_to_sql(self):
        denied = [{"JOBCTL": 0, "SQLADM": 0, "SYSMON": 0}]
        with patch.object(probe, "rows", return_value=denied):
            sample, overlap = self.experiment.concurrent_census(object())
        self.assertEqual(sample["status"], "rejected")
        self.assertEqual(overlap["status"], "rejected")
        self.assertFalse(self.sql)
        self.assertFalse(self.experiment.churn_thread.is_alive())

    def test_failed_terminal_cannot_be_reported_as_successful_overlap(self):
        self.wait_for_query = True
        self.fail_terminal = True
        with patch.object(probe, "rows", side_effect=self.query):
            with self.assertRaisesRegex(RuntimeError, "controlled writer failure"):
                self.experiment.concurrent_census(object())
        self.assertFalse(self.experiment.churn_thread.is_alive())
        self.assertEqual(self.experiment.report["background_errors"], ["controlled writer failure"])
        self.assertIn("census", self.experiment.report["concurrent_writers"][0])

    def test_signon_failure_prevents_queries_and_releases_controller(self):
        self.experiment.connection = Mock(side_effect=RuntimeError("controlled sign-on failure"))
        with patch.object(probe, "rows") as read:
            with self.assertRaisesRegex(RuntimeError, "failed to become ready"):
                self.experiment.concurrent_census(object())
        read.assert_not_called()
        self.assertFalse(self.sql)
        self.assertFalse(self.experiment.churn_thread.is_alive())
        self.assertEqual(self.experiment.background_error, ["controlled sign-on failure"])

    def test_cycle_budget_and_stop_during_insert_finish_admitted_work(self):
        self.experiment.churn_go.set()
        self.experiment.enumeration_writer()
        self.assertEqual(len(self.sql), 128)
        self.assertEqual([op for op, _ in self.sql].count("commit"), 32)
        self.assertEqual([op for op, _ in self.sql].count("rollback"), 32)
        self.assertEqual(len({sql for op, sql in self.sql if op == "insert"}), 64)
        self.sql.clear()
        self.stop_during_insert = True
        self.experiment.enumeration_writer()
        self.assertEqual([op for op, _ in self.sql], ["insert", "commit"])

    def test_expired_workload_deadline_starts_no_sql(self):
        self.experiment.churn_go.set()
        self.experiment.deadline = time.monotonic() - 1
        self.experiment.connection = Mock()
        self.experiment.enumeration_writer()
        self.experiment.connection.assert_not_called()
        self.assertFalse(self.sql)
        self.assertIn("budget exhausted", self.experiment.background_error[0])

    def test_undrained_writer_blocks_connection_close_and_fixture_deletion(self):
        thread = Mock()
        thread.is_alive.return_value = True
        self.experiment.churn_thread = thread
        connection = Mock(autocommit=False)
        self.experiment.connections.append(connection)
        self.experiment.cleanup()
        thread.join.assert_called_once_with(timeout=20)
        connection.rollback.assert_not_called()
        connection.close.assert_not_called()
        self.fixture.cleanup.assert_not_called()
        self.assertEqual(self.experiment.report["fixture_cleanup"]["status"], "blocked")


class NativeDriverContractTest(unittest.TestCase):
    def setUp(self):
        self.experiment = probe.Experiment.__new__(probe.Experiment)
        self.experiment.ledger = []
        self.experiment.fixture = Mock()
        self.experiment.fixture.redact.side_effect = str
        self.connection = Mock(autocommit=False)
        self.connection.pending = []
        self.connection.committed = []
        self.connection.commit.side_effect = self.commit
        self.connection.rollback.side_effect = self.connection.pending.clear
        self.cursors = []

        def cursor():
            result = MagicMock(messages=[])
            result.execute.side_effect = lambda sql: self.connection.pending.append(sql)
            result.__enter__.return_value = result
            # Model the documented pyodbc behavior which caused native SQL0880.
            result.__exit__.side_effect = lambda *args: self.connection.commit()
            self.cursors.append(result)
            return result

        self.connection.cursor.side_effect = cursor
        self.session = self.connection, "123456/TEST/WRITER"

    def commit(self):
        self.connection.committed.extend(self.connection.pending)
        self.connection.pending.clear()

    def test_writer_statements_remain_pending_until_explicit_commit_or_rollback(self):
        for operation, sql in [("insert", "INSERT INTO DCNC0912.M VALUES (1)"),
                               ("savepoint", "SAVEPOINT S1 ON ROLLBACK RETAIN CURSORS"),
                               ("clear", "TRUNCATE TABLE DCNC0912.B REUSE STORAGE IGNORE DELETE TRIGGERS")]:
            self.experiment.execute(self.session, operation, sql)
        self.assertEqual(len(self.connection.pending), 3)
        self.assertEqual(self.connection.committed, [])
        self.connection.commit.assert_not_called()
        self.experiment.execute(self.session, "rollback")
        self.assertEqual(self.connection.pending, [])
        self.assertEqual(self.connection.committed, [])
        self.experiment.execute(self.session, "insert", "INSERT INTO DCNC0912.M VALUES (2)")
        self.experiment.execute(self.session, "commit")
        self.assertEqual(self.connection.committed, ["INSERT INTO DCNC0912.M VALUES (2)"])
        self.connection.commit.assert_called_once()
        for cursor in self.cursors:
            cursor.close.assert_called_once()

    def test_statement_failure_or_warning_closes_cursor_without_ending_transaction(self):
        make_cursor = self.connection.cursor
        for failure in ["execute", "warning"]:
            with self.subTest(failure=failure):
                cursor = make_cursor()
                if failure == "execute":
                    cursor.execute.side_effect = RuntimeError("controlled SQL failure")
                else:
                    cursor.messages = [("warning", "partial work")]
                self.connection.cursor = Mock(return_value=cursor)
                with self.assertRaisesRegex((RuntimeError, ValueError), "controlled SQL failure" if failure == "execute" else "Writer SQL warning"):
                    self.experiment.execute(self.session, "insert", "INSERT INTO DCNC0912.M VALUES (3)")
                cursor.close.assert_called_once()
                self.connection.commit.assert_not_called()
                self.connection.rollback.assert_not_called()
                self.assertEqual(self.experiment.ledger[-1]["status"], "failed")

    def test_census_accepts_169_definitions_but_rejects_overflow_before_resource_lookups(self):
        authority = [{"JOBCTL": 1, "SQLADM": 0, "SYSMON": 0}]
        for count in [169, probe.MAX_DEFINITIONS + 1]:
            with self.subTest(count=count):
                definitions = [{"LOCK_SPACE_ID": f"UDB_{i:016d}"} for i in range(count)]
                resource_queries = []

                def read(connection, sql, parameters=(), maximum=128):
                    if sql == probe.AUTHORITY:
                        return authority
                    if sql == probe.DEFINITIONS:
                        self.assertEqual(maximum, 256)
                        if count > maximum:
                            raise ValueError("Bounded SQL response overflow")
                        return definitions
                    resource_queries.append(parameters)
                    return []

                with patch.object(probe, "rows", side_effect=read):
                    result = probe.census(object(), "DCNC0912")
                self.assertEqual(result["status"], "observed" if count == 169 else "rejected")
                self.assertEqual(len(resource_queries), 169 if count == 169 else 0)
                self.assertFalse(result["startup_proof"])


class WriterLedgerTest(unittest.TestCase):
    def setUp(self):
        self.job = "123456/TEST/WRITER"
        self.other = "123457/TEST/WRITER"
        self.library = "DCNC0911"
        self.ledger = [{"operation": "connection", "job": job, "transaction": transaction, "read_only": False}
                       for job, transaction in [(self.job, True), (self.other, False)]]

        def command(operation, sql=None, job=None):
            self.ledger.append({"operation": operation, "sql": sql, "job": job or self.job, "status": "completed"})

        command("insert", "INSERT INTO DCNC0911.B VALUES (10)")
        command("unrelated-insert", "INSERT INTO DCNC0911.A VALUES (1000001)", self.other)
        command("savepoint", "SAVEPOINT S1 ON ROLLBACK RETAIN CURSORS")
        command("clear", "TRUNCATE TABLE DCNC0911.B REUSE STORAGE IGNORE DELETE TRIGGERS")
        command("insert", "INSERT INTO DCNC0911.B VALUES (11)")
        command("rollback-to-savepoint", "ROLLBACK TO SAVEPOINT S1")
        command("insert", "INSERT INTO DCNC0911.B VALUES (12)")
        command("commit")
        command("rollback")  # No native outcome is expected for this redundant call.

        def event(sequence, kind, identifier=None, job=None, cycle=5, file="B", **extra):
            return {"sequence": str(sequence), "cycle": str(cycle), "job": job or self.job, "type": kind,
                    "library": self.library, "file": file, **({"id": identifier} if identifier is not None else {}), **extra}

        self.replay = {"status": "replayed", "startup_proof": False, "events": [
            event(5, "C/SC"), event(6, "R/PT", 10), event(7, "R/PT", 1000001, self.other, 0, "A"),
            event(8, "C/SB"), event(9, "C/SB"), event(10, "D/CG", subtype="8"), event(11, "C/SQ", target="9"),
            event(12, "R/PX", 11), event(13, "C/SU", target="8"), event(14, "C/SB"), event(15, "R/PT", 12), event(16, "C/CM")],
            "batches": [{"sequence": "7", "cycle": "0", "job": self.other, "changes": [self.change("7", 1000001, "A")]},
                        {"sequence": "16", "cycle": "5", "job": self.job, "changes": [self.change("6", 10), self.change("15", 12)]}]}

    def change(self, sequence, identifier, file="B"):
        return {"sequence": sequence, "operation": "c", "library": self.library, "file": file, "id": identifier}

    def audit(self):
        return probe.audit_ledger(self.ledger, self.replay, self.library)

    def test_sql_savepoints_and_unrelated_progress_match_independently_replayed_output(self):
        result = self.audit()
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["completed_batches"], 2)
        self.assertFalse(result["startup_proof"])

    def test_entire_cycle_missing_from_both_captures_still_fails_writer_reconciliation(self):
        self.replay["events"] = [e for e in self.replay["events"] if e["job"] != self.job]
        self.replay["batches"] = [b for b in self.replay["batches"] if b["job"] != self.job]
        self.assertEqual(self.audit()["status"], "rejected")

    def test_wrong_row_identity_producer_scope_or_cycle_cannot_match(self):
        for field, value in [("id", 999), ("file", "M"), ("library", "OTHER"), ("job", self.other), ("cycle", "99")]:
            with self.subTest(field=field):
                original = copy.deepcopy(self.replay)
                self.replay["events"][-2][field] = value
                self.assertEqual(self.audit()["status"], "rejected")
                self.replay = original

    def test_wrong_clear_subtype_and_unexplained_journal_record_reject(self):
        self.replay["events"][5]["subtype"] = "3"
        self.assertEqual(self.audit()["status"], "rejected")
        self.replay["events"][5]["subtype"] = "8"
        self.replay["events"].append({**self.replay["events"][1], "sequence": "17"})
        self.assertEqual(self.audit()["status"], "rejected")

    def test_commit_vs_rollback_is_checked_even_when_rows_match(self):
        self.replay["events"][-1]["type"] = "C/RB"
        self.assertEqual(self.audit()["status"], "rejected")
        self.ledger[-2]["operation"] = "rollback"
        self.assertEqual(self.audit()["status"], "rejected")  # Leaked committed batch.
        self.replay["batches"].pop()
        self.assertEqual(self.audit()["status"], "matched")

    def test_savepoint_effects_must_match_the_buffer_output(self):
        self.replay["batches"][-1]["changes"].insert(1, self.change("12", 11))
        self.assertEqual(self.audit()["status"], "rejected")

    def test_reordered_or_partial_committed_batch_rejects(self):
        self.replay["batches"][-1]["changes"].reverse()
        self.assertEqual(self.audit()["status"], "rejected")
        self.replay["batches"][-1]["changes"].pop()
        self.assertEqual(self.audit()["status"], "rejected")

    def test_incomplete_ledger_and_replay_never_match(self):
        self.ledger[-2]["status"] = "failed"
        self.assertEqual(self.audit()["status"], "rejected")
        self.ledger[-2]["status"] = "completed"
        self.replay["status"] = "rejected"
        self.assertEqual(self.audit()["status"], "rejected")

    def test_unknown_sql_and_still_open_writer_state_reject(self):
        self.ledger[2]["sql"] = "INSERT INTO DCNC0911.B SELECT ID FROM OTHER"
        self.assertEqual(self.audit()["status"], "rejected")
        self.ledger[2]["sql"] = "INSERT INTO DCNC0911.B VALUES (10)"
        self.ledger = self.ledger[:-2]
        self.replay["events"].pop()
        self.replay["batches"].pop()
        self.assertEqual(self.audit()["status"], "rejected")


if __name__ == "__main__":
    unittest.main()
