# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Offline operator admission; no source, Kafka or credential access."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

TOOL = Path(__file__).with_name("dcg-start-window")


class StartWindowTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.window = Path(self.folder.name) / "window"

    def prepare(self, extra=(), optimized=False):
        return subprocess.run([sys.executable, *(["-O"] if optimized else []), str(TOOL), "prepare", str(self.window),
                               "--operator", "test-operator", "--journal", "TESTLIB/JRN", "--captured", "TESTLIB.A,TESTLIB.B",
                               "--controlled", "TESTLIB.A,TESTLIB.B", "--seconds", "60", "--rows", "30000", "--bytes", "32000000",
                               "--all-journal-writers-stopped", "--maintenance-stopped", "--operator-managed-retention", *extra],
                              capture_output=True, text=True, timeout=10)

    def test_default_plan_matches_normal_chunked_startup_and_large_total(self):
        result = self.prepare()
        self.assertEqual(0, result.returncode, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual("2", output["plan"]["version"])
        self.assertEqual("30000", output["plan"]["snapshot.rows"])
        self.assertEqual("256", output["plan"]["chunk.rows"])
        self.assertEqual("1000000", output["plan"]["chunk.bytes"])
        self.assertFalse(output["enrolled"])
        self.assertFalse(output["source_pause_proven_by_this_file"])
        self.assertFalse(output["native_drain_proven_by_this_file"])

    def test_explicit_chunk_bounds_remain_pinned(self):
        result = self.prepare(["--chunk-rows", "32", "--chunk-bytes", "64000"])
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("32", json.loads(result.stdout)["plan"]["chunk.rows"])
        self.assertEqual("64000", json.loads(result.stdout)["plan"]["chunk.bytes"])

    def test_legacy_experiment_requires_explicit_bounded_list_plan(self):
        result = self.prepare(["--legacy-list-plan", "--rows", "9000", "--bytes", "16000000"])
        self.assertEqual(0, result.returncode, result.stderr)
        plan = json.loads(result.stdout)["plan"]
        self.assertEqual("1", plan["version"])
        self.assertNotIn("chunk.rows", plan)

    def test_invalid_admission_never_writes_a_plan_even_under_optimized_python(self):
        cases = [["--seconds", "0"], ["--seconds", "1801"], ["--rows", "0"], ["--bytes", str(2**63)],
                 ["--captured", "TESTLIB.A,TESTLIB.A"], ["--controlled", "TESTLIB.A"], ["--journal", "OTHER/JRN\nextra=true"],
                 ["--chunk-rows", "256"], ["--chunk-rows", "257", "--chunk-bytes", "1000"], ["--legacy-list-plan"]]
        for optimized in [False, True]:
            for case in cases:
                with self.subTest(optimized=optimized, case=case):
                    self.assertNotEqual(0, self.prepare(case, optimized).returncode)
                    self.assertFalse(self.window.exists())

    def test_existing_plan_cannot_be_overwritten(self):
        self.assertEqual(0, self.prepare().returncode)
        original = (self.window / "plan.properties").read_bytes()
        self.assertNotEqual(0, self.prepare().returncode)
        self.assertEqual(original, (self.window / "plan.properties").read_bytes())

    def test_revocation_retains_enrollment_and_is_not_completed_native_drain(self):
        self.assertEqual(0, self.prepare().returncode)
        (self.window / "enrolled").write_text("existing-claim\n")
        result = subprocess.run([sys.executable, str(TOOL), "revoke", str(self.window)], capture_output=True, text=True, timeout=10)
        self.assertEqual(0, result.returncode, result.stderr)
        output = json.loads(result.stdout)
        self.assertTrue(output["revoked"])
        self.assertTrue(output["enrolled"])
        self.assertFalse(output["native_drain_proven_by_this_file"])
        self.assertEqual("existing-claim\n", (self.window / "enrolled").read_text())


if __name__ == "__main__":
    unittest.main()
