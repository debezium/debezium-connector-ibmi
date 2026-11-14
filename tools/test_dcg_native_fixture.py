# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Offline admission/redaction/cleanup tests. Never import a native driver."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock

from probe_test_support import load_probe

fixture = load_probe("dcg-native-fixture.py", "native_fixture")


class NativeFixtureTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.path = self.root / "credentials.json"
        self.config = {"host": "test.invalid", "user": "test-user", "password": "sensitive-password",
                       "server": "TESTSYS", "allow_isolated_fixture_writes": True}
        self.path.write_text(json.dumps(self.config))
        self.path.chmod(0o600)
        environment = patch.dict(os.environ, {"IBMI_TEST_CONFIG": str(self.path)})
        environment.start()
        self.addCleanup(environment.stop)

    def test_java_home_takes_precedence_without_silent_fallback_to_another_jdk(self):
        java = self.root / "jdk with spaces" / "bin" / "java"
        java.parent.mkdir(parents=True)
        java.write_text("#!/bin/sh\nexit 0\n")
        java.chmod(0o700)
        with patch.dict(os.environ, {"JAVA_HOME": str(java.parent.parent)}), patch.object(fixture.shutil, "which") as search:
            self.assertEqual(str(java), fixture.java_executable())
            java.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "JAVA_HOME"):
                fixture.java_executable()
            java.unlink()
            with self.assertRaisesRegex(ValueError, "JAVA_HOME"):
                fixture.java_executable()
            search.assert_not_called()

    def test_java_can_use_path_without_a_version_manager_and_reports_a_missing_jdk(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture.shutil, "which", return_value="/example/jdk/bin/java"):
            self.assertEqual("/example/jdk/bin/java", fixture.java_executable())
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture.shutil, "which", return_value=None):
            with self.assertRaisesRegex(ValueError, "JDK 21"):
                fixture.java_executable()

    def test_no_default_host_or_implicit_mutation_admission(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                fixture.load_config()
        self.config["allow_isolated_fixture_writes"] = False
        self.path.write_text(json.dumps(self.config))
        with self.assertRaises(ValueError):
            fixture.load_config()

    def test_optimized_python_rejects_native_admission_before_reading_credentials(self):
        script = """
import importlib.util
from pathlib import Path
import sys
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('native_fixture', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
with patch.object(Path, 'read_text', side_effect=RuntimeError('credentials read before admission')):
    module.load_config()
"""
        for flag, optimization in [("-O", None), ("-OO", None), (None, "1"), (None, "2")]:
            with self.subTest(flag=flag, optimization=optimization):
                environment = dict(os.environ)
                environment.pop("PYTHONOPTIMIZE", None)
                if optimization is not None:
                    environment["PYTHONOPTIMIZE"] = optimization
                command = [sys.executable, "-B", *([flag] if flag else []), "-c", script, fixture.__file__]
                result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("Native test runners require Python assertions enabled", result.stderr)
                self.assertNotIn("RuntimeError: credentials read before admission", result.stderr)

    def test_reject_shared_or_symlink_credentials(self):
        self.path.chmod(0o644)
        with self.assertRaises(ValueError):
            fixture.load_config()
        self.path.chmod(0o600)
        link = self.root / "link"
        link.symlink_to(self.path)
        with patch.dict(os.environ, {"IBMI_TEST_CONFIG": str(link)}):
            with self.assertRaises(ValueError):
                fixture.load_config()

    def test_generic_credentials_are_not_retained_in_evidence(self):
        source = fixture.NativeFixture(self.root / "evidence", "DCNC9991")
        self.assertEqual("TESTSYS", source.server_name)
        self.assertEqual([], list(source.directory.iterdir()))
        self.assertNotIn("sensitive-password", source.redact("error sensitive-password test-user test.invalid"))
        source.redactions.add("192.0.2.1")
        self.assertEqual("<redacted>:<redacted> <redacted>", source.redact("test.invalid:192.0.2.1 TESTSYS"))
        self.assertEqual("{a}}b;c}", fixture.odbc_value("a}b;c"))
        with self.assertRaises(ValueError):
            fixture.NativeFixture(self.root / "unsafe", "EXISTING")

    def test_unknown_object_prevents_cleanup(self):
        source = fixture.NativeFixture(self.root / "evidence", "DCNC9991")
        source.created = True
        source.owned_objects = [{"OBJNAME": "A", "OBJTYPE": "*FILE", "OBJATTRIBUTE": "PF", "OBJCREATED": "original"}]
        source.save("owner.json", {"library": source.library, "server": source.server_name})
        source.server = Mock()
        source.inventory = Mock(return_value=[{"OBJNAME": "A", "OBJTYPE": "*FILE", "OBJATTRIBUTE": "PF", "OBJCREATED": "replacement"}])
        source.sql = Mock()
        with self.assertRaisesRegex(AssertionError, "Unexpected objects"):
            source.cleanup()
        source.sql.assert_not_called()

    def test_rotation_keeps_original_file_identity_when_an_open_clear_hides_details(self):
        original = {'OBJNAME': 'B', 'OBJTYPE': '*FILE', 'OBJATTRIBUTE': 'PF', 'OBJCREATED': 'original'}
        hidden = {**original, 'OBJATTRIBUTE': None, 'OBJCREATED': None}
        receiver = {'OBJNAME': 'QSQJRN0003', 'OBJTYPE': '*JRNRCV', 'OBJATTRIBUTE': '', 'OBJCREATED': 'new'}
        self.assertEqual([original, receiver], fixture.NativeFixture.rotation_inventory([original], [hidden, receiver], 'QSQJRN0003'))
        for observed in [[{**original, 'OBJCREATED': 'replacement'}, receiver], [hidden],
                         [hidden, receiver, {**original, 'OBJNAME': 'FOREIGN'}],
                         [hidden, {**receiver, 'OBJCREATED': None}]]:
            with self.assertRaises(ValueError):
                fixture.NativeFixture.rotation_inventory([original], observed, 'QSQJRN0003')


if __name__ == "__main__":
    unittest.main()
