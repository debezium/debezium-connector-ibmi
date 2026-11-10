# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Offline admission/redaction/cleanup tests. Never import a native driver."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

spec = importlib.util.spec_from_file_location("native_fixture", Path(__file__).with_name("dcg-native-fixture.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


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

    def test_no_default_host_or_implicit_mutation_admission(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                fixture.load_config()
        self.config["allow_isolated_fixture_writes"] = False
        self.path.write_text(json.dumps(self.config))
        with self.assertRaises(ValueError):
            fixture.load_config()

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
