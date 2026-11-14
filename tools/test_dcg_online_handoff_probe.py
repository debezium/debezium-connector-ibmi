# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Exporter provenance tests; synthetic manifests, no native or Kafka connection."""
import hashlib
import base64
import json
from pathlib import Path
import struct
import tempfile
import unittest

from probe_test_support import load_probe

handoff = load_probe("dcg-online-handoff-probe.py", "handoff")


class LegacyProgressBoundaryTest(unittest.TestCase):
    def test_delete_tombstone_remains_in_same_acknowledged_prefix(self):
        rows = [{"op": "c", "sequence": "10", "id": 1}, {"op": "d", "sequence": "12", "id": 1},
                {"op": "tombstone", "id": 1}, {"op": "c", "sequence": "14", "id": 2}]
        self.assertEqual(rows[:1], handoff.prefix_through(rows, 11))
        self.assertEqual(rows[:3], handoff.prefix_through(rows, 12))
        self.assertEqual(rows, handoff.prefix_through(rows, 14))

    def test_orphan_or_wrong_key_tombstone_cannot_define_a_progress_boundary(self):
        for rows in [[{"op": "tombstone", "id": 1}], [{"op": "d", "sequence": "12", "id": 1}, {"op": "tombstone", "id": 2}]]:
            with self.subTest(rows=rows), self.assertRaisesRegex(ValueError, "Unpaired tombstone"):
                handoff.prefix_through(rows, 20)


class ExportEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ["oracle", "native-bindings"]:
            (self.root / name).mkdir()
        self.entries = []
        for n, kind, cycle in [(1, "R/PT", "0"), (2, "C/SC", "2"), (3, "R/PT", "2"), (4, "C/CM", "2"), (5, "D/CG", "5"), (6, "R/PT", "5")]:
            data = bytearray(20)
            struct.pack_into(">i", data, 16, 7)
            name = f"entry-{n}.bin"
            (self.root / "oracle" / name).write_bytes(data)
            self.entries.append({"file": name, "position": self.position(n), "type": kind, "cycle": cycle, "sha256": hashlib.sha256(data).hexdigest()})
        self.write("lower.json", {"from": self.position(2)})
        self.write("upper.json", {"through": self.position(3)})
        self.write("census.json", {"status": "observed", "resources": [{"COMMIT_CYCLE": "2"}]})
        self.write("native-bindings/bindings.json", {"tables": []})
        bindings_sha = hashlib.sha256((self.root / "native-bindings/bindings.json").read_bytes()).hexdigest()
        self.capture = {"status": "captured", "library": "DCNC9999", "entries": self.entries, "receiver_observation": [], "receiver_recheck": []}
        self.write("oracle/capture.json", self.capture)
        self.report = {"status": "captured-unqualified", "writer_ledger_replay": {"status": "matched"}, "enumeration_overlap_covered": True,
                       "fixture_cleanup": {"library_absent": True}, "attempts": [{"scenario": "silent", "census": "census.json", "lower": "lower.json",
                       "upper": "upper.json", "audit": {"status": "capture-consistent"}, "unrelated_writes_during_observation": 1}]}
        self.write("report.json", self.report)
        self.write("replay.json", {"status": "replayed", "comparison_captures": {"oracle": self.entries}, "bindings_sha256": bindings_sha,
                   "events": [{"sequence": e["position"]["sequence"], "file": "A", "type": e["type"], "cycle": e["cycle"]} for e in self.entries],
                   "batches": [{"sequence": "6", "changes": [{"file": "B", "sequence": "5", "operation": "t"},
                                                            {"file": "B", "sequence": "6", "operation": "c", "id": 9}]}]})

    def position(self, n):
        return {"sequence": str(n), "receiver": "QSQJRN0002", "library": "DCNC9999"}

    def write(self, name, value):
        (self.root / name).write_text(json.dumps(value))

    def export(self):
        return handoff.export_capture(self.root, self.root / "replay.json", 4)

    def test_direct_audited_report_preserves_original_bytes(self):
        bundle, expected = self.export()
        self.assertFalse(bundle["startup_proof"])
        self.assertNotIn("native_identity_evidence", bundle)
        self.assertEqual("4", expected["candidate_W"])
        self.assertEqual("c", expected["prefix"][0]["op"])

    def test_prior_separate_offline_audit_remains_supported(self):
        self.write("offline-audit.json", {"status": "captured-unqualified", "writer_ledger_replay": {"status": "matched"},
                   "replay_sha256": hashlib.sha256((self.root / "replay.json").read_bytes()).hexdigest(),
                   "original_report_sha256": hashlib.sha256((self.root / "report.json").read_bytes()).hexdigest()})
        self.export()
        self.report["status"] = "changed"
        self.write("report.json", self.report)
        with self.assertRaisesRegex(ValueError, "provenance differs"):
            self.export()

    def test_native_observations_and_acquisition_bytes_are_preserved_without_promotion(self):
        self.capture["native_identity_evidence"] = {"profile": "synthetic-unit-evidence", "startup_proof": False}
        self.write("oracle/capture.json", self.capture)
        self.write("native-bindings/capture.json", {"status": "captured", "binding_provenance": {
            "bindings_sha256": hashlib.sha256((self.root / "native-bindings/bindings.json").read_bytes()).hexdigest()}})
        bundle, _ = self.export()
        self.assertEqual(self.capture["native_identity_evidence"], bundle["native_identity_evidence"])
        self.assertFalse(bundle["startup_proof"])
        self.assertEqual((self.root / "native-bindings/bindings.json").read_bytes(), handoff.base64.b64decode(bundle["bindings_raw"]))
        # The Java loader, not this transport exporter, validates native profile semantics.
        self.write("native-bindings/capture.json", {"status": "captured", "binding_provenance": {"bindings_sha256": "wrong"}})
        with self.assertRaisesRegex(ValueError, "Acquired bindings changed"):
            self.export()

    def test_partial_native_evidence_cannot_downgrade_to_fixture_tokens(self):
        self.write("native-bindings/capture.json", {"status": "captured", "binding_provenance": {}})
        with self.assertRaisesRegex(ValueError, "Partial native evidence"):
            self.export()
        self.write("native-bindings/capture.json", {"status": "captured"})
        self.capture["native_identity_evidence"] = {}
        self.write("oracle/capture.json", self.capture)
        with self.assertRaisesRegex(ValueError, "Partial native evidence"):
            self.export()

    def test_partial_fault_requires_output_after_the_first_truncate(self):
        original = json.loads((self.root / "replay.json").read_text())
        for preceding_row in [False, True]:
            with self.subTest(preceding_row=preceding_row):
                replay = json.loads(json.dumps(original))
                changes = replay["batches"][0]["changes"]
                replay["batches"][0]["changes"] = [changes[1], changes[0]] if preceding_row else [changes[0]]
                self.write("replay.json", replay)
                with self.assertRaisesRegex(ValueError, "First truncate must have following selected output"):
                    self.export()

    def test_unqualified_audits_never_export(self):
        for fault in ["overlap", "audit", "progress", "ledger", "cleanup"]:
            with self.subTest(fault=fault):
                changed = json.loads(json.dumps(self.report))
                if fault == "overlap":
                    changed["enumeration_overlap_covered"] = False
                elif fault == "audit":
                    changed["attempts"][0]["audit"]["status"] = "rejected"
                elif fault == "progress":
                    changed["attempts"][0]["unrelated_writes_during_observation"] = 0
                elif fault == "ledger":
                    changed["writer_ledger_replay"]["status"] = "rejected"
                else:
                    changed["fixture_cleanup"]["library_absent"] = False
                self.write("report.json", changed)
                with self.assertRaises(ValueError):
                    self.export()


class ReceiverInventoryExportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        fixture = json.loads((handoff.ROOT / "debezium-connector-ibmi/src/test/resources/native-transactions/receiver-control-history.json").read_text())
        case = next(c for c in fixture["cases"] if c["name"] == "silent-in-detached-receiver")
        self.library = case["lower"]["library"]
        self.cut = int(case["native_identity_evidence"]["through"]["sequence"]) + 1
        self.write("lower.json", {"from": case["lower"]})
        self.write("upper.json", {"through": case["native_identity_evidence"]["through"]})
        self.write("observation.json", case["receiver_observation"])
        bracket = [{"position": position, "type": "R/PT", "cycle": "0"}
                   for position in [case["lower"], case["native_identity_evidence"]["through"]]]
        self.write("bracket.json", {"status": "captured", "entries": bracket})
        self.write("controls.json", {"status": "controls-captured", "mode": "controls", "entries": []})
        entries = []
        for index, sha in enumerate(case["entries"]):
            entry = fixture["entries"][sha]
            name = f"entry-{index}.bin"
            (self.root / name).write_bytes(base64.b64decode(entry["base64"]))
            entries.append({k: entry[k] for k in ["position", "type", "cycle"]} | {"file": name, "sha256": sha})
        self.write("history.json", {"status": "controls-captured", "mode": "controls", "entries": entries,
                   "from": case["native_identity_evidence"]["from"], "through": case["native_identity_evidence"]["through"],
                   "native_identity_evidence": case["native_identity_evidence"]})
        for name in ["directory_before", "directory_after"]:
            self.write(name + ".json", {"directory_evidence": case[name]})
        self.candidate = {"status": "receiver-floor-candidate", "startup_proof": False, "visibility_proof": False,
                          "floor": case["native_identity_evidence"]["from"], "pending_receivers": 2, "bracket_cycles": []}
        self.report = {"attempts": [{"scenario": case["name"], "candidate": self.candidate, "lower": "lower.json", "upper": "upper.json",
                                    "bracket": "bracket.json", "controls": "controls.json", "observation": "observation.json", "history": "history.json",
                                    "directory_before": "directory_before.json", "directory_after": "directory_after.json", "unrelated_writes_during_observation": 1}],
                       "floor_checks": [{"scenario": case["name"], "all_original_starts_retained": True}]}

    def write(self, name, value):
        (self.root / name).write_text(json.dumps(value))

    def export(self):
        return handoff.receiver_inventory(self.root, self.report, self.library, self.cut)

    def test_native_discovery_bytes_and_pending_observation_survive_transport(self):
        lower, upper, inventory, discovery = self.export()
        self.assertEqual(["100"], inventory)
        self.assertFalse(discovery["startup_proof"])
        self.assertEqual(lower, discovery["lower"])
        self.assertLess(int(upper["sequence"]), self.cut)
        for entry in discovery["entries"]:
            self.assertEqual((self.root / entry["file"]).read_bytes(), base64.b64decode(entry["raw"]))

    def test_changed_floor_and_damaged_capture_do_not_export(self):
        self.candidate["pending_receivers"] = 0
        with self.assertRaisesRegex(ValueError, "Receiver floor differs"):
            self.export()
        self.candidate["pending_receivers"] = 2
        (self.root / "entry-0.bin").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "Changed or excessive"):
            self.export()

    def test_wrong_selection_or_incomplete_observation_do_not_export(self):
        value = json.loads((self.root / "controls.json").read_text())
        value["status"] = "captured"
        self.write("controls.json", value)
        with self.assertRaisesRegex(ValueError, "Separate complete row and C/J"):
            self.export()
        value["status"] = "controls-captured"
        self.write("controls.json", value)
        self.report["floor_checks"] = []
        with self.assertRaisesRegex(ValueError, "Incomplete original-start"):
            self.export()


if __name__ == "__main__":
    unittest.main()
