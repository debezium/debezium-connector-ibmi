#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Captured IBM i history + actual local Connect handoff. Never connects to IBM i."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import struct
import sys
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("barrier", ROOT / "tools/dcg-legacy-boundary-probe.py")
barrier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(barrier)
base = barrier.base
CLASS = "io.debezium.connector.db2as400.As400OnlineHandoffProbeConnector"
SCENARIOS = ["beforebarrier", "barriercallback", "migrationcallback", "partialbatch", "reassign"]


def prefix_through(prefix, cut):
    """A delete and its immediately following tombstone share the same source cut."""
    result, previous = [], None
    for item in prefix:
        if item["op"] == "tombstone":
            if previous is None or previous["op"] != "d" or previous["id"] != item["id"]:
                raise ValueError("Unpaired tombstone in expected legacy prefix")
            sequence = int(previous["sequence"])
        else:
            sequence = int(item["sequence"])
        if sequence <= cut:
            result.append(item)
        previous = item
    return result


def receiver_inventory(source, report, library, cut):
    """Export original C/J evidence; the Java loader reconstructs and cross-checks it.

    This alternate fixture inventory does not authorize native startup. In
    particular, a pending-receiver candidate cannot certify historical schemas.
    """
    spec = importlib.util.spec_from_file_location("census", ROOT / "tools/dcg-online-census-probe.py")
    census = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(census)
    attempts = report["attempts"]
    if not attempts or any(a["unrelated_writes_during_observation"] <= 0 for a in attempts):
        raise ValueError("Receiver experiment lacks unrelated progress")
    accepted = [a for a in attempts if a["candidate"]["status"] == "receiver-floor-candidate"]
    checks = report.get("floor_checks", [])
    if {a["scenario"] for a in accepted} != {c["scenario"] for c in checks} or not all(c["all_original_starts_retained"] for c in checks):
        raise ValueError("Incomplete original-start back-read validation")
    attempt = next(a for a in accepted if a["scenario"] == "silent-in-detached-receiver")

    def read(name):
        path = (source / name).resolve()
        if not path.is_relative_to(source.resolve()) or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("Invalid bounded discovery path")
        return json.loads(path.read_text())

    lower, upper = read(attempt["lower"])["from"], read(attempt["upper"])["through"]
    if int(upper["sequence"]) > cut:
        raise ValueError("Receiver observation exceeds publication cut")
    bracket, controls, history = read(attempt["bracket"]), read(attempt["controls"]), read(attempt["history"])
    if bracket["status"] != "captured" or any(c["status"] != "controls-captured" or c["mode"] != "controls" for c in [controls, history]):
        raise ValueError("Separate complete row and C/J captures required")
    union = {int(e["position"]["sequence"]): e for e in bracket["entries"]}
    for entry in controls["entries"]:
        sequence = int(entry["position"]["sequence"])
        if sequence in union and any(entry[k] != union[sequence][k] for k in ["position", "type", "cycle"]):
            raise ValueError("Control bracket differs from complete row bracket")
        union[sequence] = entry
    observation = read(attempt["observation"])
    candidate = census.receiver_history_floor(observation, [union[k] for k in sorted(union)], library)
    if candidate != attempt["candidate"] or history["from"] != candidate["floor"] or history["through"] != upper:
        raise ValueError("Receiver floor differs from retained observation")
    inventory, copied, size = set(), [], 0
    for entry in history["entries"]:
        if not re.fullmatch(r"entry-[0-9]+\.bin", entry["file"]):
            raise ValueError("Unexpected discovery entry path")
        path = source / Path(attempt["history"]).parent / entry["file"]
        raw = path.read_bytes()
        size += len(raw)
        if size > 64 * 1024 * 1024 or len(copied) >= 20000 or hashlib.sha256(raw).hexdigest() != entry["sha256"]:
            raise ValueError("Changed or excessive discovery bytes")
        copied.append({**entry, "raw": base64.b64encode(raw).decode()})
        if int(entry["position"]["sequence"]) < int(lower["sequence"]):
            if entry["type"] == "C/SC":
                inventory.add(entry["cycle"])
            elif entry["type"] in ["C/CM", "C/RB"]:
                inventory.discard(entry["cycle"])
    discovery = {"profile": "receiver-history-candidate-v1", "startup_proof": False, "lower": lower,
                 "native_identity_evidence": history["native_identity_evidence"], "entries": copied,
                 "directory_before": read(attempt["directory_before"])["directory_evidence"],
                 "directory_after": read(attempt["directory_after"])["directory_evidence"], "observation": observation}
    return lower, upper, sorted(inventory, key=int), discovery


def export_capture(source, replay_file, cut):
    """Pin the original bytes, acquired bindings and one observed inventory bracket.

    This is immutable fixture provenance, NOT native completeness/incarnation proof.
    The replay's independent SQL-ledger audit must already have matched this capture.
    """
    report = json.loads((source / "report.json").read_text())
    capture = json.loads((source / "oracle/capture.json").read_text())
    replay = json.loads(replay_file.read_text())
    audit_path = replay_file.with_name("offline-audit.json")
    audit = json.loads(audit_path.read_text()) if audit_path.exists() else report
    receiver_mode = report.get("experiment") == "Receiver pending history with independent raw brackets"
    ledger = audit.get("writer_ledger_replay", audit.get("ledger_audit", {}) if receiver_mode else {})
    if audit["status"] != "captured-unqualified" or ledger.get("status") != "matched" or replay["status"] != "replayed":
        raise ValueError("Prior matched writer-ledger audit and strict replay required")
    if audit_path.exists() and (hashlib.sha256(replay_file.read_bytes()).hexdigest() != audit["replay_sha256"] or hashlib.sha256((source / "report.json").read_bytes()).hexdigest() != audit["original_report_sha256"]):
        raise ValueError("Audit provenance differs")
    if not audit_path.exists() and (replay_file.resolve() != (source / "replay.json").resolve() or not receiver_mode and (not report.get("enumeration_overlap_covered") or
            not report["attempts"] or any(a["audit"]["status"] != "capture-consistent" or a["unrelated_writes_during_observation"] <= 0 for a in report["attempts"]))):
        raise ValueError("Direct native report lacks completed census/ledger validation")
    library = capture["library"]
    if not re.fullmatch(r"DCNC[0-9]{4}", library) or capture["status"] != "captured" or not report["fixture_cleanup"]["library_absent"]:
        raise ValueError("Completed, cleaned-up isolated census capture required")
    if receiver_mode and (report["fixture_cleanup"].get("status") != "passed" or report.get("connection_cleanup_errors") != []):
        raise ValueError("Receiver experiment cleanup did not complete")
    discovery = None
    if receiver_mode:
        lower, upper, inventory, discovery = receiver_inventory(source, report, library, cut)
    else:
        attempt = next(a for a in report["attempts"] if a["scenario"] == "silent")
        census = json.loads((source / attempt["census"]).read_text())
        lower = json.loads((source / attempt["lower"]).read_text())["from"]
        upper = json.loads((source / attempt["upper"]).read_text())["through"]
        if census["status"] != "observed":
            raise ValueError("Observed census required")
        inventory = sorted({str(r["COMMIT_CYCLE"]) for r in census["resources"] if r["COMMIT_CYCLE"] is not None}, key=int)
    if int(upper["sequence"]) > cut or not 1 < len(capture["entries"]) <= 20000:
        raise ValueError("Bounded observed census preceding the cut required")
    copied = []
    for item in capture["entries"]:
        if not re.fullmatch(r"entry-[0-9]+\.bin", item["file"]):
            raise ValueError("Unexpected raw path")
        raw = (source / "oracle" / item["file"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("Changed source bytes")
        copied.append({**item, "raw": base64.b64encode(raw).decode()})
    comparisons = [{key: item[key] for key in ["position", "type", "cycle", "sha256"]} for item in copied]
    audited = [{key: item[key] for key in ["position", "type", "cycle", "sha256"]} for item in replay["comparison_captures"]["oracle"]]
    if comparisons != audited or hashlib.sha256((source / "native-bindings/bindings.json").read_bytes()).hexdigest() != replay["bindings_sha256"]:
        raise ValueError("Capture or bindings differ from audited replay")
    if not any(int(x["position"]["sequence"]) == cut for x in copied):
        raise ValueError("Missing exact cut")
    bundle = {"scope": "controlled-capture-assumption-v1", "startup_proof": False,
              "library": library, "entries": copied, "lower": lower, "upper": upper,
              "inventory": inventory,
              "bindings": json.loads((source / "native-bindings/bindings.json").read_text()),
              "provenance": {"source": str(source), "audit_sha256": hashlib.sha256((audit_path if audit_path.exists() else source / "report.json").read_bytes()).hexdigest(),
                             "receiver_observation": capture["receiver_observation"], "receiver_recheck": capture["receiver_recheck"],
                             "native_journal_receiver_incarnations": "not established; fixture-scoped tokens only"}}
    if discovery is not None:
        bundle["control_history"] = discovery
    acquired_path = source / "native-bindings/capture.json"
    acquired = json.loads(acquired_path.read_text()) if acquired_path.exists() else {}
    if ("native_identity_evidence" in capture) != ("binding_provenance" in acquired):
        raise ValueError("Partial native evidence cannot fall back to fixture tokens")
    if "native_identity_evidence" in capture:
        if acquired["status"] != "captured" or "binding_provenance" not in acquired:
            raise ValueError("Missing native acquisition provenance")
        bindings_raw = (source / "native-bindings/bindings.json").read_bytes()
        if acquired["binding_provenance"]["bindings_sha256"] != hashlib.sha256(bindings_raw).hexdigest():
            raise ValueError("Acquired bindings changed")
        bundle.update(native_identity_evidence=capture["native_identity_evidence"], binding_provenance=acquired["binding_provenance"],
                      bindings_raw=base64.b64encode(bindings_raw).decode(), bindings_sha256=hashlib.sha256(bindings_raw).hexdigest())
        bundle["provenance"]["native_journal_receiver_incarnations"] = "actual ordinary native identity observations; not restore protection or startup proof"
    selected = {"A", "B", "M"}  # Existing ordinary capture filter; U still contributes recovery context.
    prefix = []
    for item in copied[1:]:
        seq = int(item["position"]["sequence"])
        if seq > cut:
            break
        raw = base64.b64decode(item["raw"])
        # Use the independently replayed source identity; this fixture has one BIN4 primary key.
        event = next((e for e in replay["events"] if int(e["sequence"]) == seq), None)
        if event is None or event["file"] not in selected or event["type"] not in {"R/PT", "R/PX", "R/DR", "R/DL"}:
            continue
        data = struct.unpack_from(">i", raw, 8)[0] + 16
        row_id = struct.unpack_from(">i", raw, data)[0]
        op = "c" if event["type"] in {"R/PT", "R/PX"} else "d"
        prefix.append({"op": op, "file": event["file"], "sequence": str(seq), "id": row_id})
        if op == "d":
            prefix.append({"op": "tombstone", "id": row_id})
    completions = [int(e["sequence"]) for e in replay["events"] if int(e["sequence"]) <= cut and
                   (e["type"] in {"C/CM", "C/RB"} or e["cycle"] == "0" and e["type"] in {"R/PT", "R/PX", "R/UP", "R/DL", "F/CR"})]
    completed = max(completions)
    expected = list(prefix)
    fault_truncate = None
    for batch in replay["batches"]:
        if int(batch["sequence"]) <= completed:
            continue
        changes = [c for c in batch["changes"] if c["file"] in selected]
        if not changes:
            continue
        for index, change in enumerate(changes):
            item = {k: change[k] for k in ["file", "sequence"]}
            item["op"] = change["operation"]
            if "id" in change:
                item["id"] = change["id"]
            expected.append(item)
            if fault_truncate is None and item["op"] == "t":
                if index + 1 == len(changes):
                    raise ValueError("First truncate must have following selected output for the partial-batch fault")
                fault_truncate = item["sequence"]
    if fault_truncate is None:
        raise ValueError("Fixture must contain a multi-record clear batch after migration")
    return bundle, {"prefix": prefix, "all": expected, "candidate_W": str(completed), "fault_truncate_sequence": fault_truncate}


def export_prospective(source, replay_file, cut):
    """Exact native prospective fixture only; native origin is still unqualified.

    Independent expected output comes from the controlled SQL ledger, not from
    whatever output the connector happens to produce. Original matrix unchanged.
    """
    if source.stat().st_size > 16 * 1024 * 1024 or replay_file.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("Excessive prospective fixture")
    bundle, replay = json.loads(source.read_text()), json.loads(replay_file.read_text())
    if (bundle.get("profile") not in ("prospective-native-fixture-v1", "prospective-native-boundary-v1") or bundle.get("startup_proof") is not False or
            replay.get("status") != "verified-offline" or replay.get("startup_proof") is not False or cut != 66 or
            bundle["origin"]["sequence"] != 59 or bundle["cut"]["sequence"] != cut or
            replay["recovery"]["reference_records"] != 3 or not replay["recovery"]["cases"] or
            any(c["status"] != "passed" for c in replay["recovery"]["cases"])):
        raise ValueError("Exact verified prospective fixture required")
    library = bundle["prospective-rows"]["library"]
    if not re.fullmatch(r"DCNC[0-9]{4}", library):
        raise ValueError("Isolated prospective source required")
    bundle.update(library=library, entries=bundle["prospective-rows"]["entries"],
                  native_identity_evidence=bundle["prospective-rows"]["native_identity_evidence"])
    prefix = [{"op": "c", "file": "M", "sequence": "64", "id": 21},
              {"op": "c", "file": "A", "sequence": "66", "id": 12}]
    return bundle, {"prefix": prefix, "all": prefix + [
        {"op": "c", "file": "A", "sequence": "78", "id": 14},
        {"op": "t", "file": "B", "sequence": "75"},
        {"op": "c", "file": "B", "sequence": "77", "id": 101}],
        "candidate_W": "66", "fault_truncate_sequence": "75", "prospective": True}


class HandoffExperiment(barrier.BarrierExperiment):
    def __init__(self, directory, source, replay, cut):
        self.bundle, self.expected = (export_prospective if source.is_file() else export_capture)(source, replay, cut)
        self.standard_history = os.environ.get("DCG_HANDOFF_STANDARD_HISTORY", "false") == "true"
        self.normal_recovery = os.environ.get("DCG_HANDOFF_NORMAL_RECOVERY", "false") == "true"
        if self.normal_recovery and not self.standard_history:
            raise ValueError("Normal recovery requires standard binding history")
        if self.standard_history and self.bundle.get("profile") != "prospective-native-boundary-v1":
            raise ValueError("Standard-history experiment requires checked native boundary evidence")
        self.cut = cut
        self.expected_replays = {}
        super().__init__(directory)

    def stage(self):
        super().stage()
        jar = self.plugin / "synthetic-probe.jar"
        temporary = jar.with_suffix(".tmp")
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(jar) as source, zipfile.ZipFile(temporary, "w") as target:
            for item in source.infolist():
                target.writestr(item, source.read(item.filename))
            for name in ["As400OnlineHandoff", "As400OnlineHandoffProbeConnector", "As400OnlineCensusReplay", "As400OnlineCensusRecovery", "As400JournalCaptureContent", "As400OnlineNativeEvidence", "As400OnlineControlInventory",
                         "As400ProspectiveReplay", "As400ProspectiveHandoff", "As400OnlineFenceEvidence", "As400OnlineLockScope", "As400ShortBoundaryEvidence", "As400HandoffBindingHistory", "As400NativeHistoryDiscoveryProbe"]:
                for path in classes.rglob(name + "*.class"):
                    if path.stem == name or path.name.startswith(name + "$"):
                        target.write(path, str(path.relative_to(classes)))
        temporary.replace(jar)
        raw = json.dumps(self.bundle, sort_keys=True, separators=(",", ":")).encode()
        (self.plugin / "online-handoff.json").write_bytes(raw)
        self.bundle_hash = hashlib.sha256(raw).hexdigest()
        (self.directory / "expected.json").write_text(json.dumps(self.expected, indent=2))
        self.report.update({"scope": "Captured native bytes and acquired bindings through actual legacy RPC loop/handler, task, buffered dispatcher and local Connect",
                            "source_connected": False, "startup_proof": False, "migration_enabled": False,
                            "native_visibility": "controlled fixture assumption; not a platform guarantee", "source_transport": "controlled RetrieveJournal responses; no JT400 sign-on/pagination",
                            "history": "read-only digest-pinned captured bundle, reloaded on every task; not native startup or Kafka binding-history initialization",
                            "identity": "original file/member JIDs, producer metadata and receiver coordinates; capture-scoped stand-ins for uncollected journal/receiver incarnations",
                            "custom_publication_authority": False, "delivery_contract": "Connect EOS whole publication stages/batches; read_committed consumers",
                            "transaction_boundary": "connector", "initialization": "Only ordinary legacy seed installed by controller; task publishes barrier then F/W via Connect",
                            "bundle_sha256": self.bundle_hash, "cut": self.cut, "capture_filter": self.bundle["library"] + r"\.(A|B|M)"})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        if "native_identity_evidence" in self.bundle:
            self.report["identity"] = "original source metadata plus actual ordinary native journal/receiver observations; no synthetic incarnation tokens"
            self.report["historical_binding_proof"] = False
        if "control_history" in self.bundle:
            self.report["inventory"] = "native receiver-pending candidate and original C/J history, reconstructed at task load; still test-only admission"
        if self.expected.get("prospective"):
            self.report["inventory"] = "native control-only history before schema origin; busy attempt rejected, successful retry had no remaining prior-schema cycles"
            self.report["origin"] = "native candidate; complete maintenance visibility and exact start qualification remain unproved"
            self.report["receiver_coverage"] = "single receiver; original rotation matrix remains separate"
        if self.bundle.get("profile") == "prospective-native-boundary-v1":
            self.report["origin"] = "checked native short-pulse scope, drain, raw A/N/Z and complete audit; fixed-schema/retention contract, still test-only admission"
        if self.standard_history:
            self.report["history"] = "existing As400BindingHistory/As400KafkaBindingHistory, pending bindings persisted before legacy barrier; actual barrier acknowledgement required before F/W"
        self.report["standard_binding_history"] = self.standard_history
        self.report["normal_task_recovery"] = self.normal_recovery
        if self.normal_recovery:
            self.report["delivery_contract"] = "Connect EOS per poll after acknowledged handoff; source at-least-once replay from durable F/W; read_committed consumers"
            self.report["recovery_contract"] = "Actual normal task legacy transition and restart/runtime/acknowledgements/stop; captured transport. Per-poll Connect transactions; incomplete source batch may replay."

        self.report["harness_sha256"]["tools/dcg-online-handoff-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        shutil.copyfile(__file__, self.directory / "harness.py")
        self.save()

    def create_handoff(self, worker, scenario):
        library = self.bundle["library"]
        config = {"connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario,
                  "probe.bundle.sha256": self.bundle_hash, "probe.cut": str(self.cut), "probe.fixture.visibility.assumed": "true", "topic.prefix": "test",
                  "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture", "database.password": "fixture", "database.dbname": "DB",
                  "database.schema": library, "table.include.list": library + r"\.(A|B|M)", "skipped.operations": "none",
                  "max.batch.size": "1", "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
                  "provide.transaction.metadata": "false", "event.converting.failure.handling.mode": "fail", "errors.tolerance": "none",
                  "exactly.once.support": "requested", "transaction.boundary": "connector"}
        if self.expected.get("prospective"):
            config["probe.prospective"] = "true"
        if self.standard_history:
            config["probe.standard.history"] = "true"
        if self.normal_recovery:
            config["probe.normal.recovery"] = "true"
            config["topic.naming.strategy"] = CLASS + "$Topics"
            config["producer.override.interceptor.classes"] = CLASS + "$HoldTruncateOffset"
        if scenario == "handoffdeferred":
            config["probe.defer.cuts"] = ",".join(map(str, self.deferral_cuts))
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        for suffix in ["", "-control"]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create",
                         "--topic", "events-" + scenario + suffix, "--partitions", "1", "--replication-factor", "1")
        self.rest(worker, "/connectors", {"name": scenario, "initial_state": "STOPPED", "config": config})
        def created_and_stopped():
            try:
                return self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED"
            except base.RestError as error:
                if error.status != 404:
                    raise
                return False  # POST succeeded; status-topic propagation may lag creation.
        base.wait_for("stopped before legacy seed", created_and_stopped)
        anchor = self.bundle["entries"][0]["position"]
        initial = {"offset.event_sequence": anchor["sequence"], "offset.time": "0", "offset.receiver": anchor["receiver"],
                   "offset.receiver_library": library, "offset.processed": "true", "snapshot_completed": "true"}
        self.patch_offset(worker, scenario, initial)
        self.check(scenario + ": only ordinary legacy offset seeded", self.checkpoint(self.observer_container, scenario, "seed") == initial,
                   "control/" + scenario + "-seed-offsets-read_committed.json")
        self.request(worker, scenario, "PUT", "/resume")
        generation = self.generation(scenario, "w1")
        self.marker(generation, "before-legacy")
        return initial, generation

    @staticmethod
    def normalized(rows):
        result = []
        for row in rows:
            if row["value"] is None:
                result.append({"op": "tombstone", "id": row["key"]["ID"]})
                continue
            value = row["value"]
            # IBM i's existing source envelope has receiver/sequence, not a table
            # field. This single-topic experiment verifies source coordinates and
            # row identity; ordinary per-table routing is outside its scope.
            item = {"op": value["op"], "sequence": value["source"]["sequence"]}
            if value["op"] != "t":
                item["id"] = value["after" if value["after"] else "before"]["ID"]
                if row["key"] != {"ID": item["id"]}:
                    raise AssertionError("Lost source row identity")
            elif row["key"] is not None:
                raise AssertionError("Truncate key must be null")
            result.append(item)
        return result

    def permit(self, generation, count):
        self.signal(generation, "permit", count)
        base.wait_for("polled requested fault boundary", lambda: sum(e["event"] == "poll" for e in self.events(generation)) >= count)

    def complete(self, scenario, generation):
        for stage in ["legacy", "migration", "history", "activation", "stream"]:
            self.signal(generation, "release-" + stage)
        self.signal(generation, "permit", 10000)
        self.marker(generation, "stream-enqueued")
        base.wait_for("final checkpoint acknowledgement", lambda: any(e["event"] == "commitRecord" and e["fields"]["kind"] == "final" for e in self.events(generation)))
        data = self.inspect(scenario, "complete")
        controls = self.records(self.observer_container, "events-" + scenario + "-control", scenario + "-complete-control")
        offset = self.checkpoint(self.observer_container, scenario, "complete")
        expected = [{k: v for k, v in row.items() if k != "file"} for row in self.expected["all"]]
        if self.normal_recovery:
            prefix = len(self.expected["prefix"])
            expected[prefix:prefix] = self.expected_replays.get(scenario, [])
            label = ": exact normal-task replay, only the proven uncheckpointed row may repeat; no snapshot/loss/partial clear"
        else:
            label = ": exact complete legacy prefix plus buffered suffix, no snapshot/loss/duplicate/partial clear"
        self.check(scenario + label, self.normalized(data) == expected
                   and not any(r["value"] and r["value"]["op"] == "r" for r in data), "control/" + scenario + "-complete.json")
        self.check(scenario + ": one durable barrier then one F/W transition", [r["value"] for r in controls if r["value"] in ["barrier", "migration"]] == ["barrier", "migration"]
                   and offset["offset.version"] == "2" and "scan.sequence" not in offset, "control/" + scenario + "-complete-control.json")
        coordinates = {e["position"]["sequence"]: e["position"] for e in self.bundle["entries"]}
        self.check(scenario + ": original source coordinates and expected receiver coverage survive", all(r["value"] is None or
                   (r["value"]["source"]["receiver"], r["value"]["source"]["receiver_library"]) ==
                   (coordinates[r["value"]["source"]["sequence"]]["receiver"], coordinates[r["value"]["source"]["sequence"]]["library"]) for r in data)
                   and (len({r["value"]["source"]["receiver"] for r in data if r["value"]}) == 1 if self.expected.get("prospective")
                        else len({r["value"]["source"]["receiver"] for r in data if r["value"]}) > 1), "control/" + scenario + "-complete.json")
        if self.standard_history:
            history = self.records(self.observer_container, "bindings-" + scenario, scenario + "-bindings-complete")
            self.check(scenario + ": native bindings and required activation recovered from durable ordinary history", len(history) == (2 if self.normal_recovery else 1)
                       and history[0]["value"]["position"]["fingerprint"] == offset["capture_fingerprint"]
                       and history[0]["value"]["position"]["sequence"] == str(self.bundle["origin"]["sequence"])
                       and ((self.control / (generation + ".history-recovered.json")).exists()
                            or (self.control / (generation + ".history-durable.json")).exists()),
                       "control/" + scenario + "-bindings-complete.json")
        if self.normal_recovery:
            self.check(scenario + ": activation is separate from pending bindings and snapshots",
                       history[1]["value"]["position"]["kind"] == "legacy-handoff"
                       and history[1]["value"]["position"]["checkpoint.capture_fingerprint"] == offset["capture_fingerprint"]
                       and "initialization.epoch" not in history[0]["value"]["position"],
                       "control/" + scenario + "-bindings-complete.json")
        return data, offset

    def exercise_case(self, fault):
        scenario = "handoff" + fault
        first = self.start_worker(scenario, "w1")
        initial, generation = self.create_handoff(first, scenario)
        prefix_count = len(self.expected["prefix"])
        if fault in ["barriercallback", "migrationcallback", "barrierhistoryloss"]:
            stage = "barrier" if fault == "barrierhistoryloss" else fault.removesuffix("callback")
            self.signal(generation, "hold-" + stage + "-callback")
        if fault == "historycallback":
            self.signal(generation, "hold-history")
        if fault == "activationcallback":
            self.signal(generation, "hold-activation")
        self.signal(generation, "release-legacy")
        if fault == "beforebarrier":
            # The native prefix can exceed the bounded output queue. Admit only
            # the two fault records without requiring the producer to enqueue
            # its distant barrier first. The source may still be backpressured
            # when killed; none of this prefix may commit.
            self.permit(generation, 2)
            raw = self.snapshot(self.observer_container, scenario, "partial", isolation="read_uncommitted")
            saved = self.checkpoint(self.observer_container, scenario, "fault")
            self.check(scenario + ": partial legacy records reached broker without committed output or offset", len(raw) == 2 and
                       not self.inspect(scenario, "fault") and saved == initial, "control/" + scenario + "-partial-records-read_uncommitted.json")
        elif fault in ["barriercallback", "barrierhistoryloss"]:
            self.permit(generation, prefix_count + 1)
            self.marker(generation, "barrier-enqueued")
            self.marker(generation, "barrier-callback-held")
            saved = self.checkpoint(self.observer_container, scenario, "fault")
            self.check(scenario + ": durable native cut exists before callback and before F/W publication", saved.get(barrier.BARRIER) == "native-capture-output-v1"
                       and saved["offset.event_sequence"] == str(self.cut) and "completed_through.sequence" not in saved
                       and not (self.control / (generation + ".migration-enqueued.json")).exists(), "control/" + scenario + "-fault-offsets-read_committed.json")
        elif fault == "activationcallback":
            self.permit(generation, prefix_count + 1)
            self.marker(generation, "activation-durable")
            saved = self.checkpoint(self.observer_container, scenario, "fault")
            history = self.records(self.observer_container, "bindings-" + scenario, scenario + "-activation-before-offset")
            self.check(scenario + ": activation survives crash before F/W without replacing the durable legacy barrier",
                       len(history) == 2 and saved.get("probe.legacy.barrier") == "native-capture-output-v1"
                       and "offset.version" not in saved and not (self.control / (generation + ".migration-enqueued.json")).exists(),
                       "control/" + scenario + "-activation-before-offset.json")
        elif fault == "historycallback":
            self.signal(generation, "release-migration")
            self.permit(generation, prefix_count)
            self.marker(generation, "history-durable")
            saved = self.checkpoint(self.observer_container, scenario, "fault")
            history = self.records(self.observer_container, "bindings-" + scenario, scenario + "-bindings-before-offset")
            self.check(scenario + ": pending history survives crash before barrier without creating committed output or F/W",
                       len(history) == 1 and saved == initial and not self.inspect(scenario, "history-before-barrier")
                       and not (self.control / (generation + ".barrier-enqueued.json")).exists()
                       and not (self.control / (generation + ".migration-enqueued.json")).exists(),
                       "control/" + scenario + "-bindings-before-offset.json")
        else:
            self.signal(generation, "release-migration")
            self.permit(generation, prefix_count + 2)
            self.marker(generation, "barrier-enqueued")
            if fault == "migrationcallback":
                self.marker(generation, "migration-callback-held")
                saved = self.checkpoint(self.observer_container, scenario, "fault")
                self.check(scenario + ": durable F/W survives lost migration callback with expected relation to R", saved.get("completed_through.sequence") == self.expected["candidate_W"]
                           and ((saved["completed_through.sequence"] == str(self.cut) and saved["replay_from.sequence"] == saved["completed_through.sequence"])
                                if self.expected.get("prospective") else (saved["completed_through.sequence"] != str(self.cut)
                                and saved["replay_from.sequence"] != saved["completed_through.sequence"])),
                           "control/" + scenario + "-fault-offsets-read_committed.json")
            else:
                self.signal(generation, "hold-first-truncate")
                self.signal(generation, "release-stream")
                self.signal(generation, "permit", 10000)
                self.marker(generation, "truncate-polled")
                def truncate_reached_broker():
                    observed = self.snapshot(self.observer_container, scenario, "partial", isolation="read_uncommitted")
                    return bool(observed) and observed[-1]["value"] is not None and observed[-1]["value"]["op"] == "t"
                base.wait_for("held truncate reaches broker", truncate_reached_broker)
                raw = self.snapshot(self.observer_container, scenario, "partial", isolation="read_uncommitted")
                committed = self.inspect(scenario, "fault")
                saved = self.checkpoint(self.observer_container, scenario, "fault")
                if self.normal_recovery:
                    # Normal source records deliberately carry the previous W.
                    # In this exact ledger the unrelated A14 was already committed
                    # before the held truncate. It must repeat only when W still
                    # precedes A14; never permit arbitrary duplicate/missing rows.
                    prefix = [{k: v for k, v in row.items() if k != "file"} for row in self.expected["prefix"]]
                    row = {k: v for k, v in self.expected["all"][len(prefix)].items() if k != "file"}
                    self.check(scenario + ": normal fault is before offset enqueue with unrelated row already committed",
                               self.normalized(committed) == prefix + [row]
                               and row["op"] == "c" and row["id"] == 14
                               and (self.control / (generation + ".truncate-offset-held.json")).exists(),
                               "control/" + scenario + "-fault.json")
                    self.expected_replays[scenario] = [row] if int(saved["completed_through.sequence"]) < int(row["sequence"]) else []
                self.check(scenario + ": truncate prefix reached broker but remains uncommitted", len(raw) > len(committed)
                           and raw[-1]["value"]["op"] == "t" and not any(r["value"] and r["value"]["op"] == "t" for r in committed)
                           and raw[-1]["value"]["source"]["sequence"] == self.expected["fault_truncate_sequence"]
                           and saved.get("offset.version") == "2", "control/" + scenario + "-partial-records-read_uncommitted.json")
        self.command("docker", "kill", *(["--signal", "STOP"] if fault == "reassign" else []), first)
        if fault == "barrierhistoryloss":
            before_loss = self.inspect(scenario, "history-loss-before-restart")
            self.delete_owned_history(scenario)
        second = self.start_worker(scenario, "w2")
        replacement = self.generation(scenario, "w2")
        self.check(scenario + ": replacement loads actual durable offset without controller rewriting it",
                   json.loads((self.control / (replacement + ".start.json")).read_text())["loaded"] == saved,
                   "control/" + replacement + ".start.json")
        if fault == "barrierhistoryloss":
            base.wait_for("acknowledged barrier missing-history rejection", lambda: any(
                t["state"] == "FAILED" for t in self.rest(second, "/connectors/" + scenario + "/status")["tasks"]))
            self.check(scenario + ": durable barrier cannot recreate missing bindings from fixture metadata",
                       self.inspect(scenario, "history-loss-rejected") == before_loss
                       and self.checkpoint(self.observer_container, scenario, "history-loss-rejected") == saved,
                       "control/" + scenario + "-history-loss-rejected.json")
            self.stop_workers(second)
            return
        data, final = self.complete(scenario, replacement)
        if self.normal_recovery and saved.get("offset.version") == "2":
            self.check(scenario + ": replacement uses actual normal task startup and recovered history",
                       (self.control / (replacement + ".normal-task.json")).exists()
                       and json.loads((self.control / (replacement + ".history-recovered.json")).read_text()).get("normal_task") is True,
                       "control/" + replacement + ".normal-task.json")
        if fault == "reassign":
            self.command("docker", "kill", "--signal", "CONT", first)
            self.signal(generation, "release-truncate")
            self.signal(generation, "permit", 10000)
            base.wait_for("old worker stops or is fenced", lambda: (self.control / (generation + ".stopped.json")).exists() or
                          any(x in self.command("docker", "logs", first) for x in ["ProducerFencedException", "InvalidProducerEpochException"]), 90)
            self.check(scenario + ": returning old worker cannot change committed output or F/W", self.inspect(scenario, "old-return") == data
                       and self.checkpoint(self.observer_container, scenario, "old-return") == final, "control/" + scenario + "-old-return.json")
            self.stop_workers(first)
        if fault == "migrationcallback":
            self.request(second, scenario, "PUT", "/stop")
            base.wait_for("stopped before offset loss", lambda: self.rest(second, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
            self.patch_offset(second, scenario, None)
            self.request(second, scenario, "PUT", "/resume")
            base.wait_for("missing offset rejection", lambda: any(t["state"] == "FAILED" for t in self.rest(second, "/connectors/" + scenario + "/status")["tasks"]))
            self.check(scenario + ": missing offset rejects without output or replacement checkpoint", self.inspect(scenario, "missing") == data
                       and self.checkpoint(self.observer_container, scenario, "missing") is None, "control/" + scenario + "-missing.json")
        if self.standard_history and fault == "partialbatch":
            self.request(second, scenario, "PUT", "/stop")
            base.wait_for("stopped before owned history loss", lambda: self.rest(second, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
            self.delete_owned_history(scenario)
            self.request(second, scenario, "PUT", "/resume")
            base.wait_for("missing history rejection", lambda: any(t["state"] == "FAILED" for t in self.rest(second, "/connectors/" + scenario + "/status")["tasks"]))
            self.check(scenario + ": missing history cannot fall back to captured bindings or change output/F/W",
                       self.inspect(scenario, "missing-history") == data and self.checkpoint(self.observer_container, scenario, "missing-history") == final,
                       "control/" + scenario + "-missing-history.json")
        self.stop_workers(second)

    def delete_owned_history(self, scenario):
        if not self.standard_history or not re.fullmatch(r"handoff[a-z]+", scenario):
            raise ValueError("Exact owned local history topic required")
        self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                     "--delete", "--topic", "bindings-" + scenario)
        base.wait_for("owned history topic deleted", lambda: "bindings-" + scenario not in self.command(
            "docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list").splitlines())

    def exercise(self):
        self.observer()
        selected = os.environ.get("DCG_HANDOFF_SCENARIOS", ",".join(SCENARIOS)).split(",")
        allowed = SCENARIOS + ["deferred"] + (["historycallback", "barrierhistoryloss"] if self.standard_history else [])
        if self.normal_recovery:
            allowed += ["activationcallback"]
        if not selected or len(set(selected)) != len(selected) or any(s not in allowed for s in selected):
            raise ValueError("Unknown or duplicate handoff scenario")
        self.report["selected_scenarios"] = selected
        self.save()
        for scenario in selected:
            if scenario == "deferred":
                self.exercise_deferred()
            else:
                self.exercise_case(scenario)
        self.check("no custom ownership or receipt gates", not list(self.control.glob("*.registry*")) and not list(self.control.glob("*.authorized-worker*")), "control/")

    def exercise_deferred(self):
        """Two exact legacy progress barriers while the same selected clear stays open.

        The controller supplies fault timing only. The task independently validates
        each temporary refusal and publishes progress through Connect; no F/W is
        installed or repaired by the controller.
        """
        self.deferral_cuts = [int(n) for n in os.environ.get("DCG_HANDOFF_DEFER_CUTS", "").split(",")]
        if (len(self.deferral_cuts) != 2 or self.deferral_cuts != sorted(set(self.deferral_cuts))
                or any(n >= self.cut or str(n) not in {e["position"]["sequence"] for e in self.bundle["entries"]} for n in self.deferral_cuts)):
            raise ValueError("Two ordered native deferral anchors preceding the migration cut required")
        self.report["deferral_cuts"] = self.deferral_cuts
        self.save()
        scenario = "handoffdeferred"
        first = self.start_worker(scenario, "w1")
        initial, generation = self.create_handoff(first, scenario)
        self.signal(generation, "permit", 10000)
        self.signal(generation, "release-legacy")
        prior = []
        for cut in self.deferral_cuts:
            self.marker(generation, "progress-" + str(cut))
            if cut == self.deferral_cuts[-1]:
                self.marker(generation, "legacy-progress-callback-held")
            else:
                base.wait_for("durable legacy progress", lambda: any(e["event"] == "commitRecord" and
                              e["fields"]["kind"] == "legacy-progress" and e["fields"]["offset"]["offset.event_sequence"] == str(cut)
                              for e in self.events(generation)))
            data = self.inspect(scenario, "progress-" + str(cut))
            saved = self.checkpoint(self.observer_container, scenario, "progress-" + str(cut))
            expected = prefix_through(self.expected["prefix"], cut)
            coordinates = {e["position"]["sequence"]: e for e in self.bundle["entries"]}
            newly_visible = data[len(prior):]
            self.check(scenario + ": unrelated output commits while migration is deferred at " + str(cut),
                       self.normalized(data) == [{k: v for k, v in r.items() if k != "file"} for r in expected]
                       and any(r["value"] and coordinates[r["value"]["source"]["sequence"]]["table"] == "A" for r in newly_visible)
                       and saved.get(barrier.BARRIER) == "native-capture-progress-v1"
                       and saved["offset.event_sequence"] == str(cut) and "offset.version" not in saved
                       and not (self.control / (generation + ".migration-enqueued.json")).exists(), "control/" + scenario + "-progress-" + str(cut) + ".json")
            prior = data
            if cut != self.deferral_cuts[-1]:
                self.signal(generation, "hold-legacy-progress-callback")
                self.signal(generation, "release-progress-" + str(cut))
        self.check(scenario + ": second progress is durable before its callback returns",
                   not any(e["event"] == "commitRecord" and e["fields"]["kind"] == "legacy-progress"
                           and e["fields"]["offset"]["offset.event_sequence"] == str(self.deferral_cuts[-1]) for e in self.events(generation)),
                   "control/" + generation + ".legacy-progress-callback-held.json")
        self.command("docker", "kill", first)
        second = self.start_worker(scenario, "w2")
        replacement = self.generation(scenario, "w2")
        self.check(scenario + ": replacement resumes durable legacy progress without F/W or controller edits",
                   json.loads((self.control / (replacement + ".start.json")).read_text())["loaded"] == saved,
                   "control/" + replacement + ".start.json")
        data, final = self.complete(scenario, replacement)
        controls = self.records(self.observer_container, "events-" + scenario + "-control", scenario + "-progress-control")
        self.check(scenario + ": two progress commits precede the single migration and are not replayed",
                   [r["value"] for r in controls if r["value"] in ["legacy-progress", "barrier", "migration"]]
                   == ["legacy-progress", "legacy-progress", "barrier", "migration"], "control/" + scenario + "-progress-control.json")
        self.stop_workers(second)


def main():
    if len(sys.argv) != 5:
        raise SystemExit("Usage: dcg-online-handoff-probe.py NEW_EVIDENCE_DIRECTORY CAPTURE_DIRECTORY AUDITED_REPLAY_JSON CUT_SEQUENCE")
    experiment = HandoffExperiment(Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), int(sys.argv[4]))
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.exercise()
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report.update(status="failed", error=str(error))
        if hasattr(experiment, "observer_container"):
            for config in experiment.directory.glob("*-connector.json"):
                scenario = config.name.removesuffix("-connector.json")
                try:
                    experiment.inspect(scenario, "failure")
                    experiment.checkpoint(experiment.observer_container, scenario, "failure")
                except Exception as evidence_error:
                    experiment.report.setdefault("evidence_errors", []).append(str(evidence_error))
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Cleanup failed")
    print(experiment.directory / "report.json")


if __name__ == "__main__":
    main()
