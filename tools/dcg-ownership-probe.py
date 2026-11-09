#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Test-only Connect ownership/recovery, with no custom publication authority."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("task_probe", ROOT / "tools/dcg-task-probe.py")
task_probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(task_probe)
base = task_probe.base
CLASS = "io.debezium.connector.db2as400.As400ConnectOwnershipProbeConnector"
EXPECTED = {"data": ["t", "c:1", "c:2", "c:3"], "tx": ["BEGIN", "END", "BEGIN", "END"], "hb": ["HB", "HB"]}
SUFFIX = {"data": "", "tx": "-tx", "hb": "-hb"}


class OwnershipExperiment(task_probe.TaskExperiment):
    test_classes = base.Experiment.test_classes

    # Reuse container/offset utilities directly; the stricter fixture's worker
    # authorization and one-topic validation must not run in this candidate.
    snapshot = base.Experiment.snapshot

    def start_worker(self, scenario, role):
        worker = base.Experiment.start_worker(self, scenario, role)
        if scenario == "partition":
            response = json.loads(self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", base.CLASS,
                                              "rest-put", "/admin/loggers/org.apache.kafka.connect.util.KafkaBasedLog", '{"level":"TRACE"}'))
            if response["status"] != 200:
                raise base.RestError(response["status"], response["body"])
        return worker

    def stage(self):
        # Reuse packaging only, never the receipt fixture's startup/create methods.
        task_probe.TaskExperiment.stage(self, include_receipts=False)
        jar_path = self.plugin / "synthetic-probe.jar"
        temporary = jar_path.with_suffix(".tmp")
        with zipfile.ZipFile(jar_path) as source, zipfile.ZipFile(temporary, "w") as target:
            for item in source.infolist():
                if any(name in item.filename for name in ["As400TaskProbeConnector", "As400ProbeReceiptStore", "As400CheckpointReceipts"]):
                    continue
                target.writestr(item, source.read(item.filename))
            classes = ROOT / "debezium-connector-ibmi/target/test-classes"
            for pattern in ["As400ConnectOwnershipProbeConnector*.class", "As400ConnectBrokerEvidence*.class"]:
                for path in classes.rglob(pattern):
                    target.write(path, str(path.relative_to(classes)))
        temporary.replace(jar_path)
        with zipfile.ZipFile(jar_path) as jar:
            self.check("candidate artifact excludes custom receipt and owner authority classes",
                       any("As400ConnectOwnershipProbeConnector$RuntimeTask.class" in n for n in jar.namelist()) and
                       not any(any(x in n for x in ["As400TaskProbeConnector", "As400ProbeReceiptStore", "As400CheckpointReceipts"])
                               for n in jar.namelist()), "plugin/synthetic-probe.jar")
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["harness_sha256"].update({name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in [
            "tools/dcg-ownership-probe", "tools/dcg-ownership-probe.py",
            "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400ConnectOwnershipProbeConnector.java",
            "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400ConnectBrokerEvidence.java"]})
        self.report["scope"] = "Real task/dispatcher; Connect-only publication and offsets; synthetic resume-only source, no native startup proof"
        self.report["custom_publication_authority"] = False
        boundary = os.environ.get("DCG_PROBE_BOUNDARY", "poll")
        if boundary not in ["poll", "connector"]:
            raise ValueError("DCG_PROBE_BOUNDARY must be poll or connector")
        self.report["transaction_boundary"] = boundary
        self.report["initialization"] = "Controller seeds synthetic F=W=1 using public stopped-connector offset API; tasks never initialize"
        self.report["topics"] = "Separate data/transaction/heartbeat topics, one partition each; no global cross-topic ordering claim"
        self.save()

    def request(self, worker, scenario, method, suffix, payload=None):
        for attempt in range(5):
            result = json.loads(self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS,
                                            "rest-method", method, "/connectors/" + scenario + suffix,
                                            json.dumps({} if payload is None else payload), timeout=20))
            if result["status"] < 400:
                return result
            # After the suspended worker rejoins as leader its old config-topic
            # producer can also be fenced. Kafka explicitly requests a retry and
            # reclaims that writer. Retry ONLY this idempotent administrative stop,
            # never a failed source send/commit or a failed data assertion.
            message = json.loads(result["body"]).get("message", "")
            retryable = method == "PUT" and suffix == "/stop" and (
                result["status"] == 409 or (result["status"] == 500 and message ==
                "Failed to write to config topic; this may be due to a transient error and the request can be safely retried"))
            if not retryable or attempt == 4:
                raise base.RestError(result["status"], result["body"])
            self.report.setdefault("administrative_retries", []).append({
                "scenario": scenario, "operation": method + " " + suffix, "attempt": attempt + 1, "response": result})
            self.save()
            time.sleep(2)

    def create(self, worker, scenario, support="requested", mixed=False):
        for suffix in SUFFIX.values():
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                         "--create", "--if-not-exists", "--topic", "events-" + scenario + suffix,
                         "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")
        config = {
            "connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario,
            "probe.mixed": str(mixed).lower(), "exactly.once.support": support, "transaction.boundary": self.report["transaction_boundary"], "errors.tolerance": "none",
            "topic.prefix": "synthetic", "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture",
            "database.password": "fixture", "database.dbname": "DB", "database.schema": "LIB", "skipped.operations": "none",
            "max.batch.size": "1", "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
            "event.converting.failure.handling.mode": "fail", "provide.transaction.metadata": "true",
            "producer.override.interceptor.classes": base.CLASS + "$BlockingInterceptor"}
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        self.rest(worker, "/connectors", {"name": scenario, "initial_state": "STOPPED", "config": config})
        base.wait_for("stopped before explicit synthetic initialization", lambda:
                      self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        baseline = json.loads(self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS, "baseline"))
        self.patch_offset(worker, scenario, baseline)
        self.check(scenario + ": synthetic baseline is in actual Kafka offsets before task startup",
                   self.checkpoint(worker, scenario, "seed") == baseline and
                   not list(self.control.glob(scenario + "-*.start.json")), "control/" + scenario + "-seed-offsets-read_committed.json")
        self.request(worker, scenario, "PUT", "/resume")

    def patch_offset(self, worker, scenario, offset):
        self.request(worker, scenario, "PATCH", "/offsets", {"offsets": [{"partition": {"server": "synthetic"}, "offset": offset}]})

    def snapshot_all(self, worker, scenario, label, isolation="read_committed"):
        result = {}
        for kind, suffix in SUFFIX.items():
            rows = base.Experiment.snapshot(self, worker, scenario + suffix, label, isolation=isolation)
            for row in rows:
                row["envelope"] = row["value"]
                row["value"] = json.loads(row["probe"])
            result[kind] = rows
        return result

    @staticmethod
    def operations(rows):
        return {kind: [r["value"]["op"] for r in entries] for kind, entries in rows.items()}

    def evidence(self, scenario, label):
        return "control/" + scenario + "*-" + label + "-records-read_committed.json"

    def validate(self, scenario, rows, mixed=False):
        data, tx, hb = rows["data"], rows["tx"], rows["hb"]
        self.check(scenario + ": real envelope, row key and original source metadata survive dispatch", bool(data) and all(
            (r["envelope"] is None and r["key"] is not None) if r["value"]["op"] == "tombstone" else
            (int(r["envelope"]["source"]["sequence"]) == r["value"]["sequence"] and
             r["envelope"]["source"]["ts_ms"] == r["value"]["sequence"] * 1000 and
             (r["key"] is None if r["envelope"]["op"] == "t" else
              r["key"]["ID"] == r["envelope"]["before" if r["envelope"]["op"] == "d" else "after"]["ID"]))
            for r in data), self.evidence(scenario, "final"))
        # Count within a generation/transaction. A partial attempt may contain BEGIN
        # without END; do not synthesize a global order by merging independent topics.
        ends = [r for r in tx if r["value"]["op"] == "END"]
        def valid_end(end):
            changes = [r for r in data if r["envelope"] and r["value"]["generation"] == end["value"]["generation"] and
                       r["envelope"]["transaction"]["id"] == end["envelope"]["id"]]
            return end["envelope"]["event_count"] == len(changes) and [r["envelope"]["transaction"]["total_order"] for r in changes] == list(range(1, len(changes) + 1))
        self.check(scenario + ": transaction metadata counts real changes excluding tombstones",
                   bool(ends) and all(valid_end(r) for r in ends), self.evidence(scenario, "final"))
        self.check(scenario + ": real heartbeat carries the later completed checkpoint", bool(hb) and all(
            r["key"] == {"serverName": "synthetic"} and list(r["envelope"]) == ["ts_ms"] and
            r["value"]["checkpoint"]["completed_through.sequence"] == str(r["value"]["sequence"]) for r in hb), self.evidence(scenario, "final"))
        projection = {99}
        for row in data:
            envelope = row["envelope"]
            if envelope:
                if envelope["op"] == "t":
                    projection.clear()
                elif envelope["op"] == "d":
                    projection.discard(row["key"]["ID"])
                elif envelope["op"] == "c":
                    projection.add(envelope["after"]["ID"])
        self.check(scenario + ": ordered table-topic replay converges in the synthetic projection",
                   projection == ({3} if mixed else {1, 2, 3}), self.evidence(scenario, "final"))
        self.check(scenario + ": all output carries immutable conservative F/W", all(
            r["value"]["checkpoint"]["completed_through.sequence"] == (
                str(r["value"]["sequence"]) if r["value"]["op"] == "HB" else
                "1" if mixed or r["value"]["sequence"] <= 6 else "6") and
            r["value"]["checkpoint"]["replay_from.sequence"] == (
                str(r["value"]["sequence"]) if r["value"]["op"] == "HB" else
                "2" if mixed or r["value"]["sequence"] <= 6 else "7") and
            r["value"]["checkpoint"]["replay_from.inclusive"] == ("false" if r["value"]["op"] == "HB" else "true") and
            "scan" not in r["value"]["checkpoint"] for entries in rows.values() for r in entries), self.evidence(scenario, "final"))

    def acknowledgements(self):
        scenario = "ack"
        worker = self.start_worker(scenario, "w1")
        try:
            self.create(worker, scenario, "required")
            raise AssertionError("Framework accepted a false EOS capability")
        except base.RestError as error:
            (self.directory / "required-rejected.json").write_text(error.body)
            self.check("required EOS rejects the honest UNSUPPORTED capability", error.status == 400 and "exactly" in error.body.lower(), "required-rejected.json")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 5)
        self.callbacks(generation, 5)
        before = self.completed(worker, scenario, 1)
        self.check("batch records persist F=2 inclusive and previous W=1", before["replay_from.sequence"] == "2" and
                   before["replay_from.inclusive"] == "true", "control/ack-wait-1-offsets-read_committed.json")
        self.signal(generation, "ack-block", 5)
        self.signal(generation, "permit", 6)
        self.marker(generation, "ack-blocked")
        visible = self.snapshot_all(worker, scenario, "delayed")
        raw = self.snapshot_all(worker, scenario, "delayed", "read_uncommitted")
        self.check("delayed heartbeat acknowledgement leaves durable W and committed visibility unchanged",
                   self.checkpoint(worker, scenario, "delayed") == before and not visible["hb"] and len(raw["hb"]) == 1,
                   "control/ack*-delayed-*-read_*.json")
        self.signal(generation, "ack-release")
        accepted = self.completed(worker, scenario, 6)
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, accepted)
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        rows = self.snapshot_all(replacement, scenario, "final")
        self.check("completed first batch is suppressed on fresh-process restart", self.operations(rows) == EXPECTED, self.evidence(scenario, "final"))
        self.validate(scenario, rows)
        self.stop(replacement)

    def interrupted(self, mixed=False):
        scenario = "mixed" if mixed else "interrupted"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, mixed=mixed)
        generation = self.generation(scenario, "w1")
        count = 5 if mixed else 3
        self.signal(generation, "permit", count)
        self.callbacks(generation, count)
        before = self.completed(worker, scenario, 1)
        partial = self.snapshot_all(worker, scenario, "partial")
        prefix = ["t", "c:1", "d", "tombstone"] if mixed else ["t", "c:1"]
        self.check(scenario + ": crash follows a committed clear and partial batch", self.operations(partial) == {
            "data": prefix, "tx": ["BEGIN"], "hb": []}, self.evidence(scenario, "partial"))
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, before)
        self.signal(fresh, "permit", 30)
        self.completed(replacement, scenario, 9)
        rows = self.snapshot_all(replacement, scenario, "final")
        expected = {"data": prefix + (["t", "c:1", "d", "tombstone", "c:2", "d", "tombstone", "c:3"] if mixed else EXPECTED["data"]),
                    "tx": ["BEGIN"] + (["BEGIN", "END"] if mixed else EXPECTED["tx"]), "hb": ["HB"] if mixed else EXPECTED["hb"]}
        self.check(scenario + ": complete batch replays before later table changes", self.operations(rows) == expected, self.evidence(scenario, "final"))
        def identity(row):
            env = row["envelope"]
            return row["key"], None if env is None else {k: env[k] for k in ["op", "before", "after", "source", "transaction"]}
        self.check(scenario + ": replay preserves row identity and original source metadata",
                   [identity(r) for r in rows["data"][:len(prefix)]] == [identity(r) for r in rows["data"][len(prefix):2 * len(prefix)]], self.evidence(scenario, "final"))
        self.validate(scenario, rows, mixed)
        self.stop(replacement)

    def latest_generation(self, scenario, role, excluded=()):
        def found():
            candidates = sorted(self.control.glob(scenario + "-" + role + "-*.start.json"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
            for path in candidates:
                generation = json.loads(path.read_text())["generation"]
                if generation not in excluded and (self.control / (generation + ".prepared.json")).exists():
                    return generation
            return None
        return base.wait_for("new assignment on " + role, found)

    def fencing(self, suspended=True):
        scenario = "fencing" if suspended else "partition"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "send-block", 1)
        self.signal(generation, "permit", 20)
        self.marker(generation, "send-blocked")
        before = self.completed(worker, scenario, 1)
        if suspended:
            self.command("docker", "pause", worker)
        else:
            self.command("docker", "network", "disconnect", self.network, worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, before)
        self.signal(fresh, "permit", 6)
        accepted = self.completed(replacement, scenario, 6)
        rows_before = self.snapshot_all(replacement, scenario, "replacement-complete")
        self.signal(generation, "send-release")
        if suspended:
            self.command("docker", "unpause", worker)
        else:
            self.command("docker", "network", "connect", self.network, worker)
        self.marker(generation, "send-released")
        def rejected():
            log = self.command("docker", "logs", worker)
            pattern = r"ProducerFencedException|InvalidProducerEpochException" if suspended else r"Cannot perform operation after producer has been closed"
            return log if self.rejected_source_send(generation, suspended) and re.search(pattern, log) else None
        log = base.wait_for("stale producer fails in " + scenario, rejected, 60)
        (self.directory / (scenario + "-old-worker.log")).write_text(log)
        rows_after = self.snapshot_all(replacement, scenario, "after-stale-send")
        self.check(scenario + ": old producer rejected with " + ("broker fencing" if suspended else "local close"),
                   rows_before == rows_after and self.operations(rows_after) == {
                       "data": ["t", "c:1", "c:2"], "tx": ["BEGIN", "BEGIN", "END"], "hb": ["HB"]},
                   scenario + "-old-worker.log; " + self.evidence(scenario, "after-stale-send"))
        self.check(scenario + ": stale generation cannot regress durable F/W", self.checkpoint(replacement, scenario, "after-stale-send") == accepted,
                   "control/" + scenario + "-after-stale-send-offsets-read_committed.json")
        # Remove the replacement worker, not its connector. Connect must legitimately
        # assign a NEW task to the original worker. No authorized-worker token exists.
        self.failure_context = {"scenario": scenario, "worker": worker, "accepted": accepted, "output": rows_after}
        self.stop(replacement)
        reassigned = self.latest_generation(scenario, "w1", [generation])
        status = base.wait_for("new task running on original worker", lambda:
                              self.running_on(worker, scenario, worker))
        (self.directory / (scenario + "-reassigned-status.json")).write_text(json.dumps(status, indent=2))
        self.assert_loaded(reassigned, accepted)
        self.signal(reassigned, "permit", 20)
        self.completed(worker, scenario, 9)
        rows = self.snapshot_all(worker, scenario, "final")
        self.check(scenario + ": original worker's new task publishes remaining work without custom authorization",
                   self.operations(rows) == {"data": EXPECTED["data"], "tx": ["BEGIN"] + EXPECTED["tx"], "hb": EXPECTED["hb"]} and
                   rows["data"][-1]["value"]["generation"] == reassigned and reassigned != generation,
                   scenario + "-reassigned-status.json; " + self.evidence(scenario, "final"))
        self.validate(scenario, rows)
        history = self.snapshot(worker, scenario, "final-history", offsets=True)
        values = [int(r["value"]["completed_through.sequence"]) for r in history if r["key"] == [scenario, {"server": "synthetic"}] and r["value"]]
        self.check(scenario + ": independently read committed W history never regresses", values == sorted(values) and values[-1] == 9,
                   "control/" + scenario + "-final-history-offsets-read_committed.json")
        self.stop_task(worker, scenario)
        self.check(scenario + ": normal task stop closes the actual lifecycle",
                   any(e["event"] == "stop" for e in self.events(reassigned)), reassigned + ".jsonl")
        self.stop(worker)

    def capture_failure(self):
        context = getattr(self, "failure_context", None)
        if context is None:
            return
        scenario, worker = context["scenario"], context["worker"]
        diagnostic = {"scenario": scenario, "expected_checkpoint": context["accepted"]}
        self.report["failure_evidence"] = diagnostic
        self.save()
        for label, command in [
                ("status", ["java", "-cp", "/probe/*:/opt/kafka/libs/*", base.CLASS, "rest", "/connectors/" + scenario + "/status", "GET"]),
                ("threads", ["jcmd", "1", "Thread.print"])]:
            try:
                output = self.command("docker", "exec", worker, *command, timeout=25, check=False)
                (self.directory / ("failure-" + label + ".txt")).write_text(output)
            except Exception as error:
                diagnostic[label + "_error"] = str(error)
        # A separate JVM in a fresh observer container has no Connect membership,
        # cached worker consumer state, producer, or publication/ownership role.
        observer = self.container("observer", ["--user", str(os.getuid()) + ":" + str(os.getgid()),
                                  "-v", str(self.plugin) + ":/probe:ro", "-v", str(self.control) + ":/control",
                                  "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 300"])
        self.command("docker", "exec", observer, "java", "-cp", "/probe/*:/opt/kafka/libs/*",
                     "io.debezium.connector.db2as400.As400ConnectBrokerEvidence", scenario, "/control/failure-broker.json", timeout=100)
        evidence = json.loads((self.control / "failure-broker.json").read_text())
        diagnostic["broker_file"] = "control/failure-broker.json"
        committed = evidence["read_committed"]["consumer"]
        def normalize(row):
            return {"offset": row["offset"], "key": None if row["key"] is None else json.loads(row["key"]),
                    "envelope": None if row["value"] is None else json.loads(row["value"]),
                    "probe": json.loads(row["probe"]), "timestamp": row["timestamp"]}
        expected = {kind: [{"offset": row["offset"], "key": row["key"], "envelope": row["envelope"],
                            "probe": row["value"], "timestamp": row["timestamp"]} for row in rows]
                    for kind, rows in context["output"].items()}
        actual = {kind: [normalize(row) for row in committed["events-" + scenario + suffix]["records"]] for kind, suffix in SUFFIX.items()}
        checkpoints = [None if row["value"] is None else json.loads(row["value"])
                       for row in committed["offsets-" + scenario]["records"]
                       if json.loads(row["key"]) == [scenario, {"server": "synthetic"}]]
        diagnostic["committed_output_unchanged_after_failure"] = actual == expected
        diagnostic["committed_checkpoint_unchanged_after_failure"] = bool(checkpoints) and checkpoints[-1] == context["accepted"]
        diagnostic["snapshots_complete"] = all(t["position"] >= t["end"] for t in committed.values())
        self.save()

    def running_on(self, worker, scenario, expected_worker):
        status = self.rest(worker, "/connectors/" + scenario + "/status", tolerate=True)
        return status if status and status["tasks"] and status["tasks"][0]["state"] == "RUNNING" and status["tasks"][0]["worker_id"] == expected_worker + ":8083" else None

    def stop_task(self, worker, scenario):
        self.request(worker, scenario, "PUT", "/stop")
        def stopped():
            status = self.rest(worker, "/connectors/" + scenario + "/status")
            return status["connector"]["state"] == "STOPPED" and not status["tasks"]
        base.wait_for("connector and tasks stopped", stopped)

    def invalid_offsets(self):
        scenario = "offsets"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 20)
        accepted = self.completed(worker, scenario, 9)
        before = self.snapshot_all(worker, scenario, "before-loss")
        self.check("offset rejection starts after a fully committed batch", self.operations(before) == EXPECTED, self.evidence(scenario, "before-loss"))
        self.stop_task(worker, scenario)
        self.patch_offset(worker, scenario, None)
        self.check("offset reset is durably visible before restart", self.checkpoint(worker, scenario, "deleted") is None,
                   "control/offsets-deleted-offsets-read_committed.json")
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        for case, offset, message in [
                ("missing", None, "Missing durable Kafka offset"),
                ("malformed", {**accepted, "replay_from.inclusive": "unknown"}, "Malformed replay inclusivity"),
                ("incompatible", {**accepted, "capture_fingerprint": "v1:" + "b" * 64}, "Journal incarnation or capture configuration changed")]:
            if offset is not None:
                self.patch_offset(replacement, scenario, offset)
            existing = set(self.control.glob(scenario + "-w2-*.start.json"))
            self.request(replacement, scenario, "PUT", "/resume")
            path = base.wait_for("new rejected " + case + " task", lambda: next(iter(set(self.control.glob(scenario + "-w2-*.start.json")) - existing), None))
            startup = json.loads(path.read_text())
            self.signal(startup["generation"], "permit", 20)
            def failed():
                status = self.rest(replacement, "/connectors/" + scenario + "/status")
                return status if status["tasks"] and status["tasks"][0]["state"] == "FAILED" else None
            status = base.wait_for("startup rejects " + case, failed)
            (self.directory / (case + "-failed.json")).write_text(json.dumps(status, indent=2))
            self.check(case + " offset fails before journal preparation", message in status["tasks"][0]["trace"] and
                       not (self.control / (startup["generation"] + ".prepared.json")).exists(), case + "-failed.json; " + path.name)
            after = self.snapshot_all(replacement, scenario, case)
            self.check(case + " offset restart emits zero output and cannot replace the rejected checkpoint",
                       before == after and self.checkpoint(replacement, scenario, case) == offset and
                       not any(e["event"] in ["poll", "commitRecord"] for e in self.events(startup["generation"])),
                       self.evidence(scenario, case) + "; control/offsets-" + case + "-offsets-read_committed.json")
            self.stop_task(replacement, scenario)
        self.stop(replacement)


def main():
    cases = ["ack", "interrupted", "mixed", "fencing", "partition", "offsets"]
    if len(sys.argv) not in [2, 3] or (len(sys.argv) == 3 and sys.argv[2] not in cases):
        raise SystemExit("Usage: dcg-ownership-probe.py NEW_EVIDENCE_DIRECTORY [" + "|".join(cases) + "]")
    experiment = OwnershipExperiment(Path(sys.argv[1]))
    selected = [sys.argv[2]] if len(sys.argv) == 3 else cases
    experiment.report["selected_scenarios"] = selected
    try:
        experiment.stage()
        experiment.start_broker()
        actions = {"ack": experiment.acknowledgements, "interrupted": experiment.interrupted,
                   "mixed": lambda: experiment.interrupted(True), "fencing": experiment.fencing,
                   "partition": lambda: experiment.fencing(False), "offsets": experiment.invalid_offsets}
        for name in selected:
            experiment.failure_context = None
            experiment.report["running_scenario"] = name
            actions[name]()
        experiment.check("no owner registry or authorized-worker files were used",
                         not list(experiment.control.glob("*.registry*")) and not list(experiment.control.glob("*.authorized-worker*")), "control/")
        experiment.check("all fault gates released before their deadlines", not list(experiment.control.glob("*.hook-timeout.json")), "control/*-released.json")
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        experiment.save()
        try:
            experiment.capture_failure()
        except Exception as diagnostic_error:
            experiment.report["failure_evidence_error"] = str(diagnostic_error)
            experiment.save()
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Cleanup failed; inspect report.json")
    print(str(experiment.directory / "report.json"), flush=True)


if __name__ == "__main__":
    main()
