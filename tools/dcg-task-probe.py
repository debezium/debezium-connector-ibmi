#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Local real-task lifecycle experiment; source discovery/startup remain synthetic."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("connect_probe", ROOT / "tools/dcg-connect-probe.py")
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
CLASS = "io.debezium.connector.db2as400.As400TaskProbeConnector"


class TaskExperiment(base.Experiment):
    test_classes = ROOT / "debezium-connector-ibmi/target/prototype-test-classes"

    def stage(self, include_receipts=True):
        super().stage()
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "w") as jar:
            seen = set()
            for module in ["debezium-connector-ibmi", "journal-parsing", "jt400-override-ccsid"]:
                directory = ROOT / module / "target/classes"
                for path in directory.rglob("*"):
                    if path.is_file() and str(path.relative_to(directory)) not in seen:
                        name = str(path.relative_to(directory))
                        jar.write(path, name)
                        seen.add(name)
            directory = self.test_classes
            patterns = ["As400ConnectProbeConnector*.class"]
            if include_receipts:
                patterns += ["As400TaskProbeConnector*.class", "As400ProbeReceiptStore*.class", "As400CheckpointReceipts*.class"]
            for pattern in patterns:
                for path in directory.rglob(pattern):
                    jar.write(path, str(path.relative_to(directory)))
        dependencies = (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":")
        for item in dependencies:
            path = Path(item)
            if (("/io/debezium/" in item and not path.name.endswith("-tests.jar")
                 and not any(x in path.name for x in ["ibmi", "jt400-override"])) or "/io/openlineage/" in item):
                shutil.copy2(path, self.plugin / path.name)
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        sources = [
            "tools/dcg-task-probe", "tools/dcg-task-probe.py",
            "debezium-connector-ibmi/src/main/java/io/debezium/connector/db2as400/As400BatchDispatcher.java"]
        if include_receipts:
            sources += [
                "debezium-connector-ibmi/src/prototype-test/java/io/debezium/connector/db2as400/As400CheckpointReceipts.java",
                "debezium-connector-ibmi/src/prototype-test/java/io/debezium/connector/db2as400/As400TaskProbeConnector.java",
                "debezium-connector-ibmi/src/prototype-test/java/io/debezium/connector/db2as400/As400ProbeReceiptStore.java"]
        self.report["harness_sha256"].update({name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources})
        self.report["scope"] = "Real As400ConnectorTask/BaseSourceTask lifecycle, dispatcher and emitter; synthetic source startup and local test authority"
        self.save()

    def start_worker(self, scenario, role):
        # The controller designates the next test owner. A resumed old worker may
        # rejoin Connect, but it must not claim a new fixture generation merely
        # because Connect assigns it another task during the stale-callback test.
        allowed = self.control / (scenario + ".authorized-worker")
        temporary = allowed.with_suffix(".tmp")
        temporary.write_text(scenario + "-" + role + "\n")
        temporary.replace(allowed)
        return super().start_worker(scenario, role)

    def create(self, worker, scenario, support="requested", batch=1):
        # Preserve the full diagnostic trace; earlier fixture versions also used
        # historical Kafka timestamps for their marker records.
        self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                     "--create", "--if-not-exists", "--topic", "events-" + scenario, "--partitions", "1",
                     "--replication-factor", "1", "--config", "retention.ms=-1")
        path = self.control / (scenario + ".registry.json")
        if not path.exists():
            with path.open("x") as stream:
                json.dump({"enrollment": "fixture-" + scenario, "generation": 0, "manifest": "fixture-manifest", "receipt": None}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            descriptor = os.open(self.control, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return self.rest(worker, "/connectors", {"name": scenario, "config": {
            "connector.class": CLASS, "name": scenario, "probe.mixed": str(scenario == "mixed-output").lower(), "tasks.max": "1", "probe.topic": "events-" + scenario,
            "exactly.once.support": support, "transaction.boundary": "poll", "errors.tolerance": "none",
            "topic.prefix": "synthetic", "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture",
            "database.password": "fixture", "database.dbname": "DB", "database.schema": "LIB", "skipped.operations": "none",
            "max.batch.size": str(batch), "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000", "event.converting.failure.handling.mode": "fail",
            "provide.transaction.metadata": "true", "producer.override.interceptor.classes": base.CLASS + "$BlockingInterceptor"}})

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        rows = super().snapshot(worker, scenario, label, offsets=offsets, isolation=isolation)
        if not offsets:
            for row in rows:
                if "probe" in row:
                    row["envelope"] = row["value"]
                    row["value"] = json.loads(row["probe"])
                    # Compare source timestamp, not the broker timestamp assigned to dispatcher records.
                    row["broker_timestamp"] = row["timestamp"]
                    if row["envelope"] and "source" in row["envelope"]:
                        row["timestamp"] = row["envelope"]["source"]["ts_ms"]
                    elif row["envelope"] and "status" in row["envelope"]:
                        row["timestamp"] = row["envelope"]["ts_ms"]
            if label == "final":
                self.validate_dispatch(scenario, rows)
        return rows

    def receipt(self, scenario):
        return json.loads((self.control / (scenario + ".registry.json")).read_text())["receipt"]

    def receipt_completed(self, scenario, sequence):
        def found():
            receipt = self.receipt(scenario)
            return receipt if receipt and receipt["checkpoint"]["completed_through.sequence"] == str(sequence) else None
        return base.wait_for("durable receipt W=" + str(sequence), found, 60)

    def validate_dispatch(self, scenario, rows):
        self.validate_metadata(scenario, rows)
        data = [r for r in rows if r["value"]["op"] in ["t", "c:1", "c:2", "c:3"]]
        self.check(scenario + ": real dispatcher preserves envelope, key and source metadata", bool(data) and all(
            row["envelope"]["op"] in ["t", "c"] and
            int(row["envelope"]["source"]["sequence"]) == row["value"]["sequence"] and
            row["envelope"]["source"]["ts_ms"] == row["value"]["sequence"] * 1000 and
            (row["key"] is None if row["envelope"]["op"] == "t" else row["key"]["ID"] == row["envelope"]["after"]["ID"])
            for row in data), "control/" + scenario + "-final-records-read_committed.json")

    def validate_metadata(self, scenario, rows):
        batches = []
        current = None
        for row in rows:
            op = row["value"]["op"]
            if op == "BEGIN":
                current = {"begin": row["envelope"], "data": []}
            elif op == "END" and current is not None:
                current["end"] = row["envelope"]
                batches.append(current)
                current = None
            elif op not in ["HB", "tombstone"] and current is not None:
                current["data"].append(row["envelope"])
        self.check(scenario + ": real transaction metadata counts emitted changes exactly once",
                   bool(batches) and all(batch["begin"]["id"].startswith("dcg-v1:") and
                     batch["end"]["id"] == batch["begin"]["id"] and
                     batch["end"]["event_count"] == len(batch["data"]) and
                     all(event["transaction"]["id"] == batch["begin"]["id"] and
                         event["transaction"]["total_order"] == index + 1
                         for index, event in enumerate(batch["data"])) for batch in batches),
                   "control/" + scenario + "-final-records-read_committed.json")
        heartbeats = [r for r in rows if r["value"]["op"] == "HB"]
        self.check(scenario + ": real heartbeat records carry completed checkpoints",
                   bool(heartbeats) and all(r["key"] == {"serverName": "synthetic"} and
                       list(r["envelope"]) == ["ts_ms"] and
                       r["value"]["checkpoint"]["completed_through.sequence"] == str(r["value"]["sequence"])
                       for r in heartbeats), "control/" + scenario + "-final-records-read_committed.json")

    def crash_receipt(self, phase):
        scenario = "receipt-" + phase
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "receipt-" + phase + "-block", 6)
        self.signal(generation, "permit", 6)
        self.marker(generation, "receipt-" + phase + "-blocked")
        durable = self.completed(worker, scenario, 6)
        before = self.receipt(scenario)
        expected = "1" if phase == "before" else "6"
        self.check(scenario + ": crash boundary distinguishes Kafka offset from receipt persistence",
                   before["checkpoint"]["completed_through.sequence"] == expected,
                   generation + ".receipt-" + phase + "-blocked.json")
        (self.directory / (scenario + "-before-crash.json")).write_text(json.dumps(before, indent=2))
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, durable)
        reconciled = self.receipt_completed(scenario, 6)
        self.check(scenario + ": fresh owner reconciles from exact durable read-back", reconciled["checkpoint"] == durable and
                   reconciled["owner"]["generation"] == before["owner"]["generation"] + 1,
                   fresh + ".jsonl")
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        final = self.receipt_completed(scenario, 9)
        rows = self.snapshot(replacement, scenario, "final")
        self.check(scenario + ": committed batch stays suppressed and receipt advances under new owner",
                   [r["value"]["op"] for r in rows] == base.EXPECTED and self.assert_projection(rows) and
                   final["owner"] == reconciled["owner"], "control/" + scenario + "-final-records-read_committed.json")
        self.stop(replacement)

    def stale_callback(self):
        scenario = "stale-callback"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "callback-block", 5)
        self.signal(generation, "permit", 6)
        self.marker(generation, "callback-blocked")
        durable = self.completed(worker, scenario, 6)
        old = self.receipt(scenario)
        self.command("docker", "pause", worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, durable)
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        finished = self.receipt_completed(scenario, 9)
        rows_before = self.snapshot(replacement, scenario, "replacement-complete")
        self.signal(generation, "callback-release")
        self.command("docker", "unpause", worker)
        base.wait_for("stale acknowledged record rejected", lambda: any(
            e["event"] == "ackRejected" and e["fields"]["value"]["index"] == 5 and "Stale owner" in e["fields"]["error"]
            for e in self.events(generation)), 60)
        rows_after = self.snapshot(replacement, scenario, "final")
        self.check("stale callback cannot change receipt or output after ownership transfer", self.receipt(scenario) == finished and
                   rows_before == rows_after and finished["owner"]["generation"] == old["owner"]["generation"] + 1,
                   generation + ".jsonl; stale-callback.registry.json")
        self.stop(worker)
        self.stop(replacement)

    def coalesced(self):
        scenario = "coalesced"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, batch=3)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 20)
        self.completed(worker, scenario, 9)
        self.receipt_completed(scenario, 9)
        rows = self.snapshot(worker, scenario, "final")
        self.check("coalesced BaseSourceTask callbacks preserve exact checkpoint and record order",
                   [r["value"]["op"] for r in rows] == base.EXPECTED and self.assert_projection(rows),
                   generation + ".jsonl")
        # Stop via the actual task stop path before killing the disposable worker.
        response = json.loads(self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", base.CLASS,
                                           "rest-put", "/connectors/" + scenario + "/stop", "{}"))
        if response["status"] >= 400:
            raise RuntimeError(response["body"])
        base.wait_for("real task stop lifecycle", lambda: any(e["event"] == "stop" for e in self.events(generation)))
        status = self.rest(worker, "/connectors/" + scenario + "/status")
        (self.directory / "coalesced-stopped.json").write_text(json.dumps(status, indent=2))
        self.check("normal BaseSourceTask stop closes the receipt adapter without worker errors",
                   status["connector"]["state"] == "STOPPED" and not status["tasks"] and
                   "] ERROR " not in self.command("docker", "logs", worker),
                   generation + ".jsonl; coalesced-stopped.json; " + worker + ".log")
        self.stop(worker)

    def mixed_output(self):
        scenario = "mixed-output"
        expected = ["BEGIN", "t", "c:1", "d", "tombstone", "c:2", "d", "tombstone", "c:3", "END", "HB"]
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 5)
        self.callbacks(generation, 5)
        before = self.completed(worker, scenario, 1)
        partial = self.snapshot(worker, scenario, "partial")
        self.check("mixed output crash follows actual key-change delete and tombstone",
                   [r["value"]["op"] for r in partial] == expected[:5] and partial[4]["envelope"] is None and
                   partial[3]["key"] == partial[4]["key"] == {"ID": 1},
                   "control/mixed-output-partial-records-read_committed.json")
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, before)
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        self.receipt_completed(scenario, 9)
        rows = self.snapshot(replacement, scenario, "final")
        self.check("mixed output restart repeats complete batch including both tombstones",
                   [r["value"]["op"] for r in rows] == expected[:5] + expected and
                   all(r["value"]["checkpoint"]["completed_through.sequence"] == "1" for r in rows[:-1]),
                   "control/mixed-output-final-records-read_committed.json")
        projection = {99}
        for row in rows:
            envelope = row["envelope"]
            if envelope and "op" in envelope:
                if envelope["op"] == "t":
                    projection.clear()
                elif envelope["op"] == "d":
                    projection.discard(row["key"]["ID"])
                elif envelope["op"] == "c":
                    projection.add(envelope["after"]["ID"])
        self.check("ordered replay of actual envelopes converges after key change and delete", projection == {3},
                   "control/mixed-output-final-records-read_committed.json")
        self.stop(replacement)

    def missing_offset(self):
        scenario = "missing-offset"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, batch=3)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 20)
        self.completed(worker, scenario, 9)
        accepted = self.receipt_completed(scenario, 9)
        before = self.snapshot(worker, scenario, "before-offset-loss")
        self.check("offset-loss fixture starts with fully committed output and receipt",
                   [r["value"]["op"] for r in before] == base.EXPECTED and self.assert_projection(before),
                   "control/" + scenario + "-before-offset-loss-records-read_committed.json")
        self.stop(worker)
        # Delete only this disposable scenario's Kafka offset topic, preserving
        # output and the external receipt. The replacement creates an empty topic.
        topic = "offsets-" + scenario
        self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                     "--delete", "--topic", topic)
        base.wait_for("disposable offset topic removed", lambda: topic not in self.command(
            "docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list").splitlines())
        replacement = self.start_worker(scenario, "w2")
        started = base.wait_for("missing-offset startup attempt", lambda: next(iter(self.control.glob(scenario + "-w2-*.start.json")), None))
        startup = json.loads(started.read_text())
        fresh = startup["generation"]
        self.signal(fresh, "permit", 20)
        def failed():
            status = self.rest(replacement, "/connectors/" + scenario + "/status")
            return status if status["tasks"] and status["tasks"][0]["state"] == "FAILED" else None
        status = base.wait_for("missing-offset task fails before publication", failed)
        (self.directory / "missing-offset-failed.json").write_text(json.dumps(status, indent=2))
        self.check("missing durable offset with an existing receipt rejects startup",
                   startup["loaded"] == {} and "Missing durable Kafka offset for existing receipt" in status["tasks"][0]["trace"],
                   "missing-offset-failed.json; " + started.name)
        after = self.snapshot(replacement, scenario, "after-rejected-start")
        self.check("missing-offset restart emits zero new records and preserves the accepted receipt",
                   before == after and self.receipt(scenario) == accepted and
                   not (self.control / (fresh + ".prepared.json")).exists() and
                   not any(e["event"] in ["poll", "commitRecord", "receiptPersisted"] for e in self.events(fresh)),
                   "control/" + scenario + "-after-rejected-start-records-read_committed.json; " + scenario + ".registry.json")
        self.stop(replacement)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: tools/dcg-task-probe NEW_EVIDENCE_DIRECTORY")
    experiment = TaskExperiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.stale_callback()
        experiment.mixed_output()
        experiment.missing_offset()
        experiment.acknowledgements()
        experiment.interrupted()
        experiment.crash_receipt("before")
        experiment.crash_receipt("after")
        experiment.coalesced()
        experiment.check("all task fault-injection gates completed within their deadlines",
                         not list(experiment.control.glob("*.hook-timeout.json")), "control/*-released.json")
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Cleanup failed; inspect report.json")
    print(str(experiment.directory / "report.json"), flush=True)


if __name__ == "__main__":
    main()
