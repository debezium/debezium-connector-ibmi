#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Actual task/coordinator first-start integration; local Kafka and controlled source only."""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("initialization", ROOT / "tools/dcg-initialization-probe.py")
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
base = previous.base
CLASS = "io.debezium.connector.db2as400.As400FirstStartProbeConnector"


class FirstStartExperiment(previous.InitializationExperiment):
    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400FirstStartProbeConnector", "As400InitializationTaskFixture", "As400RecoveryTaskFixture"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        for item in (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":"):
            if "/com/datadoghq/" in item:
                shutil.copy2(item, self.plugin / Path(item).name)
        self.report.update({"scope": "Actual As400ConnectorTask startup/coordinator/snapshot/queue/dispatcher/callbacks/stop with explicit test-only admission; controlled native I/O; real Kafka",
                            "initialization": "No seeded offset: baseline and initial F/W are committed by the real task; Kafka history precedes F/W",
                            "topics": "Separate data A/B, heartbeat, transaction and initialization topics; one partition each; no cross-topic order claim"})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["harness_sha256"]["tools/dcg-first-start-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def create(self, worker, scenario, qualified=True, holds=()):
        self.report["running_scenario"] = scenario
        self.save()
        self.signal(scenario, "initialize-request")
        if qualified:
            (self.control / (scenario + ".qualified")).write_text("controlled-" + scenario)
        for phase in holds:
            self.signal(scenario, "hold-" + phase)
        for suffix in ["", "-A", "-B", "-tx", ".initialization"]:
            self.topic("events-" + scenario + suffix)
        self.topic("history-" + scenario)
        config = {"connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario,
                  "topic.naming.strategy": CLASS + "$Topics", "exactly.once.support": "requested", "transaction.boundary": "poll", "errors.tolerance": "none",
                  "topic.prefix": "test", "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture", "database.password": "fixture",
                  "database.dbname": "DB", "database.schema": "DCOC1107", "skipped.operations": "none", "max.batch.size": "1",
                  "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
                  "event.converting.failure.handling.mode": "fail", "provide.transaction.metadata": "true",
                  "producer.override.interceptor.classes": base.CLASS + "$BlockingInterceptor"}
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        return self.rest(worker, "/connectors", {"name": scenario, "config": config})

    def topic(self, name):
        self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create",
                     "--topic", name, "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        rows = super().snapshot(worker, scenario, label, offsets, isolation)
        if not offsets:
            for suffix in ["-tx", ".initialization"]:
                rows.extend(base.Experiment.snapshot(self, worker, scenario + suffix, label, isolation=isolation))
            (self.control / (scenario + "-" + label + "-all-" + isolation + ".json")).write_text(json.dumps(rows, indent=2))
        return rows

    @staticmethod
    def data(rows):
        return [r for r in rows if isinstance(r["value"], dict) and "op" in r["value"]]

    def acknowledgements(self):
        scenario = "first-ack"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, holds=["reader"])
        generation = self.generation(scenario, "w1")
        self.signal(generation, "ack-block", 6)
        self.signal(generation, "permit", 7)
        self.marker(generation, "ack-blocked")
        saved = self.checkpoint(worker, scenario, "baseline-pending")
        self.check("snapshot barrier cannot install history or F/W before Kafka commits", saved.get("initialization.phase") == "snapshot"
                   and not self.history(worker, scenario, "baseline-pending") and not (self.control / (generation + ".handoff.json")).exists(), "control/")
        self.check("uncommitted baseline barrier remains hidden", len(self.snapshot(worker, scenario, "baseline-pending")) == 6
                   and len(self.snapshot(worker, scenario, "baseline-pending", isolation="read_uncommitted")) == 7, "control/")
        self.signal(generation, "ack-release")
        self.marker(generation, "history-durable")
        self.check("acknowledged baseline and Kafka history precede initial F/W", len(self.history(worker, scenario, "history-durable")) == 1
                   and self.checkpoint(worker, scenario, "history-durable").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(generation, "ack-block", 7)
        (self.control / (generation + ".ack-release")).unlink()
        (self.control / (generation + ".ack-blocked.json")).unlink()
        self.signal(generation, "permit", 8)
        self.marker(generation, "ack-blocked")
        self.check("initial F/W producer acknowledgement alone cannot admit reader", not (self.control / (generation + ".handoff.json")).exists()
                   and self.checkpoint(worker, scenario, "initial-pending").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(generation, "ack-release")
        self.marker(generation, "handoff")
        initial = self.completed(worker, scenario, 29)
        self.check("actual coordinator hands off exclusive F=W=29 without seeding offsets", initial["replay_from.sequence"] == "29"
                   and initial["replay_from.inclusive"] == "false" and "scan.sequence" not in initial, "control/")
        self.signal(generation, "release-reader")
        self.signal(generation, "permit", 100)
        self.completed(worker, scenario, 59)
        before = self.assert_data(worker, scenario, "complete")
        self.marker(generation, "session-released")
        (self.control / (scenario + ".qualified")).unlink()
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.marker(fresh, "handoff")
        self.signal(fresh, "release-reader")
        self.signal(fresh, "permit", 100)
        self.marker(fresh, "read")
        self.completed(replacement, scenario, 59)
        after = self.assert_data(replacement, scenario, "restart")
        self.check("fresh task resumes without initialization session or duplicate data", self.data(after) == self.data(before), "control/")
        self.stop_task(replacement, scenario)
        self.request(replacement, scenario, "PATCH", "/offsets", {"offsets": [{"partition": {"server": "test"}, "offset": None}]})
        self.check("offset deletion is durably visible", self.checkpoint(replacement, scenario, "deleted") is None, "control/")
        (self.control / (scenario + ".enrolled")).unlink()
        (self.control / (scenario + ".qualified")).write_text("controlled-" + scenario)
        self.request(replacement, scenario, "PUT", "/resume")
        rejected = self.generation(scenario, "w2", exclude=[fresh])
        self.marker(rejected, "failed")
        self.check("missing offset and lost enrollment marker still reject existing broker history/output", self.snapshot(replacement, scenario, "rejected") == after
                   and self.checkpoint(replacement, scenario, "rejected") is None and len(self.history(replacement, scenario, "rejected")) == 1
                   and not (self.control / (rejected + ".handoff.json")).exists(), "control/")
        self.stop(replacement)

    def fencing(self):
        scenario = "first-fencing"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, holds=["reader"])
        old = self.generation(scenario, "w1")
        self.signal(old, "permit", 8)
        self.marker(old, "handoff")
        initial = self.completed(worker, scenario, 29)
        self.signal(old, "send-block", 8)
        self.signal(old, "release-reader")
        self.signal(old, "permit", 9)
        self.marker(old, "send-blocked")
        self.check("first streaming send cannot advance durable F/W before commit", self.checkpoint(worker, scenario, "held-send") == initial, "control/")
        self.command("docker", "pause", worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.marker(fresh, "handoff")
        self.check("Connect reassignment loads durable F/W and independent Kafka history", json.loads((self.control / (fresh + ".start.json")).read_text())["loaded"] == initial
                   and (self.control / (fresh + ".recovered.json")).exists(), "control/")
        self.signal(fresh, "release-reader")
        self.signal(fresh, "permit", 100)
        final = self.completed(replacement, scenario, 59)
        before = self.assert_data(replacement, scenario, "replacement")
        self.signal(old, "send-release")
        self.command("docker", "unpause", worker)
        self.marker(old, "send-released")
        def fenced():
            log = self.command("docker", "logs", worker)
            return log if re.search(r"InvalidProducerEpochException|ProducerFencedException", log) else None
        log = base.wait_for("broker rejects stale actual task producer", fenced, 90)
        (self.directory / "fencing.log").write_text(log)
        self.check("broker fencing prevents stale output/checkpoint after initialization handoff", self.snapshot(replacement, scenario, "after-stale") == before
                   and self.checkpoint(replacement, scenario, "after-stale") == final, "fencing.log; control/")
        self.stop(worker)
        self.stop(replacement)


def main():
    if len(sys.argv) not in [2, 3]:
        raise SystemExit("Usage: dcg-first-start-probe.py NEW_EVIDENCE_DIRECTORY [ack|crash|qualification|stop|fencing]")
    experiment = FirstStartExperiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        actions = {"ack": experiment.acknowledgements,
                   "crash": lambda: [experiment.crash(p) for p in ["partial-snapshot", "snapshot-acknowledged", "history-durable", "checkpoint-durable"]],
                   "qualification": experiment.qualification, "stop": experiment.stop_and_loss, "fencing": experiment.fencing}
        for name in ([sys.argv[2]] if len(sys.argv) == 3 else actions):
            actions[name]()
        experiment.check("custom publication owner/receipt gates remain absent", not list(experiment.control.glob("*.registry*"))
                         and not list(experiment.control.glob("*.authorized-worker*")), "control/")
        experiment.check("all test fault hooks stayed bounded", not list(experiment.control.glob("*.hook-timeout.json")), "control/")
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        experiment.save()
        try:
            experiment.capture_failure()
        except Exception as evidence_error:
            experiment.report["failure_evidence_error"] = str(evidence_error)
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Owned resource cleanup failed")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
