#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded test-only legacy publication barrier; local Kafka, no IBM i connection."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("normal_probe", ROOT / "tools/dcg-normal-task-probe.py")
normal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(normal)
base = normal.base
CLASS = "io.debezium.connector.db2as400.As400LegacyBarrierProbeConnector"
BARRIER = "probe.legacy.barrier"


class BarrierExperiment(normal.NormalTaskExperiment):
    def stage(self):
        super().stage()
        jar = self.plugin / "synthetic-probe.jar"
        temporary = jar.with_suffix(".tmp")
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(jar) as source, zipfile.ZipFile(temporary, "w") as target:
            for item in source.infolist():
                target.writestr(item, source.read(item.filename))
            for name in ["As400LegacyBarrierProbeConnector", "As400TaskStopProbeConnector", "As400TaskStopFixture", "As400ReaderFixture"]:
                for path in classes.rglob(name + "*.class"):
                    target.write(path, str(path.relative_to(classes)))
        temporary.replace(jar)
        self.report.update({"scope": "Actual legacy RPC parser/handler/dispatcher, task poll/ack/stop and Connect; synthetic admission/schema and controlled JT400",
                            "transaction_boundary": "connector (poll only in ordinary-offset ambiguity controls)",
                            "initialization": "Public stopped-connector API seeds legacy offsets; tasks reject missing offsets",
                            "delivery_contract": "Test-only atomic complete-output barrier under Connect EOS, read_committed consumers",
                            "topics": "Data and barrier control topics, one partition each; no cross-topic ordering claim",
                            "transaction_metadata": "disabled, matching the conditional legacy handoff proof",
                            "native_proof": False, "migration_enabled": False, "custom_publication_authority": False})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["harness_sha256"]["tools/dcg-legacy-boundary-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def observer(self):
        # Independent consumer remains available if a faulted worker cannot start tasks/REST.
        self.observer_container = base.Experiment.container(self, "observer", ["--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{self.plugin}:/probe:ro", "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 3600"])
        return self.observer_container

    def create_legacy(self, worker, scenario, mode="batch", failure="none"):
        config = {"connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario,
                  "probe.mode": mode, "probe.failure": failure, "probe.policy": "fixture-LIB.TEST-v1", "topic.prefix": "test",
                  "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture", "database.password": "fixture",
                  "database.dbname": "DB", "database.schema": "LIB", "table.include.list": "LIB.TEST", "skipped.operations": "none",
                  "max.batch.size": "1", "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
                  "provide.transaction.metadata": "false", "event.converting.failure.handling.mode": "fail", "errors.tolerance": "none",
                  "exactly.once.support": "requested", "transaction.boundary": "connector" if mode == "batch" else "poll"}
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        for suffix in ["", "-control"]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create",
                         "--topic", "events-" + scenario + suffix, "--partitions", "1", "--replication-factor", "1")
        self.rest(worker, "/connectors", {"name": scenario, "initial_state": "STOPPED", "config": config})
        base.wait_for("stopped seed", lambda: self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        initial = {"offset.event_sequence": "39" if mode == "batch" else "40", "offset.time": "0", "offset.receiver": "RCV",
                   "offset.receiver_library": "LIB", "offset.processed": "true", "snapshot_completed": "true"}
        self.patch_offset(worker, scenario, initial)
        self.check(scenario + ": baseline durable before startup", self.checkpoint(self.observer_container, scenario, "seed") == initial,
                   "control/" + scenario + "-seed-offsets-read_committed.json")
        self.request(worker, scenario, "PUT", "/resume")
        return initial, self.generation(scenario, "w1")

    def release(self, generation, count=6):
        self.signal(generation, "release-read")
        self.marker(generation, "prepared")
        self.signal(generation, "permit", count)
        base.wait_for("polled " + str(count), lambda: sum(e["event"] == "poll" for e in self.events(generation)) >= count)

    def committed(self, scenario):
        return base.wait_for("durable complete-output barrier", lambda: (value if (value := self.checkpoint(
            self.observer_container, scenario, "barrier")) and value.get(BARRIER) == "complete-output-v1" else None), 75)

    def inspect(self, scenario, label):
        return self.records(self.observer_container, "events-" + scenario, scenario + "-" + label)

    def assert_complete(self, scenario, generation):
        offset = self.committed(scenario)
        rows = self.inspect(scenario, "complete")
        controls = self.records(self.observer_container, "events-" + scenario + "-control", scenario + "-complete-control")
        ops = [r["value"]["op"] if r["value"] else "tombstone" for r in rows]
        self.check(scenario + ": one complete key-change and truncate/reload, no leaked partial batch", ops == ["d", "tombstone", "c", "t", "c"]
                   and rows[0]["key"] == rows[1]["key"] == {"ID": 1} and rows[2]["key"] == {"ID": 2}
                   and rows[3]["key"] is None and rows[4]["value"]["after"] == {"ID": 3} and len(controls) == 1,
                   "control/" + scenario + "-complete*.json")
        self.check(scenario + ": barrier is exact legacy cut44, never invented F/W", offset["offset.event_sequence"] == "44"
                   and offset["offset.processed"] == "true" and offset["probe.generation"] == generation
                   and not any(k.startswith(("replay_from.", "completed_through.", "scan.")) for k in offset),
                   "control/" + scenario + "-barrier-offsets-read_committed.json")
        return offset

    def stop_workers(self, *workers):
        for worker in workers:
            self.command("docker", "stop", "--time", "3", worker)

    def ambiguous(self):
        offsets = []
        for mode in ["empty", "row"]:
            scenario = "legacy" + mode
            worker = self.start_worker(scenario, "w1")
            initial, generation = self.create_legacy(worker, scenario, mode)
            self.release(generation, 1)
            self.callbacks(generation, 1)
            offsets.append(self.checkpoint(self.observer_container, scenario, "ordinary"))
            rows = self.inspect(scenario, "ordinary")
            self.check(scenario + ": observed ordinary legacy publication", len(rows) == (0 if mode == "empty" else 1)
                       and (not rows or rows[0]["value"]["source"]["sequence"] == "42"), "control/" + scenario + "-ordinary.json")
            self.stop_workers(worker)
        self.check("same durable legacy offset40 permits two different row42 publication histories", offsets[0] == offsets[1]
                   and offsets[0]["offset.event_sequence"] == "40", "control/legacy*-ordinary-offsets-read_committed.json")

    def interrupted(self, fault, count):
        scenario = "barrier" + fault
        first = self.start_worker(scenario, "w1")
        initial, generation = self.create_legacy(first, scenario)
        if count:
            self.release(generation, count)
        else:
            self.marker(generation, "before-enqueue")
        if fault == "callback":
            raise AssertionError("callback uses its own schedule")
        uncommitted = self.snapshot(self.observer_container, scenario, "partial", isolation="read_uncommitted")
        self.check(scenario + ": fault reached after requested records entered Kafka", len(uncommitted) == min(count, 5),
                   "control/" + scenario + "-partial-records-read_uncommitted.json")
        # Read offsets independently; unresolved transactions may block read_committed
        # beyond their LSO, so the seed must remain the last visible checkpoint.
        self.check(scenario + ": no committed output or offset advancement before barrier", not self.inspect(scenario, "before-fault")
                   and self.checkpoint(self.observer_container, scenario, "before-fault") == initial,
                   "control/" + scenario + "-before-fault*.json")
        if fault == "reassign":
            self.command("docker", "kill", "--signal", "STOP", first)
        else:
            self.command("docker", "kill", first)
        second = self.start_worker(scenario, "w2")
        replacement = self.generation(scenario, "w2")
        self.check(scenario + ": replacement starts from durable legacy seed", json.loads((self.control / (replacement + ".start.json")).read_text())["loaded"] == initial,
                   "control/" + replacement + ".start.json")
        self.release(replacement)
        self.assert_complete(scenario, replacement)
        if fault == "reassign":
            self.command("docker", "kill", "--signal", "CONT", first)
            self.signal(generation, "permit", 6)
            base.wait_for("returning worker stopped or fenced", lambda: (self.control / (generation + ".stopped.json")).exists()
                          or any(s in self.command("docker", "logs", first) for s in ["ProducerFencedException", "InvalidProducerEpochException"]), 90)
            self.assert_complete(scenario, replacement)
        self.stop_workers(second)
        if fault == "reassign":
            self.stop_workers(first)

    def callback_crash(self):
        scenario = "barriercallback"
        first = self.start_worker(scenario, "w1")
        _, generation = self.create_legacy(first, scenario)
        self.signal(generation, "hold-callback")
        self.release(generation)
        self.marker(generation, "callback-held")
        durable = self.assert_complete(scenario, generation)
        self.check("Kafka commit survives missing barrier callback", not any(e["event"] == "commitRecord" and BARRIER in e["fields"]["offset"]
                   for e in self.events(generation)), "control/" + generation + ".jsonl")
        self.command("docker", "kill", first)
        second = self.start_worker(scenario, "w2")
        replacement = self.generation(scenario, "w2")
        self.marker(replacement, "resumed-barrier")
        self.signal(replacement, "permit", 100)
        self.check("fresh instance resumes durable barrier without re-emitting prefix", self.checkpoint(self.observer_container, scenario, "restart") == durable
                   and len(self.inspect(scenario, "restart")) == 5 and not any(e["event"] == "native" for e in self.events(replacement)),
                   "control/" + replacement + ".resumed-barrier.json")
        self.request(second, scenario, "PUT", "/stop")
        base.wait_for("stopped before offset-loss fault", lambda: self.rest(second, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        self.patch_offset(second, scenario, None)
        self.request(second, scenario, "PUT", "/resume")
        failed = base.wait_for("missing-offset task failure", lambda: (status if (status := self.rest(second, "/connectors/" + scenario + "/status"))["tasks"]
            and status["tasks"][0]["state"] == "FAILED" else None))
        self.check("lost offset rejects before any new output", "Missing or incomplete legacy offset" in failed["tasks"][0].get("trace", "")
                   and len(self.inspect(scenario, "offset-loss")) == 5, "control/" + scenario + "-offset-loss.json")
        self.stop_workers(second)

    def processing_failure(self, failure):
        scenario = "failure" + failure
        worker = self.start_worker(scenario, "w1")
        initial, generation = self.create_legacy(worker, scenario, failure=failure)
        self.signal(generation, "release-read")
        self.signal(generation, "permit", 100)
        if failure == "dispatch":
            self.marker(generation, "failure-held")
            base.wait_for("first key-change subrecord polled", lambda: sum(e["event"] == "poll" for e in self.events(generation)) == 1)
            uncommitted = self.snapshot(self.observer_container, scenario, "held", isolation="read_uncommitted")
            self.check(scenario + ": partial key-change actually entered Kafka", len(uncommitted) == 1,
                       "control/" + scenario + "-held-records-read_uncommitted.json")
            self.signal(generation, "release-failure")
        self.marker(generation, "producer-failure")
        def stopped_or_restarted():
            status = self.rest(worker, "/connectors/" + scenario + "/status")
            if status["tasks"] and status["tasks"][0]["state"] == "FAILED":
                return {"failed": status}
            starts = [p for p in self.control.glob(scenario + "-w1-*.start.json") if p.name != generation + ".start.json"]
            return {"restarted": json.loads(starts[0].read_text())} if starts else None
        outcome = base.wait_for("processing failure reaches task error handling", stopped_or_restarted)
        (self.control / (scenario + "-error-outcome.json")).write_text(json.dumps(outcome, indent=2))
        observed = json.loads((self.control / (generation + ".producer-failure.json")).read_text())
        handled = ("RowProcessingException" in outcome["failed"]["tasks"][0].get("trace", "")) if "failed" in outcome else outcome["restarted"]["loaded"] == initial
        self.check(scenario + ": error handler stops reader or restarts from unchanged durable offset before passing failed entry42",
                   handled and "RowProcessingException" in observed["error"] and observed["scan"]["offset.event_sequence"] == "40",
                   "control/" + scenario + "-error-outcome.json")
        data = self.inspect(scenario, "failed")
        control = self.records(self.observer_container, "events-" + scenario + "-control", scenario + "-failed-control")
        self.check(scenario + ": no committed prefix, barrier or offset advancement", not data and not control
                   and self.checkpoint(self.observer_container, scenario, "failed") == initial
                   and not (self.control / (generation + ".prepared.json")).exists(), "control/" + scenario + "-failed*.json")
        self.check(scenario + ": no record or barrier acknowledgement across attempts", not any(
                   e["event"] == "commitRecord" for path in self.control.glob(scenario + "-w1-*.jsonl") for e in self.events(path.stem)),
                   "control/" + scenario + "-w1-*.jsonl")
        self.stop_workers(worker)

    def exercise(self):
        self.observer()
        focused = os.environ.get("DCG_LEGACY_FAILURES_ONLY", "0")
        if focused not in {"0", "1"}:
            raise ValueError("DCG_LEGACY_FAILURES_ONLY must be 0 or 1")
        self.report["selected_scenarios"] = ["decode", "dispatch"] if focused == "1" else ["ambiguity", "before", "partial", "enqueued", "reassign", "callback", "decode", "dispatch"]
        self.save()
        if focused == "0":
            self.ambiguous()
            for fault, count in [("before", 0), ("partial", 2), ("enqueued", 5), ("reassign", 2)]:
                self.interrupted(fault, count)
            self.callback_crash()
        for failure in ["decode", "dispatch"]:
            self.processing_failure(failure)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: dcg-legacy-boundary-probe.py NEW_EVIDENCE_DIRECTORY")
    experiment = BarrierExperiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.exercise()
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report.update({"status": "failed", "error": str(error)})
        # Failure evidence is collected by an independent consumer, never the failed task.
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
