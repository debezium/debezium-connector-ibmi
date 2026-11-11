#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded local Connect verification of the normal task recovery integration; no IBM i."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("ownership_probe", ROOT / "tools/dcg-ownership-probe.py")
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)
base = previous.base
CLASS = "io.debezium.connector.db2as400.As400NormalTaskProbeConnector"


class NormalTaskExperiment(previous.OwnershipExperiment):
    def stage(self):
        super().stage()
        jar_path = self.plugin / "synthetic-probe.jar"
        temporary = jar_path.with_suffix(".tmp")
        with zipfile.ZipFile(jar_path) as source, zipfile.ZipFile(temporary, "w") as target:
            for item in source.infolist():
                target.writestr(item, source.read(item.filename))
            classes = ROOT / "debezium-connector-ibmi/target/test-classes"
            for pattern in ["As400NormalTaskProbeConnector*.class", "As400RecoveryTaskFixture*.class", "As400RecoveryTestSupport*.class"]:
                for path in classes.rglob(pattern):
                    target.write(path, str(path.relative_to(classes)))
            path = classes / "native-transactions/open-clear-progress.json"
            target.write(path, str(path.relative_to(classes)))
        temporary.replace(jar_path)
        for item in (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":"):
            if any(part in item for part in ["/com/datadoghq/", "/org/mockito/", "/net/bytebuddy/", "/org/objenesis/", "/org/assertj/", "/org/junit/", "/org/opentest4j/", "/org/apiguardian/"]):
                shutil.copy2(item, self.plugin / Path(item).name)
        self.agent = next(self.plugin.glob("byte-buddy-agent-*.jar")).name
        self.report.update({"scope": "Actual normal task/coordinator/queue/dispatcher/callbacks; captured native bytes with controlled I/O; no IBM i",
                            "initialization": "Controller records acknowledged test baseline and bindings to Kafka history before stopped-connector F/W seeding",
                            "topics": "Separate A/B, transaction and heartbeat topics; one partition; no cross-topic order claim"})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["harness_sha256"]["tools/dcg-normal-task-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        if name != "broker":
            path = self.directory / (name + ".properties")
            with path.open("a") as stream:
                stream.write("request.timeout.ms=5000\ndefault.api.timeout.ms=10000\nworker.sync.timeout.ms=20000\nrebalance.timeout.ms=30000\n")
            args = ["-e", "KAFKA_OPTS=-javaagent:/probe/" + self.agent, *args]
        return super().container(name, args)

    def checkpoint(self, worker, scenario, label):
        rows = self.snapshot(worker, scenario, label, offsets=True)
        matches = [r["value"] for r in rows if r["key"] == [scenario, {"server": "test"}]]
        return matches[-1] if matches else None

    def patch_offset(self, worker, scenario, offset):
        self.request(worker, scenario, "PATCH", "/offsets", {"offsets": [{"partition": {"server": "test"}, "offset": offset}]})

    def generation(self, scenario, role):
        path = base.wait_for("normal task start", lambda: next(iter(self.control.glob(scenario + "-" + role + "-*.start.json")), None))
        return json.loads(path.read_text())["generation"]

    def records(self, worker, topic, label):
        file = label + ".json"
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", base.CLASS,
                     "broker:9092", topic, "read_committed", "/control/" + file, timeout=35)
        rows = json.loads((self.control / file).read_text())
        for row in rows:
            for field in ["key", "value"]:
                row[field] = json.loads(row[field]) if row[field] is not None else None
        return rows

    def create_normal(self, worker, scenario):
        config = {"connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "unused",
                  "topic.prefix": "test", "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture", "database.password": "fixture",
                  "database.dbname": "DB", "database.schema": "DCOC1107", "skipped.operations": "none", "max.batch.size": "1", "max.queue.size": "64",
                  "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000", "provide.transaction.metadata": "true",
                  "event.converting.failure.handling.mode": "fail", "exactly.once.support": "requested", "transaction.boundary": "poll", "errors.tolerance": "none"}
        if scenario == "normalduplicates":
            config.update({"max.batch.size": "512", "max.queue.size": "2048"})
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        for topic in ["test.DCOC1107.A", "test.DCOC1107.B", "test.transaction", "__debezium-heartbeat.test"]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create", "--if-not-exists",
                         "--topic", topic, "--partitions", "1", "--replication-factor", "1")
        self.rest(worker, "/connectors", {"name": scenario, "initial_state": "STOPPED", "config": config})
        base.wait_for("stopped for baseline", lambda: self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS, "baseline", scenario)
        initial = json.loads((self.control / (scenario + ".baseline.json")).read_text())
        self.patch_offset(worker, scenario, initial)
        self.check("preexisting Kafka F/W and immutable history before task admission", self.checkpoint(worker, scenario, "seed") == initial,
                   "control/" + scenario + "-seed-offsets-read_committed.json")
        self.signal(scenario, "tail", 631 if scenario == "normalduplicates" else 42)
        self.request(worker, scenario, "PUT", "/resume")
        return initial

    def exercise(self):
        scenario = "normalresume"
        worker = self.start_worker(scenario, "w1")
        initial = self.create_normal(worker, scenario)
        first = self.generation(scenario, "w1")
        saved = self.completed(worker, scenario, 42)
        a = self.records(worker, "test.DCOC1107.A", "open-A")
        b = self.records(worker, "test.DCOC1107.B", "open-B")
        self.check("unrelated A progresses while B clear is open; F remains31 and W42", [r["value"]["after"]["ID"] for r in a] == [8] and not b and
                   saved["replay_from.sequence"] == "31" and saved["completed_through.sequence"] == "42" and "scan.sequence" not in saved, "control/open-*.json")
        # Hold the old reader after durable acknowledgement. Connect's own restart
        # moves the task to the replacement; no custom owner/receipt gate is present.
        self.signal(first, "hold")
        self.marker(first, "held")
        replacement = self.start_worker(scenario, "w2")
        self.command("docker", "kill", "--signal", "STOP", worker)
        self.signal(scenario, "tail", 64)
        second = self.generation(scenario, "w2")
        final = self.completed(replacement, scenario, 59)
        self.check("replacement recovers the actual durable offset and Kafka binding history",
                   json.loads((self.control / (second + ".start.json")).read_text())["loaded"] == saved and
                   (self.control / (second + ".recovered.json")).exists(), "control/" + second + ".recovered.json")
        self.command("docker", "kill", "--signal", "CONT", worker)
        self.marker(first, "stop-requested")
        self.check("stop requested while old read is held is not reported as drained", not (self.control / (first + ".drained.json")).exists(),
                   "control/" + first + ".stop-requested.json")
        self.signal(first, "release")
        self.marker(first, "drained")
        self.check("old reader drains only after its late read returns", json.loads((self.control / (first + ".drained.json")).read_text())["closes"] == 1,
                   "control/" + first + ".drained.json")
        self.request(replacement, scenario, "PUT", "/stop")
        base.wait_for("stopped connector", lambda: self.rest(replacement, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        a = self.records(replacement, "test.DCOC1107.A", "final-A")
        b = self.records(replacement, "test.DCOC1107.B", "final-B")
        after = self.checkpoint(replacement, scenario, "final")
        self.check("late old read adds no output or checkpoint; rollback stays invisible", [r["value"]["after"]["ID"] for r in a] == [8, 9] and
                   [r["value"]["op"] for r in b] == ["t", "c"] and b[0]["key"] is None and b[1]["value"]["after"]["ID"] == 100 and after == final,
                   "control/final-*.json")
        self.check("real schema, key and source metadata survive normal dispatch", b[1]["key"] == {"ID": 100} and
                   b[1]["value"]["source"]["receiver"] == "QSQJRN0003" and int(b[0]["value"]["source"]["sequence"]) > 29, "control/final-B.json")
        # Restart with an actual missing offset but retained history. It must emit
        # nothing and must not create a substitute starting checkpoint.
        old_starts = set(self.control.glob("*.start.json"))
        self.patch_offset(replacement, scenario, None)
        self.request(replacement, scenario, "PUT", "/resume")
        base.wait_for("missing-offset failure", lambda: any(t["state"] == "FAILED" for t in self.rest(replacement, "/connectors/" + scenario + "/status")["tasks"]))
        new_starts = set(self.control.glob("*.start.json")) - old_starts
        self.check("missing-offset restart rejects before history or reader work", bool(new_starts) and all(
            json.loads(p.read_text())["loaded"] == {} and not p.with_name(p.name.replace(".start.json", ".recovered.json")).exists() for p in new_starts), "control/*.start.json")
        self.check("missing-offset failure leaves output unchanged and offset missing", self.records(replacement, "test.DCOC1107.A", "missing-A") == a and
                   self.records(replacement, "test.DCOC1107.B", "missing-B") == b and self.checkpoint(replacement, scenario, "missing") is None, "control/missing-*.json")
        self.check("no custom ownership or receipt authorization in candidate", not list(self.control.glob("*.registry*")) and
                   not list(self.control.glob("*.authorized-worker*")), "control/")

    def exercise_pause(self):
        scenario = "normalresume"
        worker = self.start_worker(scenario, "w1")
        self.create_normal(worker, scenario)
        generation = self.generation(scenario, "w1")
        saved = self.completed(worker, scenario, 42)
        self.signal(generation, "hold")
        self.marker(generation, "held")
        self.request(worker, scenario, "PUT", "/pause")
        base.wait_for("paused task", lambda: all(t["state"] == "PAUSED" for t in
                      self.rest(worker, "/connectors/" + scenario + "/status")["tasks"]))
        # Allow the real reader to return its committed batch while polling is paused.
        self.signal(scenario, "tail", 64)
        self.signal(generation, "release")
        base.wait_for("paused reader returned batch", lambda:
                      json.loads((self.control / (generation + ".read.json")).read_text())["tail"] == 64)
        started = time.monotonic()
        while time.monotonic() - started < 125:
            time.sleep(1)
        self.report["paused_seconds"] = time.monotonic() - started
        self.check("pause beyond former 120s deadline preserves durable W and hides pending truncate",
                   self.checkpoint(worker, scenario, "paused") == saved
                   and not self.records(worker, "test.DCOC1107.B", "paused-B"), "control/paused-B.json")
        self.request(worker, scenario, "PUT", "/resume")
        self.completed(worker, scenario, 59)
        rows = self.records(worker, "test.DCOC1107.B", "resumed-B")
        self.check("same task resumes pending truncate/reload without a timeout failure or restart",
                   [r["value"]["op"] for r in rows] == ["t", "c"] and rows[0]["key"] is None
                   and len(list(self.control.glob(scenario + "-w1-*.start.json"))) == 1,
                   "control/resumed-B.json")

    def exercise_duplicates(self):
        scenario = "normalduplicates"
        worker = self.start_worker(scenario, "w1")
        initial = self.create_normal(worker, scenario)
        self.generation(scenario, "w1")
        final = self.completed(worker, scenario, 631)
        rows = self.records(worker, "test.DCOC1107.A", "duplicates-A")
        self.check("300 equal-key tombstones survive actual EOS callbacks and multiple 256-record acknowledgement barriers",
                   len(rows) == 900 and [None if r["value"] is None else r["value"]["op"] for r in rows] == ["c", "d", None] * 300
                   and all(r["key"] == {"ID": 8} for r in rows), "control/duplicates-A.json")
        self.check("durable W advances only after the complete repeated-delete transaction",
                   final["completed_through.sequence"] == "631" and initial["completed_through.sequence"] == "29",
                   "control/normalduplicates-wait-631-offsets-read_committed.json")
        self.request(worker, scenario, "PUT", "/stop")
        base.wait_for("stopped duplicate task", lambda: self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        self.request(worker, scenario, "PUT", "/resume")
        base.wait_for("duplicate task restarted", lambda: len(list(self.control.glob(scenario + "-w1-*.recovered.json"))) == 2)
        self.check("restart from completed W emits no duplicate transaction",
                   self.records(worker, "test.DCOC1107.A", "duplicates-restarted-A") == rows
                   and self.checkpoint(worker, scenario, "duplicates-restarted") == final,
                   "control/duplicates-restarted-A.json")


def main():
    if len(sys.argv) not in (2, 3) or len(sys.argv) == 3 and sys.argv[2] not in ("restart", "pause", "duplicates"):
        raise SystemExit("Usage: dcg-normal-task-probe.py NEW_EVIDENCE_DIRECTORY [restart|pause|duplicates] (compile tests/classpath first)")
    experiment = NormalTaskExperiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        if len(sys.argv) == 3 and sys.argv[2] == "pause":
            experiment.exercise_pause()
        elif len(sys.argv) == 3 and sys.argv[2] == "duplicates":
            experiment.exercise_duplicates()
        else:
            experiment.exercise()
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        # Independent broker evidence is retained even when a safety assertion fails.
        for worker in reversed(experiment.containers):
            scenario = "normalduplicates" if len(sys.argv) == 3 and sys.argv[2] == "duplicates" else "normalresume"
            if "-" + scenario + "-" in worker:
                try:
                    for topic in ["test.DCOC1107.A", "test.DCOC1107.B", "offsets-" + scenario, "bindings-" + scenario]:
                        experiment.records(worker, topic, "failure-" + topic)
                    break
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
