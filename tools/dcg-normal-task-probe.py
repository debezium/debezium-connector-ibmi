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
    compatibility = False
    source_offsets = False
    slow_offsets = False
    standalone = False
    routing = False
    transforms = False

    def destination(self, topic):
        return "routed." + topic if self.routing and topic.startswith(("test.", "__debezium-heartbeat.", "__debezium-truncate.")) else topic

    def stage(self):
        super().stage()
        jar_path = self.plugin / "synthetic-probe.jar"
        temporary = jar_path.with_suffix(".tmp")
        with zipfile.ZipFile(jar_path) as source, zipfile.ZipFile(temporary, "w") as target:
            for item in source.infolist():
                target.writestr(item, source.read(item.filename))
            classes = ROOT / "debezium-connector-ibmi/target/test-classes"
            for pattern in ["As400NormalTaskProbeConnector*.class", "As400RecoveryTaskFixture*.class", "As400RecoveryTestSupport*.class", "As400PackagedProbeAudit*.class"]:
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
        if self.compatibility:
            self.report.update(topics="Three-partition data topics, single-partition control/history; no cross-partition ordering claim",
                               heartbeat_ms=0, tombstones=False, transaction_metadata=False)
        if self.source_offsets:
            self.report.update(publication="At-least-once source-offset readback; Kafka source transactions disabled; no broker fencing claim")
            if self.slow_offsets:
                self.report["offset_flush_interval_ms"] = 60000
        if self.standalone:
            self.report.update(runtime="Kafka Connect standalone; actual WorkerSourceTask and file offsets",
                               ownership="Sequential process replacement only; no automatic reassignment or broker fencing claim")
        if self.routing:
            self.report["routing"] = "Real Connect RegexRouter on data, transaction and heartbeat records; history and offset topics unchanged"
        if self.transforms:
            self.report["transforms"] = "Real ExtractNewRecordState plus tombstone predicate Filter; standard filtering with recovery markers preserved"
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["harness_sha256"]["tools/dcg-normal-task-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        if name != "broker":
            path = self.directory / (name + ".properties")
            if self.standalone:
                # Reuse the existing controlled source and broker assertions with
                # a genuinely different herder and offset backend.
                config = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
                for key in list(config):
                    if key == "group.id" or key.startswith(("config.storage.", "status.storage.", "offset.storage.", "producer.transaction.")):
                        del config[key]
                config["offset.storage.file.filename"] = "/control/standalone-offsets.dat"
                path.write_text("".join(key + "=" + value + "\n" for key, value in config.items()))
                args = ["/opt/kafka/bin/connect-standalone.sh" if arg == "/opt/kafka/bin/connect-distributed.sh" else arg for arg in args]
            with path.open("a") as stream:
                stream.write("request.timeout.ms=5000\ndefault.api.timeout.ms=10000\nworker.sync.timeout.ms=20000\nrebalance.timeout.ms=30000\n")
                if self.source_offsets:
                    stream.write("exactly.once.source.support=disabled\n")
                if self.slow_offsets:
                    stream.write("offset.flush.interval.ms=60000\n")
            args = ["-e", "KAFKA_OPTS=-javaagent:/probe/" + self.agent, *args]
        return super().container(name, args)

    def wait_worker_membership(self, worker, identifier):
        if not self.standalone:
            return super().wait_worker_membership(worker, identifier)
        base.wait_for("standalone herder " + identifier,
                      lambda: "Kafka Connect started" in self.command("docker", "logs", worker))

    def checkpoint(self, worker, scenario, label):
        if self.standalone:
            rows = self.rest(worker, "/connectors/" + scenario + "/offsets")["offsets"]
            (self.control / (scenario + "-" + label + "-file-offsets.json")).write_text(json.dumps(rows, indent=2))
            matches = [row["offset"] for row in rows if row["partition"] == {"server": "test"}]
            return matches[-1] if matches else None
        rows = self.snapshot(worker, scenario, label, offsets=True)
        matches = [r["value"] for r in rows if r["key"] == [scenario, {"server": "test"}]]
        return matches[-1] if matches else None

    def patch_offset(self, worker, scenario, offset):
        self.request(worker, scenario, "PATCH", "/offsets", {"offsets": [{"partition": {"server": "test"}, "offset": offset}]})

    def generation(self, scenario, role):
        path = base.wait_for("normal task start", lambda: next(iter(self.control.glob(scenario + "-" + role + "-*.start.json")), None))
        return json.loads(path.read_text())["generation"]

    def records(self, worker, topic, label):
        topic = self.destination(topic)
        file = label + ".json"
        audit = getattr(self, "audit_class", "io.debezium.connector.db2as400.As400PackagedProbeAudit" if self.compatibility else base.CLASS)
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", audit,
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
        if self.source_offsets:
            config["probe.source.offsets"] = "true"
        if self.standalone:
            config.pop("exactly.once.support")
            config.pop("transaction.boundary")
        if scenario == "normalduplicates":
            config.update({"max.batch.size": "512", "max.queue.size": "2048"})
        if self.compatibility:
            config.update({"heartbeat.interval.ms": "0", "tombstones.on.delete": "false", "provide.transaction.metadata": "false"})
        if self.routing:
            # Exercise Kafka's ConfigDef.STRING trimming as well as topic routing.
            config.update({"transforms": "route", "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                           "transforms.route.regex": " ^(.*)$ ", "transforms.route.replacement": " routed.$1 "})
        if self.transforms:
            config.update({"transforms": "unwrap,dropTombstones", "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                           "transforms.unwrap.add.fields": "op,source.sequence:sequence,source.receiver:receiver",
                           "transforms.dropTombstones.type": "org.apache.kafka.connect.transforms.Filter", "transforms.dropTombstones.predicate": "tombstone",
                           "predicates": "tombstone", "predicates.tombstone.type": "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone"})
        (self.directory / (scenario + "-connector.json")).write_text(json.dumps(config, indent=2))
        for topic in ["test.DCOC1107.A", "test.DCOC1107.B", "test.transaction",
                      "__debezium-truncate.test" if self.compatibility else "__debezium-heartbeat.test"]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create", "--if-not-exists",
                         "--topic", self.destination(topic), "--partitions", "3" if self.compatibility and topic.startswith("test.DCOC1107.") else "1",
                         "--replication-factor", "1")
        self.rest(worker, "/connectors", {"name": scenario, "initial_state": "STOPPED", "config": config})
        base.wait_for("stopped for baseline", lambda: self.rest(worker, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS, "baseline", scenario)
        initial = json.loads((self.control / (scenario + ".baseline.json")).read_text())
        self.patch_offset(worker, scenario, initial)
        self.check("preexisting source F/W and immutable history before task admission", self.checkpoint(worker, scenario, "seed") == initial,
                   "control/" + scenario + "-seed-*.json")
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
        self.check("unrelated A progresses while B clear is open; F remains31 and W42", [(r["value"] if self.transforms else r["value"]["after"])["ID"] for r in a] == [8] and not b and
                   saved["replay_from.sequence"] == "31" and saved["completed_through.sequence"] == "42" and "scan.sequence" not in saved, "control/open-*.json")
        # Hold the old reader after durable acknowledgement. Connect's own restart
        # moves the task to the replacement; no custom owner/receipt gate is present.
        self.signal(first, "hold")
        self.marker(first, "held")
        old_deliveries = self.deliveries(first)
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
        same_checkpoint = after == final
        if self.source_offsets:
            fields = {"delivery.version", "delivery.session", "delivery.sequence", "delivery.receipt"}
            same_checkpoint = {k: v for k, v in after.items() if k not in fields} == {k: v for k, v in final.items() if k not in fields}
            # A returning worker can legitimately receive a NEW task and emit an
            # idle heartbeat. Do not mistake its fresh delivery session for F/W
            # movement, or excuse a late publication by the stopped task.
            legitimate = after == final
            for path in self.control.glob(scenario + "-*.start.json"):
                start = json.loads(path.read_text())
                generation = start["generation"]
                if generation != first and start["loaded"] == final:
                    legitimate |= any(event["fields"]["topic"] == "__debezium-heartbeat.test" and event["fields"]["offset"] == after
                                      for event in self.deliveries(generation))
            offsets = self.snapshot(replacement, scenario, "return-audit", offsets=True)
            values = [row["value"] for row in offsets if row["key"] == [scenario, {"server": "test"}]]
            reached = values.index(final)
            self.check("source-offset return belongs to a fresh task; stopped generation publishes no further delivery or offset",
                       legitimate and self.deliveries(first) == old_deliveries
                       and all(value.get("delivery.session") != saved["delivery.session"] for value in values[reached + 1:]),
                       "control/*start.json; control/*.jsonl; control/normalresume-return-audit-offsets-read_committed.json")
        ordered_a = sorted(a, key=lambda r: r["value"]["after"]["ID"]) if self.compatibility else a
        ordered_b = sorted(b, key=lambda r: r["value"]["op"] != "t") if self.compatibility else b
        if self.transforms:
            self.check("configured unwrap filters truncate while committed reload and unrelated rows survive restart",
                       [r["value"]["ID"] for r in a] == [8, 9] and [(r["value"]["__op"], r["value"]["ID"]) for r in b] == [("c", 100)]
                       and same_checkpoint, "control/final-*.json")
            self.check("standard unwrap preserves keys and requested native source fields", b[0]["key"] == {"ID": 100}
                       and b[0]["value"]["__receiver"] == "QSQJRN0003" and int(b[0]["value"]["__sequence"]) > 29, "control/final-B.json")
            self.check("recovery markers reach Kafka and advance the durable source checkpoint despite filtered truncate",
                       bool(self.records(replacement, "__debezium-heartbeat.test", "checkpoint-heartbeats")) and same_checkpoint, "control/checkpoint-heartbeats.json")
        else:
            self.check("late old read adds no output or checkpoint; rollback stays invisible", [r["value"]["after"]["ID"] for r in ordered_a] == [8, 9] and
                       [r["value"]["op"] for r in ordered_b] == ["t", "c"] and ordered_b[0]["key"] is None and ordered_b[1]["value"]["after"]["ID"] == 100 and same_checkpoint,
                       "control/final-*.json")
            self.check("real schema, key and source metadata survive normal dispatch", ordered_b[1]["key"] == {"ID": 100} and
                       ordered_b[1]["value"]["source"]["receiver"] == "QSQJRN0003" and int(ordered_b[0]["value"]["source"]["sequence"]) > 29, "control/final-B.json")
        if self.routing:
            topics = self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list").splitlines()
            self.check("broker contains routed output only; original output destinations were never created",
                       all("routed." + name in topics and name not in topics for name in
                           ["test.DCOC1107.A", "test.DCOC1107.B", "test.transaction", "__debezium-heartbeat.test"]),
                       "commands.log; control/final-*.json")
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

    def deliveries(self, generation):
        path = self.control / (generation + ".jsonl")
        return [event for line in path.read_text().splitlines() if (event := json.loads(line))["event"] == "commitRecord"]

    def exercise_offset_lag(self):
        scenario = "normalresume"
        worker = self.start_worker(scenario, "w1")
        initial = self.create_normal(worker, scenario)
        first = self.generation(scenario, "w1")
        started = time.monotonic()
        base.wait_for("first table before offset flush", lambda: len(self.records(worker, "test.DCOC1107.A", "lag-first-A")) == 1, seconds=20)
        self.signal(scenario, "tail", 44)
        base.wait_for("truncate and reload before offset flush", lambda: len(self.records(worker, "test.DCOC1107.B", "lag-B")) == 2, seconds=20)
        self.signal(scenario, "tail", 64)
        base.wait_for("next unrelated row before offset flush", lambda: len(self.records(worker, "test.DCOC1107.A", "lag-A")) == 2, seconds=20)
        before_crash = self.checkpoint(worker, scenario, "lag-unflushed")
        elapsed = time.monotonic() - started
        self.report["progress_before_flush_seconds"] = elapsed
        self.check("multiple cuts progress before the 60-second flush without advancing saved F/W", elapsed < 60 and before_crash == initial,
                   "control/normalresume-lag-unflushed-offsets-read_committed.json; control/lag-A.json; control/lag-B.json")
        self.command("docker", "kill", "--signal", "KILL", worker)
        replacement = self.start_worker(scenario, "w2")
        if self.standalone:
            config = json.loads((self.directory / (scenario + "-connector.json")).read_text())
            self.rest(replacement, "/connectors", {"name": scenario, "config": config})
        second = self.generation(scenario, "w2")
        base.wait_for("replay after process death before flush", lambda: len(self.records(replacement, "test.DCOC1107.A", "lag-replay-A")) == 4,
                      seconds=30)
        self.request(replacement, scenario, "PUT", "/stop")
        base.wait_for("stopped after replay", lambda: self.rest(replacement, "/connectors/" + scenario + "/status")["connector"]["state"] == "STOPPED")
        a = self.records(replacement, "test.DCOC1107.A", "lag-final-A")
        b = self.records(replacement, "test.DCOC1107.B", "lag-final-B")
        final = self.checkpoint(replacement, scenario, "lag-final")
        self.check("fresh task uses saved offsets, replaying the unflushed source suffix in order",
                   json.loads((self.control / (second + ".start.json")).read_text())["loaded"] == initial
                   and [row["value"]["after"]["ID"] for row in a] == [8, 9, 8, 9]
                   and [row["value"]["op"] for row in b] == ["t", "c", "t", "c"]
                   and [row["value"]["after"]["ID"] for row in b if row["value"]["op"] == "c"] == [100, 100]
                   and final["completed_through.sequence"] == "59" and "scan.sequence" not in final,
                   "control/lag-final-*.json; control/normalresume-lag-final-offsets-read_committed.json")
        self.check("crashed generation does not supply restart or checkpoint authority", first != second and
                   not list(self.control.glob("*.registry*")) and not list(self.control.glob("*.authorized-worker*")), "control/")
        if self.standalone:
            self.command("docker", "exec", replacement, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS, "file-offset", scenario)
            self.check("independent fresh file-store reader confirms the stopped task's durable F/W",
                       json.loads((self.control / (scenario + ".file-offset.json")).read_text()) == final,
                       "control/" + scenario + ".file-offset.json")
            self.patch_offset(replacement, scenario, None)
            starts = set(self.control.glob(scenario + "-*.start.json"))
            self.request(replacement, scenario, "PUT", "/resume")
            failed = base.wait_for("standalone missing-offset rejection", lambda: next((
                task for task in self.rest(replacement, "/connectors/" + scenario + "/status")["tasks"] if task["state"] == "FAILED"), None))
            new_starts = set(self.control.glob(scenario + "-*.start.json")) - starts
            self.check("standalone missing offsets cannot bootstrap or publish with retained history",
                       "Missing durable F/W: explicit initialization is required" in failed.get("trace", "")
                       and bool(new_starts) and all(json.loads(path.read_text())["loaded"] == {}
                                                   and not path.with_name(path.name.replace(".start.json", ".recovered.json")).exists() for path in new_starts)
                       and self.checkpoint(replacement, scenario, "missing") is None
                       and self.records(replacement, "test.DCOC1107.A", "missing-A") == a
                       and self.records(replacement, "test.DCOC1107.B", "missing-B") == b,
                       "control/normalresume-missing-file-offsets.json; control/missing-*.json")

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
    if len(sys.argv) not in (2, 3) or len(sys.argv) == 3 and sys.argv[2] not in ("restart", "pause", "duplicates", "compatibility", "source-offsets", "source-offsets-lag", "standalone", "routing", "routing-source-offsets", "transforms", "transforms-source-offsets"):
        raise SystemExit("Usage: dcg-normal-task-probe.py NEW_EVIDENCE_DIRECTORY [restart|pause|duplicates|compatibility|source-offsets|source-offsets-lag|standalone|routing|routing-source-offsets|transforms|transforms-source-offsets] (compile tests/classpath first)")
    experiment = NormalTaskExperiment(Path(sys.argv[1]))
    experiment.compatibility = len(sys.argv) == 3 and sys.argv[2] == "compatibility"
    experiment.source_offsets = len(sys.argv) == 3 and sys.argv[2] in ("source-offsets", "source-offsets-lag", "standalone", "routing-source-offsets", "transforms-source-offsets")
    experiment.slow_offsets = len(sys.argv) == 3 and sys.argv[2] in ("source-offsets-lag", "standalone")
    experiment.standalone = len(sys.argv) == 3 and sys.argv[2] == "standalone"
    experiment.routing = len(sys.argv) == 3 and sys.argv[2] in ("routing", "routing-source-offsets")
    experiment.transforms = len(sys.argv) == 3 and sys.argv[2] in ("transforms", "transforms-source-offsets")
    try:
        experiment.stage()
        experiment.start_broker()
        if experiment.slow_offsets:
            experiment.exercise_offset_lag()
        elif len(sys.argv) == 3 and sys.argv[2] == "pause":
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
                    topics = ["test.DCOC1107.A", "test.DCOC1107.B", "bindings-" + scenario]
                    if experiment.standalone:
                        experiment.checkpoint(worker, scenario, "failure")
                    else:
                        topics.append("offsets-" + scenario)
                    for topic in topics:
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
