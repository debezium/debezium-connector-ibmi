#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Local Connect/native initialization integration; controlled source, real Kafka."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("ownership", ROOT / "tools/dcg-ownership-probe.py")
ownership = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ownership)
base = ownership.base
CLASS = "io.debezium.connector.db2as400.As400ConnectInitializationProbeConnector"
ownership.CLASS = CLASS


class InitializationExperiment(ownership.OwnershipExperiment):
    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400ConnectInitializationProbeConnector", "As400InitializationAcknowledgements",
                         "As400NativeInitializationTest", "As400RecoveryTestSupport", "As400IncrementalLifecycleTest"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
            path = classes / "native-transactions/open-clear-progress.json"
            jar.write(path, str(path.relative_to(classes)))
        for item in (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":"):
            if any(x in item for x in ["/org/mockito/", "/net/bytebuddy/", "/org/objenesis/", "/org/assertj/",
                                        "/org/junit/", "/org/opentest4j/", "/org/apiguardian/"]):
                path = Path(item)
                shutil.copy2(path, self.plugin / path.name)
        self.agent = next(self.plugin.glob("byte-buddy-agent-*.jar")).name
        self.report["scope"] = "Actual native initialization adapter, task poll/commit/stop, strict captured replay and dispatcher; mocked native acquisition and qualified session; real Kafka history/output/offsets"
        self.report["worker_overrides"] = {"request.timeout.ms": "5000", "default.api.timeout.ms": "10000",
                                           "worker.sync.timeout.ms": "20000", "rebalance.timeout.ms": "30000"}
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        files = [Path(__file__)] + list((ROOT / "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400").glob("As400*Initialization*.java"))
        self.report["harness_sha256"].update({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
        self.save()

    def container(self, name, args):
        config = self.directory / (name + ".properties")
        if config.exists():
            with config.open("a") as stream:
                stream.write("\n".join(k + "=" + v for k, v in self.report["worker_overrides"].items()) + "\n")
            args = ["-e", "KAFKA_OPTS=-javaagent:/probe/" + self.agent, *args]
        return super().container(name, args)

    def create(self, worker, scenario, qualified=True, holds=()):
        self.report["running_scenario"] = scenario
        self.save()
        if qualified:
            (self.control / (scenario + ".qualified")).write_text("controlled-" + scenario)
        for phase in holds:
            (self.control / (scenario + ".hold-" + phase)).touch()
        for topic in ["events-" + scenario, "events-" + scenario + "-A", "events-" + scenario + "-B", "history-" + scenario]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                         "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")
        return self.rest(worker, "/connectors", {"name": scenario, "config": {
            "connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario,
            "exactly.once.support": "requested", "transaction.boundary": "poll", "errors.tolerance": "none",
            "topic.prefix": "test", "database.hostname": "no-live-ibmi.invalid", "database.user": "fixture", "database.password": "fixture",
            "database.dbname": "TEST", "database.schema": "DCOC1107", "skipped.operations": "none", "max.batch.size": "1",
            "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
            "event.converting.failure.handling.mode": "fail", "provide.transaction.metadata": "true",
            "producer.override.interceptor.classes": base.CLASS + "$BlockingInterceptor"}})

    def generation(self, scenario, role, exclude=()):
        def find():
            for path in self.control.glob(scenario + "-" + role + "-*.start.json"):
                result = json.loads(path.read_text())["generation"]
                if result not in exclude:
                    return result
        return base.wait_for("initialization task " + scenario + "-" + role, find)

    def marker(self, generation, suffix):
        def found():
            path = self.control / (generation + "." + suffix + ".json")
            if path.exists():
                return path
            failed = self.control / (generation + ".failed.json")
            if suffix != "failed" and failed.exists():
                raise RuntimeError("Task failed while awaiting " + suffix + ": " + failed.read_text())
        return base.wait_for(suffix, found, 100)

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        rows = base.Experiment.snapshot(self, worker, scenario, label, offsets=offsets, isolation=isolation)
        if not offsets:
            for table in ["A", "B"]:
                table_rows = base.Experiment.snapshot(self, worker, scenario + "-" + table, label, isolation=isolation)
                for row in table_rows:
                    row["fixture_table"] = table
                rows.extend(table_rows)
            (self.control / (scenario + "-" + label + "-combined-" + isolation + ".json")).write_text(json.dumps(rows, indent=2))
        return rows

    def checkpoint(self, worker, scenario, label):
        rows = self.snapshot(worker, scenario, label, offsets=True)
        matches = [r["value"] for r in rows if r["key"] == [scenario, {"server": "test"}]]
        return matches[-1] if matches else None

    def history(self, worker, scenario, label):
        file = scenario + "-" + label + "-history.json"
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", base.CLASS,
                     "broker:9092", "history-" + scenario, "read_committed", "/control/" + file, timeout=35)
        return json.loads((self.control / file).read_text())

    def completed(self, worker, scenario, sequence):
        def find():
            checkpoint = self.checkpoint(worker, scenario, "wait-" + str(sequence))
            return checkpoint if checkpoint and checkpoint.get("completed_through.sequence") == str(sequence) else None
        return base.wait_for("durable W=" + str(sequence), find, 100)

    def assert_data(self, worker, scenario, label):
        rows = self.snapshot(worker, scenario, label)
        data = [r for r in rows if isinstance(r["value"], dict) and "op" in r["value"]]
        operations = [(r["value"]["op"], r["fixture_table"], None if r["value"]["after"] is None else r["value"]["after"]["ID"]) for r in data]
        expected = [("r", "A", i) for i in [1, 2, 3]] + [("c", "A", 8), ("c", "A", 9)] + [("r", "B", i) for i in [1, 2, 3]] + [("t", "B", None), ("c", "B", 100)]
        self.check(scenario + ": snapshot precedes ordered committed CDC; rollback absent", operations == expected, "control/" + scenario + "-" + label + "-records-read_committed.json")
        self.check(scenario + ": real keys and native source sequences survive handoff", all(
            r["key"] is None if r["value"]["op"] == "t" else r["key"]["ID"] == r["value"]["after"]["ID"] for r in data)
            and [r["value"]["source"]["sequence"] for r in data if r["value"]["op"] != "r"] == ["42", "56", "38", "40"], "control/")
        return rows

    def acknowledgements(self):
        scenario = "init-ack"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, holds=["reader"])
        generation = self.generation(scenario, "w1")
        self.signal(generation, "ack-block", 6)
        self.signal(generation, "permit", 7)
        self.marker(generation, "ack-blocked")
        saved = self.checkpoint(worker, scenario, "snapshot-pending")
        self.check("snapshot barrier producer ack does not install history or F/W", saved.get("initialization.phase") == "snapshot"
                   and not self.history(worker, scenario, "snapshot-pending") and not list(self.control.glob(generation + ".handoff.json")), "control/")
        visible = self.snapshot(worker, scenario, "snapshot-pending")
        raw = self.snapshot(worker, scenario, "snapshot-pending", isolation="read_uncommitted")
        self.check("uncommitted baseline barrier is hidden", len(visible) == 6 and len(raw) == 7, "control/")
        self.signal(generation, "ack-release")
        self.marker(generation, "history-durable")
        self.check("baseline commit and immutable history precede initial offset", len(self.history(worker, scenario, "history-durable")) == 1
                   and self.checkpoint(worker, scenario, "history-durable").get("initialization.phase") == "baseline-complete", "control/")
        # Replace the interceptor gate only after the first send has returned.
        self.signal(generation, "ack-block", 7)
        (self.control / (generation + ".ack-release")).unlink()
        (self.control / (generation + ".ack-blocked.json")).unlink()
        self.signal(generation, "permit", 8)
        self.marker(generation, "ack-blocked")
        self.check("uncommitted initial checkpoint cannot admit reader", not (self.control / (generation + ".handoff.json")).exists()
                   and self.checkpoint(worker, scenario, "initial-pending").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(generation, "ack-release")
        self.marker(generation, "handoff")
        initial = self.completed(worker, scenario, 29)
        self.check("handoff uses exact durable exclusive F=W=29", initial["replay_from.sequence"] == "29" and initial["replay_from.inclusive"] == "false", "control/")
        self.signal(generation, "release-reader")
        self.signal(generation, "permit", 100)
        self.completed(worker, scenario, 59)
        before = self.assert_data(worker, scenario, "complete")
        self.marker(generation, "session-released")
        (self.control / (scenario + ".qualified")).unlink()
        self.stop(worker)
        fresh_worker = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.marker(fresh, "handoff")
        self.signal(fresh, "release-reader")
        self.signal(fresh, "permit", 100)
        self.marker(fresh, "prepared")
        self.completed(fresh_worker, scenario, 59)
        after = self.assert_data(fresh_worker, scenario, "restart")
        self.check("fresh task resumes without writer fence, second snapshot or repeated completed data", len(after) == len(before) + 1, "control/")
        self.stop_task(fresh_worker, scenario)
        self.request(fresh_worker, scenario, "PATCH", "/offsets", {"offsets": [{"partition": {"server": "test"}, "offset": None}]})
        self.check("offset removal is durably visible before restart", self.checkpoint(fresh_worker, scenario, "offset-deleted") is None, "control/")
        # Even loss of the fixture enrollment marker cannot bypass broker evidence.
        (self.control / (scenario + ".enrolled")).unlink()
        (self.control / (scenario + ".qualified")).write_text("controlled-" + scenario)
        self.request(fresh_worker, scenario, "PUT", "/resume")
        rejected = self.generation(scenario, "w2", exclude=[fresh])
        self.marker(rejected, "failed")
        self.check("missing offset with enrollment/history rejects without new output", self.snapshot(fresh_worker, scenario, "missing-offset") == after
                   and self.checkpoint(fresh_worker, scenario, "missing-offset") is None
                   and len(self.history(fresh_worker, scenario, "missing-offset")) == 1 and not (self.control / (rejected + ".handoff.json")).exists(), "control/")
        self.stop(fresh_worker)

    def crash(self, phase):
        scenario = "crash-" + phase
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, holds=[phase, "reader"])
        generation = self.generation(scenario, "w1")
        if phase == "partial-snapshot":
            self.signal(generation, "permit", 3)
            base.wait_for("three committed snapshot rows", lambda: len([e for e in self.events(generation) if e["event"] == "commitRecord"]) == 3)
        else:
            self.signal(generation, "permit", 8)
            self.marker(generation, phase)
        before = self.snapshot(worker, scenario, "before-crash")
        saved = self.checkpoint(worker, scenario, "before-crash")
        history = self.history(worker, scenario, "before-crash")
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        if phase == "checkpoint-durable":
            self.marker(fresh, "handoff")
            self.signal(fresh, "release-reader")
            self.signal(fresh, "permit", 100)
            self.completed(replacement, scenario, 59)
            self.assert_data(replacement, scenario, "resumed")
            self.check("crash after initial offset resumes without reinitializing", json.loads((self.control / (fresh + ".start.json")).read_text())["loaded"] == saved
                       and self.history(replacement, scenario, "resumed") == history, "control/")
        else:
            self.marker(fresh, "failed")
            self.check(phase + ": incomplete initialization quarantines replacement with unchanged output/offset/history",
                       self.snapshot(replacement, scenario, "rejected") == before and self.checkpoint(replacement, scenario, "rejected") == saved
                       and self.history(replacement, scenario, "rejected") == history and not (self.control / (fresh + ".handoff.json")).exists(), "control/")
        self.stop(replacement)

    def qualification(self):
        scenario = "unqualified"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, qualified=False)
        generation = self.generation(scenario, "w1")
        self.marker(generation, "failed")
        self.check("missing qualified session rejects before snapshot, history or offset", not self.snapshot(worker, scenario, "rejected")
                   and self.checkpoint(worker, scenario, "rejected") is None and not self.history(worker, scenario, "rejected"), "control/")
        self.stop(worker)

    def stop_and_loss(self):
        for mode in ["stop", "loss"]:
            scenario = "session-" + mode
            worker = self.start_worker(scenario, "w1")
            self.create(worker, scenario, holds=["history-durable"])
            generation = self.generation(scenario, "w1")
            self.signal(generation, "permit", 7)
            self.marker(generation, "history-durable")
            before = self.snapshot(worker, scenario, "before-stop")
            saved = self.checkpoint(worker, scenario, "before-stop")
            if mode == "loss":
                (self.control / (scenario + ".qualified")).unlink()
                self.marker(generation, "failed")
            else:
                self.stop_task(worker, scenario)
            self.marker(generation, "drained")
            self.signal(generation, "release-history-durable")
            self.check(mode + ": no reader or initial checkpoint after cancelled/lost session", self.snapshot(worker, scenario, "after-stop") == before
                       and self.checkpoint(worker, scenario, "after-stop") == saved and not (self.control / (generation + ".handoff.json")).exists(), "control/")
            self.stop(worker)

    def fencing(self):
        scenario = "init-fencing"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario, holds=["reader"])
        old = self.generation(scenario, "w1")
        self.signal(old, "permit", 8)
        self.marker(old, "handoff")
        initial = self.completed(worker, scenario, 29)
        self.signal(old, "send-block", 8)
        self.signal(old, "release-reader")
        self.marker(old, "prepared")
        prepared = json.loads((self.control / (old + ".prepared.json")).read_text())
        self.check("actual scan R=64 and volatile W=59 do not advance durable F/W=29", prepared["R"] == "64"
                   and prepared["checkpoint"]["completed_through.sequence"] == "59"
                   and self.checkpoint(worker, scenario, "scan-ahead") == initial, "control/")
        self.signal(old, "permit", 9)
        self.marker(old, "send-blocked")
        self.command("docker", "pause", worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.marker(fresh, "handoff")
        self.check("legitimate replacement loads durable F/W rather than old scan R=64", json.loads((self.control / (fresh + ".start.json")).read_text())["loaded"] == initial, "control/")
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
        log = base.wait_for("broker rejects stale producer after reader handoff", fenced, 90)
        (self.directory / "initialization-fencing.log").write_text(log)
        self.check("broker fencing prevents stale post-handoff output or offset regression", self.snapshot(replacement, scenario, "after-stale") == before
                   and self.checkpoint(replacement, scenario, "after-stale") == final, "initialization-fencing.log; control/")
        self.stop(worker)
        self.stop(replacement)

    def capture_failure(self):
        scenario = self.report.get("running_scenario")
        if not scenario:
            return
        observer = self.container("observer", ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.plugin}:/probe:ro",
                                               "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 120"])
        self.snapshot(observer, scenario, "failure-independent")
        self.snapshot(observer, scenario, "failure-independent", offsets=True)
        self.history(observer, scenario, "failure-independent")


def main():
    if len(sys.argv) not in [2, 3]:
        raise SystemExit("Usage: dcg-initialization-probe.py NEW_EVIDENCE_DIRECTORY [ack|crash|qualification|stop|fencing]")
    experiment = InitializationExperiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        actions = {"ack": experiment.acknowledgements,
                   "crash": lambda: [experiment.crash(p) for p in ["partial-snapshot", "snapshot-acknowledged", "history-durable", "checkpoint-durable"]],
                   "qualification": experiment.qualification, "stop": experiment.stop_and_loss, "fencing": experiment.fencing}
        for name in ([sys.argv[2]] if len(sys.argv) == 3 else actions):
            actions[name]()
        experiment.check("existing custom owner/receipt gates are absent", not list(experiment.control.glob("*.registry*"))
                         and not list(experiment.control.glob("*.authorized-worker*")), "control/")
        experiment.check("all fault hooks stayed within bounds", not list(experiment.control.glob("*.hook-timeout.json")), "control/")
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        experiment.save()
        try:
            experiment.capture_failure()
        except Exception as diagnostic_error:
            experiment.report["failure_evidence_error"] = str(diagnostic_error)
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Owned resource cleanup failed")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
