#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded real IBM i + local Connect experiment. No production connector mode."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import threading
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent

def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / file)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

local = module("local_initialization", "dcg-initialization-probe.py")
fixture_module = module("native_fixture", "dcg-native-fixture.py")
base = local.base
CLASS = "io.debezium.connector.db2as400.As400NativeConnectProbeConnector"
local.ownership.CLASS = CLASS


class NativeExperiment(local.InitializationExperiment):
    def __init__(self, directory, library):
        super().__init__(directory)
        self.fixture = fixture_module.NativeFixture(self.directory / "native", library)
        self.library = library
        self.egress = self.prefix + "-native"
        self.children = []
        self.logs = {}
        self.log_threads = []
        self.native_transactions = []
        addresses = {r[4][0] for r in socket.getaddrinfo(self.fixture.target.host, None, socket.AF_INET, socket.SOCK_STREAM)}
        if len(addresses) != 1:
            raise RuntimeError("A single verified IBM i IPv4 address is required for container-local private DNS")
        self.native_host = self.fixture.target.host + ":" + addresses.pop()
        self.report.update(scope="Real IBM i metadata/snapshot/raw journal and actual Kafka Connect task, dispatcher, broker output/history/offsets; owned fixture only",
                           library=library, source_connected=False, production_streaming_enabled=False)
        self.save()

    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400NativeConnectProbeConnector", "As400NativeConnectSource", "As400NativeConnectTransactions", "As400NativeInitializationProbe"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        # Use only supervised stdout: the image's default file appender both
        # fails under the unprivileged UID and bypasses host-side redaction.
        (self.plugin / "native-console.properties").write_text("\n".join([
            "status=warn", "name=NativeProbeConsole", "appender.console.type=Console",
            "appender.console.name=Console", "appender.console.layout.type=PatternLayout",
            "appender.console.layout.pattern=%d{ISO8601} %-5p %c - %m%n",
            "rootLogger.level=info", "rootLogger.appenderRef.console.ref=Console", ""]))
        files = [ROOT / "tools" / n for n in ["dcg-native-connect-probe.py", "dcg-native-fixture.py", "dcg-native-connect-probe"]]
        files += list((ROOT / "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400").glob("As400NativeConnect*.java"))
        self.report["harness_sha256"].update({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["scope"] = "Real native source and Kafka Connect; controller owns all fresh-library writers/DDL/maintenance and receiver retention"
        self.report["initialization"] = "Actual native snapshot and binding acquisition; Connect committed acknowledgement and offset readback precede reader handoff"
        self.save()

    def start_broker(self):
        super().start_broker()
        self.command("docker", "network", "create", "--label", "dcg.local.probe=" + self.prefix, self.egress)

    def java(self, container, arguments, label):
        path = self.directory / (label + ".native.log")
        process = subprocess.Popen(["docker", "exec", "-i", container, "java", "-Xms128m", "-Xmx512m",
                                   "-javaagent:/probe/" + self.agent,
                                   "-Dlog4j2.configurationFile=/probe/native-console.properties",
                                   "-cp", "/probe/*:/opt/kafka/libs/*", *arguments],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.children.append(process)
        process.stdin.write(self.fixture.properties())
        process.stdin.close()
        def collect():
            with path.open("w") as stream:
                for line in process.stdout:
                    stream.write(self.fixture.redact(line))
                    stream.flush()
        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        self.log_threads.append(thread)
        return process, path

    def container(self, name, args):
        config = self.directory / (name + ".properties")
        if config.exists():
            with config.open("a") as stream:
                stream.write("\n".join(k + "=" + v for k, v in self.report["worker_overrides"].items()) + "\n")
            worker = base.Experiment.container(self, name, ["--user", f"{os.getuid()}:{os.getgid()}",
                        "--add-host", self.native_host,
                        "-v", f"{self.plugin}:/probe:ro", "-v", f"{self.control}:/control", "-v", f"{config}:/worker.properties:ro",
                        "-e", "PROBE_WORKER=" + name, "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 2400"])
            self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, worker)
            process, path = self.java(worker, [CLASS, "worker", "/worker.properties"], name)
            self.logs[worker] = path
            return worker
        return base.Experiment.container(self, name, (["--add-host", self.native_host] if name == "controller" else []) + args)

    def command(self, *args, **kwargs):
        # Native workers run in a supervised docker-exec JVM to keep credentials
        # exclusively on stdin. Their sanitized logs are still authoritative.
        if len(args) >= 3 and args[:2] == ("docker", "logs") and args[-1] in getattr(self, "logs", {}):
            path = self.logs[args[-1]]
            return path.read_text() if path.exists() else ""
        return super().command(*args, **kwargs)

    def marker(self, generation, suffix):
        def found():
            path = self.control / (generation + "." + suffix + ".json")
            if path.exists():
                return path
            failed = self.control / (generation + ".failed.json")
            if suffix != "failed" and failed.exists():
                raise RuntimeError("Native task failed awaiting " + suffix + ": " + failed.read_text())
        return base.wait_for(suffix, found, 240)

    def create(self, worker, scenario):
        self.report["running_scenario"] = scenario
        self.save()
        (self.control / (scenario + ".qualified")).write_text("native-" + self.library)
        (self.control / (scenario + ".hold-checkpoint-durable")).touch()
        for topic in ["events-" + scenario + s for s in ["", "-A", "-B", "-M"]] + ["history-" + scenario]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                         "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")
        self.rest(worker, "/connectors", {"name": scenario, "config": {
            "connector.class": CLASS, "name": scenario, "tasks.max": "1", "probe.topic": "events-" + scenario, "probe.library": self.library,
            "exactly.once.support": "requested", "transaction.boundary": "poll", "errors.tolerance": "none", "topic.prefix": "test",
            "database.hostname": "stdin-native-credentials.invalid", "database.user": "stdin", "database.password": "stdin",
            "database.dbname": self.fixture.server_name, "database.schema": self.library, "skipped.operations": "none", "max.batch.size": "1",
            "max.queue.size": "64", "poll.interval.ms": "10", "heartbeat.interval.ms": "3600000",
            "event.converting.failure.handling.mode": "fail", "provide.transaction.metadata": "true",
            "producer.override.interceptor.classes": base.CLASS + "$BlockingInterceptor"}})

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        if offsets:
            return base.Experiment.snapshot(self, worker, scenario, label, offsets=True, isolation=isolation)
        rows = base.Experiment.snapshot(self, worker, scenario, label, isolation=isolation)
        for table in ["A", "B", "M"]:
            items = base.Experiment.snapshot(self, worker, scenario + "-" + table, label, isolation=isolation)
            for row in items:
                row["fixture_table"] = table
            rows.extend(items)
        (self.control / (label + "-combined-" + isolation + ".json")).write_text(json.dumps(rows, indent=2))
        return rows

    @staticmethod
    def data(rows, table):
        return [(r["value"]["op"], None if r["value"]["after"] is None else r["value"]["after"]["ID"]) for r in rows
                if r.get("fixture_table") == table and isinstance(r["value"], dict) and "op" in r["value"]]

    def cut(self, worker, generation, label):
        starting = self.checkpoint(worker, "native", label + "-before")
        self.signal(generation, "read", label)
        self.marker(generation, label + "-durable")
        evidence = json.loads((self.control / (generation + "." + label + "-durable.json")).read_text())
        checkpoint = self.checkpoint(worker, "native", label)
        self.check(label + ": actual source checkpoints equal independently consumed Kafka offsets", starting == evidence["from"]
                   and checkpoint == evidence["checkpoint"], "control/")
        self.report.setdefault("cuts", []).append({"label": label, "generation": generation, **evidence})
        self.save()
        return checkpoint

    def start_transaction(self, mode):
        directory = self.control / mode
        directory.mkdir()
        (directory / "qualified").write_text("native-" + self.library)
        process, log = self.java(self.controller, ["io.debezium.connector.db2as400.As400NativeConnectTransactions", self.library, "/control/" + mode, mode], mode)
        self.native_transactions.append((mode, process, directory))
        def opened():
            if process.poll() is not None:
                raise RuntimeError("Native transaction stopped before opening: " + log.read_text()[-2000:])
            return (directory / (mode + "-open.json")).exists()
        base.wait_for(mode + " opened with unrelated committed row", opened, 90)
        return process, directory

    def finish_transaction(self, mode, process, directory):
        (directory / (mode + ".finish")).touch()
        assert process.wait(timeout=45) == 0, mode
        assert (directory / (mode + "-terminal.json")).exists()
        if mode.startswith("delete"):
            self.check(mode + ": verified job-local fast-delete override was discarded", (directory / "fast-delete-profile.json").exists()
                       and json.loads((directory / "fast-delete-profile-discarded.json").read_text())["discarded"], "control/" + mode)

    def raw_clear(self, generation, label, subtype):
        raw = json.loads((self.control / generation / (label + "-raw.json")).read_text())
        clears = [r for r in raw if r["code"] == "D" and r["type"] == "CG"]
        cut = next(c for c in self.report["cuts"] if c["label"] == label and c["generation"] == generation)
        before = cut["from"]
        # F can conservatively retain earlier rollback/lifecycle history below W.
        # Require exactly one NEW clear beyond the independently verified incoming
        # W; preserve all replayed entries and their strict validation. This small
        # fixture must stay on its configured receiver; rotation needs its own case.
        assert all(r["receiver"] == before["completed_through.receiver"] for r in clears)
        new_clears = [r for r in clears if int(r["sequence"]) > int(before["completed_through.sequence"])]
        self.check(label + ": new raw native D/CG captured and strict replay accepted", len(new_clears) == 1
                   and new_clears[0].get("subtype") == str(subtype) and new_clears[0].get("file") == "B", "control/" + generation + "/" + label + "-raw.json")
        self.report.setdefault("clear_cases", []).append({"label": label, "expected_subtype": subtype,
                     "incoming_completed_through": before["completed_through.sequence"], "raw_sequences": [r["sequence"] for r in clears],
                     "new_clear_sequences": [r["sequence"] for r in new_clears]})
        self.save()

    def run_native(self):
        self.fixture.setup()
        self.report["source_connected"] = True
        self.save()
        self.controller = self.container("controller", ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.plugin}:/probe:ro",
                    "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 2400"])
        self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, self.controller)
        scenario = "native"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        first = self.generation(scenario, "w1")
        self.signal(first, "ack-block", 9)
        self.signal(first, "permit", 10)
        self.marker(first, "ack-blocked")
        pending = self.checkpoint(worker, scenario, "snapshot-pending")
        self.check("native snapshot producer ack is insufficient for history/F/W", pending.get("initialization.phase") == "snapshot"
                   and not self.history(worker, scenario, "snapshot-pending") and not (self.control / (first + ".handoff.json")).exists(), "control/")
        rows = self.snapshot(worker, scenario, "snapshot-pending")
        self.check("actual native snapshot contains all rows in three tables", all(self.data(rows, t) == [("r", i) for i in [1, 2, 3]] for t in ["A", "B", "M"]), "control/")
        self.signal(first, "ack-release")
        self.marker(first, "history-durable")
        self.check("native binding history is durable before initial F/W", len(self.history(worker, scenario, "history-durable")) == 1
                   and self.checkpoint(worker, scenario, "history-durable").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(first, "ack-block", 10)
        (self.control / (first + ".ack-release")).unlink()
        (self.control / (first + ".ack-blocked.json")).unlink()
        self.signal(first, "permit", 11)
        self.marker(first, "ack-blocked")
        self.check("held initial offset acknowledgement prevents native reader handoff", not (self.control / (first + ".handoff.json")).exists()
                   and self.checkpoint(worker, scenario, "initial-pending").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(first, "ack-release")
        self.marker(first, "checkpoint-durable")
        initial = self.checkpoint(worker, scenario, "initial")
        boundary = json.loads((self.control / first / "native-boundary.json").read_text())
        self.check("native initial F=W equals verified journal boundary exclusively", initial["replay_from.sequence"] == boundary["sequence"]
                   and initial["completed_through.sequence"] == boundary["sequence"] and initial["replay_from.inclusive"] == "false", "control/")
        self.stop(worker)
        worker = self.start_worker(scenario, "w2")
        generation = self.generation(scenario, "w2")
        self.marker(generation, "session-released")
        self.check("crash after durable initialization resumes without another snapshot", json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == initial
                   and not (self.control / (generation + ".snapshot-prepared.json")).exists(), "control/")
        self.signal(generation, "permit", 100)
        process, directory = self.start_transaction("truncate-commit")
        opened = self.cut(worker, generation, "truncate-open")
        self.raw_clear(generation, "truncate-open", 8)
        rows = self.snapshot(worker, scenario, "truncate-open")
        self.check("unrelated A:8 reaches Kafka while native TRUNCATE stays open and B stays hidden", self.data(rows, "A") == [("r", i) for i in [1, 2, 3]] + [("c", 8)]
                   and self.data(rows, "B") == [("r", i) for i in [1, 2, 3]] and int(opened["replay_from.sequence"]) < int(opened["completed_through.sequence"])
                   and opened["replay_from.inclusive"] == "true" and process.poll() is None, "control/")
        self.stop(worker)
        worker = self.start_worker(scenario, "w3")
        generation = self.generation(scenario, "w3")
        self.marker(generation, "session-released")
        self.signal(generation, "permit", 100)
        self.fixture.sql(f"INSERT INTO {self.library}.A VALUES (81)", "independent-after-restart.log", True)
        continued = self.cut(worker, generation, "truncate-resumed")
        rows = self.snapshot(worker, scenario, "truncate-resumed")
        self.check("replacement rebuilds open transaction and keeps unrelated CDC progressing", self.data(rows, "A") == [("r", i) for i in [1, 2, 3]] + [("c", 8), ("c", 81)]
                   and self.data(rows, "B") == [("r", i) for i in [1, 2, 3]] and continued["replay_from.sequence"] == opened["replay_from.sequence"]
                   and int(continued["completed_through.sequence"]) > int(opened["completed_through.sequence"]) and process.poll() is None, "control/")
        self.finish_transaction("truncate-commit", process, directory)
        indices = [e["fields"]["value"]["index"] for e in self.events(generation) if e["event"] == "producerAck"]
        self.signal(generation, "send-block", max(indices) + 2)  # BEGIN, then truncate
        self.signal(generation, "read", "truncate-committed")
        self.marker(generation, "send-blocked")
        blocked = json.loads((self.control / (generation + ".send-blocked.json")).read_text())
        self.check("reassignment fault is held at the actual native truncate send", blocked["op"] == "t", "control/")
        accepted = self.checkpoint(worker, scenario, "before-fence")
        self.command("docker", "pause", worker)
        replacement = self.start_worker(scenario, "w4")
        fresh = self.generation(scenario, "w4")
        self.marker(fresh, "session-released")
        self.signal(fresh, "permit", 100)
        committed = self.cut(replacement, fresh, "commit-reassigned")
        before = self.snapshot(replacement, scenario, "commit-reassigned")
        self.check("replacement uses durable F/W and emits committed truncate/reload in order", json.loads((self.control / (fresh + ".start.json")).read_text())["loaded"] == accepted
                   and self.data(before, "B") == [("r", i) for i in [1, 2, 3]] + [("t", None)] + [("c", i) for i in [100, 101, 102]], "control/")
        self.signal(generation, "send-release")
        self.command("docker", "unpause", worker)
        self.marker(generation, "send-released")
        base.wait_for("old native task producer is broker-fenced", lambda: re.search(r"InvalidProducerEpochException|ProducerFencedException", self.command("docker", "logs", worker)), 90)
        self.check("stale native task cannot add output or regress source offsets", self.snapshot(replacement, scenario, "after-fence") == before
                   and self.checkpoint(replacement, scenario, "after-fence") == committed, "control/; native-w3.native.log")
        self.stop(worker)
        # Returning/removing the old worker may legitimately restart w4's task.
        # Stop through Connect and resume a fresh, observed generation before
        # issuing the next native cut; never signal a retired generation.
        self.stop_task(replacement, scenario)
        retired = [json.loads(p.read_text())["generation"] for p in self.control.glob(scenario + "-w4-*.start.json")]
        self.request(replacement, scenario, "PUT", "/resume")
        generation = self.generation(scenario, "w4", exclude=retired)
        self.marker(generation, "session-released")
        self.check("post-rebalance generation resumes the same durable checkpoint", json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == committed,
                   "control/")
        self.signal(generation, "permit", 100)
        worker = replacement
        expected_b = [("r", i) for i in [1, 2, 3]] + [("t", None)] + [("c", i) for i in [100, 101, 102]]
        for mode, subtype, aid in [("delete-rollback", 3, 9), ("truncate-rollback", 8, 10), ("delete-commit", 3, 11)]:
            process, directory = self.start_transaction(mode)
            checkpoint = self.cut(worker, generation, mode + "-open")
            self.raw_clear(generation, mode + "-open", subtype)
            rows = self.snapshot(worker, scenario, mode + "-open")
            self.check(mode + ": unrelated row progresses before terminal and clear remains withheld", ("c", aid) in self.data(rows, "A")
                       and self.data(rows, "B") == expected_b and checkpoint["replay_from.inclusive"] == "true" and process.poll() is None, "control/")
            self.finish_transaction(mode, process, directory)
            self.cut(worker, generation, mode + "-terminal")
            if mode == "delete-commit":
                expected_b += [("t", None)] + [("c", i) for i in [400, 401, 402]]
            rows = self.snapshot(worker, scenario, mode + "-terminal")
            self.check(mode + ": committed output follows terminal outcome and rollback data is absent", self.data(rows, "B") == expected_b, "control/")
        self.fixture.verify_rows({"A": [1, 2, 3, 8, 9, 10, 11, 81], "B": [400, 401, 402], "M": [1, 2, 3], "U": [1, 2, 3]})
        self.check("native final rows match the Kafka table projections", self.data(rows, "A") == [("r", i) for i in [1, 2, 3]] + [("c", i) for i in [8, 81, 9, 10, 11]]
                   and self.data(rows, "M") == [("r", i) for i in [1, 2, 3]], "native/final-*.json; control/")
        self.stop_task(worker, scenario)
        self.marker(generation, "drained")
        self.stop(worker)
        self.check("native initializer/reader drained separately from task stop request", any(e["event"] == "stopRequested" for e in self.events(generation))
                   and json.loads((self.control / (generation + ".drained.json")).read_text())["stopped"], "control/")
        self.check("all native experiment fault hooks stayed within bounds", not list(self.control.glob("*.hook-timeout.json")), "control/")
        self.check("native publication used no custom owner/receipt gates", not list(self.control.glob("*.registry*"))
                   and not list(self.control.glob("*.authorized-worker*")), "control/")
        self.report["status"] = "passed"

    def cleanup(self):
        errors = []
        for mode, process, directory in self.native_transactions:
            if process.poll() is None:
                (directory / (mode + ".abort")).touch()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    errors.append("Transaction controller required container termination: " + mode)
        for name in reversed(self.containers):
            try:
                self.command("docker", "unpause", name, check=False)
                (self.directory / (name + ".log")).write_text(self.command("docker", "logs", name, check=False))
                self.command("docker", "rm", "-f", name, check=False)
            except Exception as error:
                errors.append(str(error))
        for process in self.children:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                errors.append("docker-exec child did not terminate after owned container removal")
        for thread in self.log_threads:
            thread.join(timeout=5)
        for network in [self.egress, self.network]:
            self.command("docker", "network", "rm", network, check=False)
        if self.command("docker", "ps", "-aq", "--filter", "label=dcg.local.probe=" + self.prefix) or self.command("docker", "network", "ls", "-q", "--filter", "label=dcg.local.probe=" + self.prefix):
            errors.append("Owned local resources remain")
        self.report["cleanup"] = {"status": "passed" if not errors else "failed", "errors": errors}
        try:
            self.report["native_cleanup"] = self.fixture.cleanup()
        except BaseException as error:
            self.report["native_cleanup"] = {"status": "blocked", "error": str(error), "library": self.library}
        if errors or self.report["native_cleanup"]["status"] not in ["passed", "not-created"]:
            self.report["status"] = "failed"
        self.save()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-native-connect-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd (explicit IBM i writes; local Kafka)")
    experiment = NativeExperiment(Path(sys.argv[1]), sys.argv[2])
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.run_native()
    except BaseException as error:
        experiment.report.update(status="failed", error=experiment.fixture.redact(str(error)))
        experiment.save()
        if experiment.report.get("running_scenario"):
            try:
                experiment.capture_failure()
            except Exception as diagnostic_error:
                experiment.report["failure_evidence_error"] = str(diagnostic_error)
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed", experiment.report.get("error", "Cleanup did not pass")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
