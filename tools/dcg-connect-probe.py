#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded, disposable real Kafka/Connect experiment. No IBM i configuration accepted."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile

ROOT = Path(__file__).resolve().parent.parent
IMAGES = {
    "4.2.0": "apache/kafka:4.2.0@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
    "4.2.2": "apache/kafka:4.2.2@sha256:1213eb3943d551e5ed1fca7a4e109001cee35770b66a02a0c37a8964efe09b69",
    "4.3.0": "apache/kafka:4.3.0@sha256:0be4c9eb3565733612d2836d65636fd611a219ebcf5b4162e5ac259ea0ecb907",
    "4.3.1": "apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837",
}
IMAGE = IMAGES[os.environ.get("DCG_KAFKA_VERSION", "4.3.1")]
CLASS = "io.debezium.connector.db2as400.As400ConnectProbeConnector"
EXPECTED = ["BEGIN", "t", "c:1", "c:2", "END", "HB", "BEGIN", "c:3", "END", "HB"]


def wait_for(description, predicate, seconds=100):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.25)
    raise AssertionError("Timed out: " + description)


class RestError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(str(status) + ": " + body)
        self.status = status
        self.body = body


class Experiment:
    test_classes = ROOT / "debezium-connector-ibmi/target/test-classes"

    def __init__(self, directory):
        self.directory = directory.resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.control = self.directory / "control"
        self.control.mkdir()
        self.plugin = self.directory / "plugin"
        self.plugin.mkdir()
        self.prefix = "dcg-probe-" + uuid.uuid4().hex[:10]
        self.network = self.prefix
        self.containers = []
        self.report = {"image": IMAGE, "source_head": self.git("rev-parse", "HEAD"),
                       "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "source_connected": False, "production_streaming_enabled": False,
                       "delivery_contract": "whole-batch at-least-once", "checks": []}
        self.save()

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()

    def command(self, *args, timeout=45, check=True):
        redact = getattr(getattr(self, "fixture", None), "redact", lambda text: text)
        try:
            result = subprocess.run(list(args), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(redact(f"Command timed out after {timeout}s: {args!r}")) from None
        with (self.directory / "commands.log").open("a") as log:
            log.write(redact(json.dumps(list(args)) + "\n" + result.stdout + "\n"))
        if check and result.returncode:
            raise RuntimeError(redact(f"Command failed ({result.returncode}): {args!r}\n{result.stdout[-3000:]}"))
        return result.stdout.strip()

    def save(self):
        (self.directory / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")

    def check(self, name, passed, evidence):
        self.report["checks"].append({"name": name, "status": "passed" if passed else "failed", "evidence": evidence})
        self.save()
        if not passed:
            raise AssertionError(name)
        print("PASS " + name, flush=True)

    def stage(self):
        # Only this test connector and current main classes; never publish this experiment jar.
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "w") as jar:
            for module in ["debezium-connector-ibmi", "journal-parsing", "jt400-override-ccsid"]:
                classes = ROOT / module / "target/classes"
                for path in classes.rglob("*.class"):
                    if module == "debezium-connector-ibmi" and path.stem.split("$", 1)[0] not in {
                            "As400ReplayCoordinator", "As400SourceNormalizer", "As400TransactionBuffer", "As400Checkpoint",
                            "As400JournalOrder", "As400SourceMetadata", "As400RowData"}:
                        continue
                    jar.write(path, str(path.relative_to(classes)))
            classes = self.test_classes
            for path in classes.rglob("As400ConnectProbeConnector*.class"):
                jar.write(path, str(path.relative_to(classes)))
        dependencies = (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":")
        for item in dependencies:
            path = Path(item)
            if (path.name in {"debezium-api-3.8.0-SNAPSHOT.jar", "debezium-config-3.8.0-SNAPSHOT.jar",
                              "debezium-util-3.8.0-SNAPSHOT.jar", "debezium-connector-common-3.8.0-SNAPSHOT.jar"}
                    or "/net/sf/jt400/" in item or "/com/h2database/h2-mvstore/" in item):
                shutil.copy2(path, self.plugin / path.name)
        self.report["harness_sha256"] = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                                         for name in ["tools/dcg-connect-probe", "tools/dcg-connect-probe.py",
                                                      "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400ConnectProbeConnector.java"]}
        self.report["plugin_sha256"] = {}
        for path in self.plugin.iterdir():
            self.report["plugin_sha256"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        name = self.prefix + "-" + name
        self.containers.append(name)
        self.command("docker", "run", "-d", "--name", name, "--label", "dcg.local.probe=" + self.prefix,
                     "--network", self.network, "--memory", "1g", *args)
        return name

    def start_broker(self):
        endpoint = os.environ.get("DOCKER_HOST") if not os.environ.get("DOCKER_CONTEXT") else None
        if not endpoint:
            endpoint = json.loads(self.command("docker", "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"))
        if not endpoint.startswith("unix://"):
            raise RuntimeError("This local-only experiment requires a Unix-socket Docker daemon")
        self.report["docker_endpoint"] = endpoint
        self.save()
        self.command("docker", "network", "create", "--internal", "--label", "dcg.local.probe=" + self.prefix, self.network)
        env = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
               "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093", "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://broker:9092",
               "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
               "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER", "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@broker:9093",
               "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
               "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
               "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1", "KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS": "1",
               "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0", "KAFKA_NUM_PARTITIONS": "1", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"}
        args = ["--network-alias", "broker"]
        for key, value in env.items():
            args.extend(["-e", key + "=" + value])
        self.broker = self.container("broker", [*args, IMAGE])
        def ready():
            return "Kafka Server started" in self.command("docker", "logs", self.broker, check=False)
        wait_for("broker startup", ready)

    def start_worker(self, scenario, role):
        identifier = scenario + "-" + role
        host = self.prefix + "-" + identifier
        config = {
            "bootstrap.servers": "broker:9092", "group.id": "group-" + scenario,
            "config.storage.topic": "configs-" + scenario, "offset.storage.topic": "offsets-" + scenario,
            "status.storage.topic": "status-" + scenario, "config.storage.replication.factor": "1",
            "offset.storage.replication.factor": "1", "status.storage.replication.factor": "1",
            "offset.storage.partitions": "1", "status.storage.partitions": "1",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false",
            "exactly.once.source.support": "enabled", "connector.client.config.override.policy": "All",
            "listeners": "http://0.0.0.0:8083", "rest.advertised.host.name": host, "rest.advertised.port": "8083",
            "session.timeout.ms": "6000", "heartbeat.interval.ms": "1000", "rebalance.timeout.ms": "10000",
            "scheduled.rebalance.max.delay.ms": "0", "task.shutdown.graceful.timeout.ms": "1000",
            "offset.flush.interval.ms": "1000", "offset.flush.timeout.ms": "20000",
            "producer.transaction.timeout.ms": "180000", "plugin.discovery": "only_scan",
        }
        config_path = self.directory / (identifier + ".properties")
        config_path.write_text("\n".join(key + "=" + value for key, value in config.items()) + "\n")
        worker = self.container(identifier, ["--user", f"{os.getuid()}:{os.getgid()}",
                                "-v", f"{self.plugin}:/probe:ro", "-v", f"{self.control}:/control",
                                "-v", f"{config_path}:/worker.properties:ro", "-e", "CLASSPATH=/probe/*",
                                "-e", "KAFKA_HEAP_OPTS=-Xms128m -Xmx512m", "-e", "PROBE_WORKER=" + identifier,
                                "--entrypoint", "/opt/kafka/bin/connect-distributed.sh", IMAGE, "/worker.properties"])
        wait_for("worker REST " + identifier, lambda: self.rest(worker, "/", tolerate=True))
        self.wait_worker_membership(worker, identifier)
        return worker

    def wait_worker_membership(self, worker, identifier):
        wait_for("worker membership " + identifier, lambda: "Successfully joined group" in self.command("docker", "logs", worker))

    def thread_dump(self, worker):
        # The Kafka image supplies a JRE without jcmd. SIGQUIT prints a JVM
        # diagnostic without terminating it; find only the Connect JVM, including
        # native harness workers where the supervisor (not Java) is PID 1.
        script = r'''for candidate in /proc/[0-9]*; do
case "$(readlink "$candidate/exe" 2>/dev/null)" in
*/java) if tr '\000' ' ' < "$candidate/cmdline" | grep -q 'org.apache.kafka.connect.cli.ConnectDistributed'; then
kill -QUIT "${candidate##*/}"; exit $?
fi;;
esac
done
exit 1'''
        self.command("docker", "exec", worker, "/bin/sh", "-c", script, timeout=10)
        def complete():
            log = self.command("docker", "logs", worker)
            dump = log[log.rfind("Full thread dump"):]
            return dump if "Full thread dump" in dump and "JNI global refs:" in dump else None
        return wait_for("Connect JVM thread dump", complete, 20)

    def rest(self, worker, path, payload=None, tolerate=False):
        try:
            output = self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS,
                                  "rest", path, "GET" if payload is None else json.dumps(payload), timeout=20)
            result = json.loads(output)
            if result["status"] >= 400:
                raise RestError(result["status"], result["body"])
            return json.loads(result["body"])
        except (RuntimeError, subprocess.TimeoutExpired):
            if tolerate:
                return None
            raise

    def create(self, worker, scenario, support="requested"):
        return self.rest(worker, "/connectors", {"name": scenario, "config": {
            "connector.class": CLASS, "tasks.max": "1", "probe.topic": "events-" + scenario,
            "exactly.once.support": support, "transaction.boundary": "poll", "errors.tolerance": "none",
            "producer.override.interceptor.classes": CLASS + "$BlockingInterceptor"}})

    def generation(self, scenario, role):
        pattern = scenario + "-" + role + "-*.start.json"
        path = wait_for("task startup " + pattern, lambda: next(iter(self.control.glob(pattern)), None))
        generation = json.loads(path.read_text())["generation"]
        wait_for("prepared synthetic journal", lambda: (self.control / (generation + ".prepared.json")).exists())
        return generation

    def signal(self, generation, suffix, value=""):
        path = self.control / (generation + "." + suffix)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(str(value))
        temporary.replace(path)

    def marker(self, generation, suffix):
        return wait_for(suffix, lambda: (self.control / (generation + "." + suffix + ".json")).exists(), 150)

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        topic = ("offsets-" if offsets else "events-") + scenario
        file = scenario + "-" + label + ("-offsets" if offsets else "-records") + "-" + isolation + ".json"
        self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS,
                     "broker:9092", topic, isolation, "/control/" + file, timeout=35)
        rows = json.loads((self.control / file).read_text())
        for row in rows:
            row["value"] = None if row["value"] is None else json.loads(row["value"])
            row["key"] = None if row["key"] is None else json.loads(row["key"])
        return rows

    def checkpoint(self, worker, scenario, label):
        rows = self.snapshot(worker, scenario, label, offsets=True)
        matches = [row["value"] for row in rows if row["key"] == [scenario, {"server": "synthetic"}]]
        return matches[-1] if matches else None

    def completed(self, worker, scenario, sequence):
        def found():
            checkpoint = self.checkpoint(worker, scenario, "wait-" + str(sequence))
            return checkpoint if checkpoint and checkpoint["completed_through.sequence"] == str(sequence) else None
        return wait_for("durable W=" + str(sequence), found, 60)

    def events(self, generation):
        path = self.control / (generation + ".jsonl")
        # The producer may be appending concurrently; ignore an incomplete last line.
        return [json.loads(line) for line in path.read_text().split("\n")[:-1]] if path.exists() else []

    def callbacks(self, generation, count):
        return wait_for("commitRecord callbacks", lambda:
                        sum(event["event"] == "commitRecord" for event in self.events(generation)) >= count)

    def rejected_source_send(self, generation, suspended):
        errors = {"org.apache.kafka.common.errors.ProducerFencedException", "org.apache.kafka.common.errors.InvalidProducerEpochException"} if suspended else {"java.lang.IllegalStateException"}
        return any(event["event"] == "producerAck" and event["fields"]["value"]["generation"] == generation and
                   event["fields"]["value"]["index"] == 1 and event["fields"]["value"]["op"] == "t" and
                   event["fields"]["error"] in errors for event in self.events(generation))

    def stop(self, worker):
        self.command("docker", "kill", worker)

    def assert_loaded(self, generation, expected):
        loaded = json.loads((self.control / (generation + ".start.json")).read_text())["loaded"]
        self.check(generation.split("-")[0] + ": fresh worker loaded the exact persisted F/W", loaded == expected,
                   generation + ".start.json")

    def assert_projection(self, rows):
        projection = {99}
        for row in rows:
            op = row["value"]["op"]
            if op == "t":
                projection.clear()
            elif op.startswith("c:"):
                projection.add(int(op[2:]))
        return projection == {1, 2, 3}

    def acknowledgements(self):
        scenario = "ack"
        worker = self.start_worker(scenario, "w1")
        try:
            self.create(worker, scenario, "required")
            raise AssertionError("Framework accepted a false EOS claim")
        except RestError as error:
            body = error.body
            (self.directory / "required-rejected.json").write_text(body)
            self.check("required EOS is rejected for the at-least-once probe", error.status == 400 and "exactly" in body.lower(), "required-rejected.json")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        prepared = json.loads((self.control / (generation + ".prepared.json")).read_text())
        self.check("R and in-memory W advance independently of durable output", prepared["scan"] == "9" and
                   prepared["checkpoint"]["completed_through.sequence"] == "9", generation + ".prepared.json")
        self.signal(generation, "permit", 5)
        self.callbacks(generation, 5)
        before = self.completed(worker, scenario, 1)
        self.check("batch records retain F=2 inclusive, previous W=1", before["replay_from.sequence"] == "2" and
                   before["replay_from.inclusive"] == "true" and "scan" not in before, "control/ack-wait-1-offsets-read_committed.json")
        self.signal(generation, "ack-block", 5)
        self.signal(generation, "permit", 6)
        self.marker(generation, "ack-blocked")
        visible = self.snapshot(worker, scenario, "delayed")
        raw = self.snapshot(worker, scenario, "delayed", isolation="read_uncommitted")
        delayed = self.checkpoint(worker, scenario, "delayed")
        self.check("delayed heartbeat acknowledgement cannot advance persisted W", delayed == before and len(visible) == 5 and len(raw) == 6,
                   "control/ack-delayed-*-read_*.json")
        self.signal(generation, "ack-release")
        after = self.completed(worker, scenario, 6)
        self.check("ordered heartbeat commits F=W=6 exclusively", after["replay_from.sequence"] == "6" and after["replay_from.inclusive"] == "false",
                   "control/ack-wait-6-offsets-read_committed.json")
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, after)
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        rows = self.snapshot(replacement, scenario, "final")
        self.check("every record carries the expected conservative checkpoint", [r["value"]["checkpoint"]["completed_through.sequence"] for r in rows] ==
                   ["1"] * 5 + ["6"] * 4 + ["9"], "control/ack-final-records-read_committed.json")
        self.check("durable batch is suppressed on restart; later batch stays ordered", [r["value"]["op"] for r in rows] == EXPECTED and self.assert_projection(rows),
                   "control/ack-final-records-read_committed.json")
        self.stop(replacement)

    def interrupted(self):
        scenario = "interrupted"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "permit", 3)
        self.callbacks(generation, 3)
        before = self.completed(worker, scenario, 1)
        partial = self.snapshot(worker, scenario, "partial")
        self.check("crash point is a committed truncate and partial reload", [r["value"]["op"] for r in partial] == EXPECTED[:3],
                   "control/interrupted-partial-records-read_committed.json")
        self.stop(worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, before)
        self.signal(fresh, "permit", 20)
        self.completed(replacement, scenario, 9)
        rows = self.snapshot(replacement, scenario, "final")
        self.check("interrupted batch replays wholly before later output", [r["value"]["op"] for r in rows] == EXPECTED[:3] + EXPECTED and self.assert_projection(rows),
                   "control/interrupted-final-records-read_committed.json")
        self.check("replay preserves synthetic source sequence and timestamp", all(
            (left["value"]["sequence"], left["timestamp"]) == (right["value"]["sequence"], right["timestamp"])
            for left, right in zip(rows[:3], rows[3:6])), "control/interrupted-final-records-read_committed.json")
        self.stop(replacement)

    def fencing(self, scenario):
        suspended = scenario == "fencing"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        generation = self.generation(scenario, "w1")
        self.signal(generation, "send-block", 1)
        self.signal(generation, "permit", 20)
        self.marker(generation, "send-blocked")
        before = self.completed(worker, scenario, 1)
        # Old source thread is deliberately stuck inside the real KafkaProducer.send.
        # Suspension prevents local close from masking the broker fencing assertion.
        if suspended:
            self.command("docker", "pause", worker)
        else:
            self.command("docker", "network", "disconnect", self.network, worker)
        replacement = self.start_worker(scenario, "w2")
        fresh = self.generation(scenario, "w2")
        self.assert_loaded(fresh, before)
        self.signal(fresh, "permit", 20)
        finished = self.completed(replacement, scenario, 9)
        rows_before = self.snapshot(replacement, scenario, "replacement-complete")
        if not suspended:
            self.command("docker", "network", "connect", self.network, worker)
        self.signal(generation, "send-release")
        if suspended:
            self.command("docker", "unpause", worker)
        self.marker(generation, "send-released")
        def rejected():
            log = self.command("docker", "logs", worker)
            pattern = r"ProducerFencedException|InvalidProducerEpochException" if suspended else r"Cannot perform operation after producer has been closed"
            return log if self.rejected_source_send(generation, suspended) and re.search(pattern, log) else None
        log = wait_for("old producer rejection: " + scenario, rejected, 60)
        (self.directory / (scenario + "-old-worker.log")).write_text(log)
        # Observe the terminal old producer failure before comparing a stable broker snapshot.
        rows_after = self.snapshot(replacement, scenario, "after-stale-send")
        self.check(scenario + ": old producer rejected after replacement (" + ("broker fence" if suspended else "local close") + ")", rows_before == rows_after and
                   [r["value"]["op"] for r in rows_after] == ["BEGIN"] + EXPECTED and self.assert_projection(rows_after),
                   scenario + "-old-worker.log; control/" + scenario + "-after-stale-send-records-read_committed.json")
        self.check(scenario + ": old generation cannot regress the durable checkpoint", self.checkpoint(replacement, scenario, "final") == finished,
                   "control/" + scenario + "-final-offsets-read_committed.json")
        self.stop(worker)
        self.stop(replacement)

    def cleanup(self):
        errors = []
        for name in reversed(self.containers):
            try:
                log = self.command("docker", "logs", name, check=False)
                (self.directory / (name + ".log")).write_text(log)
                self.command("docker", "rm", "-f", name, check=False)
            except Exception as error:
                errors.append(str(error))
        try:
            self.command("docker", "network", "rm", self.network, check=False)
            remaining = self.command("docker", "ps", "-aq", "--filter", "label=dcg.local.probe=" + self.prefix)
            networks = self.command("docker", "network", "ls", "-q", "--filter", "label=dcg.local.probe=" + self.prefix)
            if remaining or networks:
                errors.append("Owned Docker resources remain: " + remaining + " " + networks)
        except Exception as error:
            errors.append(str(error))
        self.report["cleanup"] = {"status": "passed" if not errors else "failed", "errors": errors}
        if errors:
            self.report["status"] = "failed"
        self.save()


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: dcg-connect-probe.py NEW_EVIDENCE_DIRECTORY (run the shell build wrapper first)")
    experiment = Experiment(Path(sys.argv[1]))
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.acknowledgements()
        experiment.interrupted()
        experiment.fencing("partition")
        experiment.fencing("fencing")
        experiment.check("all fault-injection gates completed within their deadlines",
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
        raise SystemExit("Experiment passed, cleanup failed; inspect report.json")
    print(str(experiment.directory / "report.json"), flush=True)


if __name__ == "__main__":
    main()
