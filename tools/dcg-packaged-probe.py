#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Packaged native D/CG connector: fresh IBM i fixture, normal Connect, isolated test controller."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("continuous", ROOT / "tools/dcg-continuous-probe.py")
continuous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(continuous)
native = continuous.native
base = continuous.base
CLASS = "io.debezium.connector.db2as400.As400RpcConnector"


class PackagedFixture(continuous.MixedFixture):
    def setup(self):
        super().setup()
        self.sql(f"CREATE TABLE {self.library}.EXTRA (ID INTEGER NOT NULL PRIMARY KEY)", "create-extra.log", True)
        self.TABLES = ["A", "B", "M", "U", "EXTRA"]
        self.owned_objects = self.inventory("objects-extra-created.json")
        self.sql(f"INSERT INTO {self.library}.EXTRA VALUES (21),(22),(23)", "seed-extra.log", True)


class PackagedExperiment(continuous.ContinuousExperiment):
    container_lifetime_seconds = 2400

    def __init__(self, directory, library, profile="smoke"):
        super().__init__(directory, library)
        self.fixture.__class__ = PackagedFixture
        self.prefix_topic = "packaged"
        assert profile in ["smoke", "large", "partial"]
        self.profile = profile
        self.extra_ids = [21, 22, 23] + (list(range(10001, 25001)) if profile != "smoke" else [])
        self.reload_count = 5000 if profile == "large" else 3
        self.capture = ["A", "B", "M", "U", "EXTRA"]
        self.window = self.control / "initialization-window"
        self.report.update(scope="Distributed Connect loads only the distribution plugin; no test task or native provider injection",
                           configured_connector=CLASS, custom_publication_authority=False, transaction_boundary="connector, commit each returned poll")
        self.save()

    def stage(self):
        # Existing independent source transaction/REST/broker audit helpers remain
        # on a separate classpath. They are never in the worker's plugin path.
        super().stage()
        self.audit = self.directory / "audit"
        self.plugin.rename(self.audit)
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.audit / "packaged-audit.jar", "w") as jar:
            for path in classes.rglob("As400PackagedProbeAudit*.class"):
                jar.write(path, str(path.relative_to(classes)))
        self.plugin.mkdir()
        distribution_dir = self.plugin / "ibmi"
        distribution_dir.mkdir()
        distribution = ROOT / "debezium-connector-ibmi/target/debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.zip"
        with zipfile.ZipFile(distribution) as archive:
            for item in archive.infolist():
                if item.filename.endswith(".jar"):
                    (distribution_dir / Path(item.filename).name).write_bytes(archive.read(item))
        self.check("worker plugin contains packaged connector and no test provider", any(distribution_dir.glob("debezium-connector-ibmi-*.jar"))
                   and all("-tests" not in f.name and "synthetic" not in f.name for f in distribution_dir.iterdir()), "plugin/")
        for check in self.report["checks"]:
            if check["name"] == "candidate artifact excludes custom receipt and owner authority classes":
                check.update(name="independent test audit bundle excludes custom receipt/owner authority classes", evidence="audit/synthetic-probe.jar")
        self.report.pop("production_streaming_enabled", None)
        self.report.update(scope="Configured packaged connector in ordinary distributed Connect; isolated IBM i fixture only", profile=self.profile,
                           default_dcg_enabled=False, configured_dcg_enabled=True, fixture_only=True, snapshot_rows=len(self.extra_ids) + 12, reload_rows=self.reload_count)
        with zipfile.ZipFile(self.directory / "source-at-stage.zip", "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(ROOT.rglob("*.java")):
                if "/src/" in str(path) and "/target/" not in str(path): archive.write(path, str(path.relative_to(ROOT)))
            for path in sorted((ROOT / "tools").iterdir()):
                if path.is_file() and path.suffix in ["", ".py"]: archive.write(path, str(path.relative_to(ROOT)))
        self.report["distribution_sha256"] = hashlib.sha256(distribution.read_bytes()).hexdigest()
        self.report["plugin_sha256"] = {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in distribution_dir.iterdir()}
        self.report["harness_sha256"]["tools/dcg-packaged-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        config = self.directory / (name + ".properties")
        if not config.exists():
            return base.Experiment.container(self, name, (["--add-host", self.native_host] if name == "controller" else []) + args)
        with config.open("a") as stream:
            stream.write("\n".join(k + "=" + v for k, v in self.report["worker_overrides"].items()) + "\n")
            stream.write("plugin.path=/connector\nconfig.providers=file\nconfig.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider\n")
        worker = base.Experiment.container(self, name, ["--user", f"{os.getuid()}:{os.getgid()}", "--add-host", self.native_host,
                     "-v", f"{self.plugin}:/connector:ro", "-v", f"{self.audit}:/audit:ro", "-v", f"{self.control}:/control",
                     "-v", f"{config}:/worker.properties:ro", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep " + str(self.container_lifetime_seconds)])
        self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, worker)
        # Standard FileConfigProvider, with the credentials only in container
        # tmpfs. They never enter command args, config topics or retained evidence.
        result = subprocess.run(["docker", "exec", "-i", worker, "sh", "-c", "umask 077; cat > /dev/shm/dcg-credentials.properties"],
                                input=self.fixture.properties(), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        if result.returncode:
            raise RuntimeError("Cannot install transient worker credentials")
        path = self.directory / (name + ".native.log")
        process = subprocess.Popen(["docker", "exec", "-e", "KAFKA_HEAP_OPTS=-Xms128m -Xmx512m",
                                   "-e", "KAFKA_OPTS=-Dlog4j2.configurationFile=/audit/native-console.properties", worker,
                                   "/opt/kafka/bin/connect-distributed.sh", "/worker.properties"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.children.append(process)
        def collect():
            with path.open("w") as stream:
                for line in process.stdout:
                    stream.write(self.fixture.redact(line)); stream.flush()
        thread = threading.Thread(target=collect, daemon=True); thread.start(); self.log_threads.append(thread)
        self.logs[worker] = path
        return worker

    def rest_request(self, worker, path, method="GET", payload=None):
        args = ["rest", path, "GET" if payload is None else json.dumps(payload)] if method in ["GET", "POST"] else ["rest-put", path, "" if payload is None else json.dumps(payload)]
        raw = self.command("docker", "exec", worker, "java", "-cp", "/audit/*:/opt/kafka/libs/*", base.CLASS, *args, timeout=30)
        result = json.loads(raw)
        if result["status"] >= 400:
            raise base.RestError(result["status"], result["body"])
        return json.loads(result["body"]) if result["body"] else None

    def rest(self, worker, path, payload=None, tolerate=False):
        try:
            return self.rest_request(worker, path, "GET" if payload is None else "POST", payload)
        except (RuntimeError, subprocess.TimeoutExpired):
            if tolerate:
                return None
            raise

    def java(self, container, arguments, label):
        if arguments[0].endswith("As400NativeConnectTransactions") and len(arguments) == 4:
            arguments = arguments + [str(self.reload_count)]
        path = self.directory / (label + ".native.log")
        process = subprocess.Popen(["docker", "exec", "-i", container, "java", "-Xms64m", "-Xmx256m", "-cp", "/audit/*:/opt/kafka/libs/*", *arguments],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.children.append(process); process.stdin.write(self.fixture.properties()); process.stdin.close()
        def collect():
            with path.open("w") as stream:
                for line in process.stdout:
                    stream.write(self.fixture.redact(line)); stream.flush()
        thread = threading.Thread(target=collect, daemon=True); thread.start(); self.log_threads.append(thread)
        return process, path

    def topics(self):
        return [f"{self.prefix_topic}.{self.library}.{t}" for t in self.capture] + ["__debezium-heartbeat.packaged", "__debezium-heartbeat.packaged.initialization", "packaged.transaction", "packaged-history"]

    def consume(self, worker, topic, label):
        name = label + "-" + topic + ".json"
        self.command("docker", "exec", worker, "java", "-cp", "/audit/*:/opt/kafka/libs/*", "io.debezium.connector.db2as400.As400PackagedProbeAudit",
                     "broker:9092", topic, "read_committed", "/control/" + name, timeout=35)
        rows = json.loads((self.control / name).read_text())
        for row in rows:
            for key in ["key", "value"]:
                value = None if row[key] is None else json.loads(row[key])
                row[key] = value.get("payload") if isinstance(value, dict) and "schema" in value and "payload" in value else value
        return rows

    def rows(self, worker, table, label):
        return self.consume(worker, f"packaged.{self.library}.{table}", label)

    def checkpoint(self, worker, scenario, label):
        rows = self.consume(worker, "offsets-" + scenario, label)
        matches = [r["value"] for r in rows if r["key"] == [scenario, {"server": "packaged"}]]
        return matches[-1] if matches else None

    @staticmethod
    def data(rows):
        return [(r["value"]["op"], None if r["value"]["after"] is None else r["value"]["after"]["ID"]) for r in rows if isinstance(r["value"], dict) and "op" in r["value"]]

    def create(self, worker, scenario):
        self.report["running_scenario"] = scenario; self.save()
        for topic in self.topics():
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--create", "--topic", topic,
                         "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")
        command = [sys.executable, str(ROOT / "tools/dcg-start-window"), "prepare", str(self.window), "--operator", "isolated-packaged-fixture-controller",
                   "--journal", self.library + "/QSQJRN", "--captured", ",".join(self.library + "." + t for t in self.capture),
                   "--controlled", ",".join(self.library + "." + t for t in self.fixture.TABLES), "--seconds", "600", "--rows", "100000", "--bytes", "16000000",
                   "--chunk-rows", "256", "--chunk-bytes", "1000000", "--all-journal-writers-stopped", "--maintenance-stopped", "--operator-managed-retention"]
        self.plan_command = command
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        (self.directory / "operator-plan.json").write_text(result.stdout)
        config = {"connector.class": CLASS, "name": scenario, "dcg.mode": "initialize", "dcg.journal": self.library + "/QSQJRN",
                  "dcg.tables": ",".join(self.library + "." + t for t in self.capture), "dcg.controlled.tables": ",".join(self.library + "." + t for t in self.fixture.TABLES),
                  "dcg.journaled.libraries": self.library, "dcg.initialization.window": "/control/initialization-window", "dcg.retention": "operator-managed",
                  "database.schema": self.library, "database.dbname": self.fixture.server_name, "secure": "false", "tasks.max": "1", "topic.prefix": "packaged",
                  "schema.history.internal.kafka.bootstrap.servers": "broker:9092", "schema.history.internal.kafka.topic": "packaged-history",
                  "producer.override.bootstrap.servers": "broker:9092", "admin.override.bootstrap.servers": "broker:9092", "exactly.once.support": "requested",
                  "transaction.boundary": "connector", "errors.tolerance": "none", "event.converting.failure.handling.mode": "fail", "skipped.operations": "none",
                  "provide.transaction.metadata": "true", "tombstones.on.delete": "true", "topic.naming.strategy": "io.debezium.schema.SchemaTopicNamingStrategy",
                  "decimal.handling.mode": "precise", "binary.handling.mode": "bytes", "time.precision.mode": "adaptive_time_microseconds",
                  "schema.name.adjustment.mode": "none", "field.name.adjustment.mode": "none", "trim.non.xml.charsequence.field.mode": "none",
                  "key.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                  "key.converter.schemas.enable": "true", "value.converter.schemas.enable": "true", "max.batch.size": "128", "max.queue.size": "1024", "poll.interval.ms": "25",
                  "heartbeat.interval.ms": "1000"}
        for key in ["database.hostname", "database.user", "database.password"]:
            config[key] = "${file:/dev/shm/dcg-credentials.properties:" + key + "}"
        self.configure(config)
        self.connector_config = config
        (self.directory / "connector-config.json").write_text(json.dumps(config, indent=2))
        return self.rest(worker, "/connectors", {"name": scenario, "config": config})

    def configure(self, config):
        pass

    def expected_key(self, value):
        return {"ID": value["ID"]}

    def mixed_value(self, value):
        return continuous.ContinuousExperiment.decode_mixed(value)

    def expected_mixed(self, identifier, changed=False):
        return continuous.ContinuousExperiment.expected_mixed(identifier, changed)

    def healthy(self, worker):
        status = self.rest(worker, "/connectors/native/status")
        if status["connector"]["state"] == "FAILED" or any(t["state"] == "FAILED" for t in status["tasks"]):
            (self.directory / "failed-status.json").write_text(json.dumps(status, indent=2))
            raise RuntimeError("Packaged connector failed; see failed-status.json")
        return status

    def await_data(self, worker, table, expected, label):
        def ready():
            self.healthy(worker)
            rows = self.rows(worker, table, label)
            return rows if self.data(rows) == expected else None
        return base.wait_for(label, ready, 180)

    def run_native(self):
        self.fixture.setup(); self.report["source_connected"] = True; self.save()
        for offset in range(3, len(self.extra_ids), 100):
            self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES " + ",".join("(" + str(i) + ")" for i in self.extra_ids[offset:offset + 100]), "large-seed-" + str(offset) + ".log", True)
        self.controller = self.container("controller", ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.audit}:/audit:ro", "-v", f"{self.control}:/control",
                              "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep " + str(self.container_lifetime_seconds)])
        self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, self.controller)
        worker = self.start_worker("native", "w1"); self.create(worker, "native")
        if self.profile == "partial":
            return self.partial_start(worker)
        def initialized():
            self.healthy(worker)
            path = self.window / "status.properties"
            return path.exists() and "phase=HANDED_OFF" in path.read_text()
        base.wait_for("packaged native snapshot/history/FW handoff", initialized, 600)
        self.check("packaged connector completes native initialization", True, "control/initialization-window/status.properties")
        baseline = {}
        for table in self.capture:
            expected = [("r", x) for x in (self.extra_ids if table == "EXTRA" else [1, 2, 3])]
            baseline[table] = self.rows(worker, table, "baseline")
            self.check("native baseline has exact keys for " + table, self.data(baseline[table]) == expected
                       and all(r["key"] == self.expected_key(r["value"]["after"]) for r in baseline[table]), "control/baseline-*.json")
        saved = self.checkpoint(worker, "native", "initialized")
        self.check("normal task created durable F/W", saved and saved.get("offset.version") == "2", "control/initialized-offsets-native.json")
        (self.window / "plan.properties").rename(self.window / "completed-plan.properties")
        process, directory = self.start_transaction("truncate-commit")
        self.await_data(worker, "A", [("r", i) for i in [1, 2, 3]] + [("c", 8)], "unrelated-open")
        self.check("unrelated CDC progresses while native clear remains open", process.poll() is None and self.rows(worker, "B", "clear-open") == baseline["B"], "control/")
        self.command("docker", "kill", "--signal=KILL", worker)
        replacement = self.start_worker("native", "w2")
        base.wait_for("replacement task running", lambda: any(t["state"] == "RUNNING" for t in self.healthy(replacement)["tasks"]), 90)
        self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES (24)", "replacement-progress.log", True)
        self.await_data(replacement, "EXTRA", [("r", i) for i in self.extra_ids] + [("c", 24)], "replacement-progress")
        self.check("fresh worker resumes without a window or duplicate snapshot", self.rows(replacement, "B", "recovered-open") == baseline["B"], "control/")
        self.finish_transaction("truncate-commit", process, directory)
        expected_b = [("r", i) for i in [1, 2, 3]] + [("t", None)] + [("c", i) for i in range(100, 100 + self.reload_count)]
        self.await_data(replacement, "B", expected_b, "truncate-committed")
        self.check("committed clear precedes reload after task death", True, "control/truncate-committed-*.json")
        for mode, aid in [("delete-rollback", 9), ("truncate-rollback", 10), ("delete-commit", 11)]:
            process, directory = self.start_transaction(mode)
            self.await_data(replacement, "A", [("r", i) for i in [1, 2, 3]] + [("c", i) for i in range(8, aid + 1)], mode + "-open")
            self.check(mode + ": open clear is withheld", self.data(self.rows(replacement, "B", mode + "-before")) == expected_b and process.poll() is None, "control/")
            self.finish_transaction(mode, process, directory)
            if mode == "delete-commit":
                expected_b += [("t", None)] + [("c", i) for i in range(400, 400 + self.reload_count)]
            # An unrelated row after the terminal establishes forward progress for rollback too.
            marker = 100 + aid
            self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES ({marker})", mode + "-progress.log", True)
            expected_extra = [("r", i) for i in self.extra_ids] + [("c", 24)] + [("c", 100 + i) for i in range(9, aid + 1)]
            self.await_data(replacement, "EXTRA", expected_extra, mode + "-terminal")
            self.check(mode + ": output follows terminal outcome", self.data(self.rows(replacement, "B", mode + "-after")) == expected_b, "control/")
        for rotation in range(17 if self.profile == "large" else 1):
            self.fixture.rotate()
            # Save every rotation separately; the helper's standard paths are reused.
            for path in self.fixture.directory.glob("rotation-*.json"):
                shutil.copyfile(path, self.fixture.directory / ("turn-" + str(rotation) + "-" + path.name))
        self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES (999)", "post-rotation.log", True)
        self.await_data(replacement, "EXTRA", expected_extra + [("c", 999)], "post-rotation")
        self.check("configured reader continues across receiver rotation", True, "native/rotation-*.json; control/")
        mixed = self.rows(replacement, "M", "mixed-before")
        self.check("configured JsonConverter preserves native mixed-column values and keys", all(self.mixed_value(r["value"]["after"]) == self.expected_mixed(i) for r, i in zip(mixed, [1, 2, 3])), "control/mixed-before-*.json")
        self.fixture.sql(f"UPDATE {self.library}.M SET S=-123,N=-12.34,D=0.001,C='XYZ',V=NULL,B=BX'FF00807F',VB=BX'1000FF',L=-9007199254740993,F=-1.5,G=3.25 WHERE ID=1", "mixed-update.log", True)
        mixed = self.await_data(replacement, "M", [("r", i) for i in [1, 2, 3]] + [("u", 1)], "mixed-updated")
        update = mixed[-1]
        self.check("packaged update preserves before/after, decimals, text, binary and 64-bit integer", update["key"] == self.expected_key(update["value"]["after"])
                   and self.mixed_value(update["value"]["before"]) == self.expected_mixed(1)
                   and self.mixed_value(update["value"]["after"]) == self.expected_mixed(1, True), "control/mixed-updated-*.json")
        self.fixture.sql(f"DELETE FROM {self.library}.M WHERE ID=2", "mixed-delete.log", True)
        deleted = self.await_data(replacement, "M", [("r", i) for i in [1, 2, 3]] + [("u", 1), ("d", None)], "mixed-deleted")
        self.check("native delete preserves row identity and publishes its tombstone", deleted[-1]["key"] == self.expected_key(deleted[-2]["value"]["before"]) and deleted[-1]["value"] is None
                   and deleted[-2]["key"] == self.expected_key(deleted[-2]["value"]["before"]) and self.mixed_value(deleted[-2]["value"]["before"]) == self.expected_mixed(2), "control/mixed-deleted-*.json")
        replacement = self.verify_projection(replacement) or replacement
        self.rest_request(replacement, "/connectors/native/stop", "PUT")
        base.wait_for("normal task stopped", lambda: self.rest(replacement, "/connectors/native/status")["connector"]["state"] == "STOPPED", 60)
        base.wait_for("native handles drained", lambda: "D/CG native handles drained" in self.logs[replacement].read_text(), 60)
        self.check("normal stop completes native drain", True, self.logs[replacement].name)
        self.missing_offset(replacement)
        self.report["status"] = "passed"; self.save()

    def verify_projection(self, worker):
        # Independent reference consumer of the real read_committed wire records.
        # This is a convergence oracle, not validation of any commercial sink plugin.
        for table in self.capture:
            state = {}
            for row in self.rows(worker, table, "sink"):
                value = row["value"]
                if value is None: continue
                op = value["op"]
                if op == "t":
                    assert row["key"] is None
                    state.clear()
                elif op in ["r", "c", "u"]: state[row["key"]["ID"]] = value["after"]
                elif op == "d": state.pop(row["key"]["ID"], None)
                else: raise AssertionError(op)
            actual = self.fixture.sql(f"SELECT ID FROM {self.library}.{table} ORDER BY ID", "sink-native-" + table + ".json")
            self.check("reference truncate-aware consumer converges for " + table, sorted(state) == [r["ID"] for r in actual], "control/sink-*.json; native/sink-native-" + table + ".json")

    def missing_offset(self, worker):
        before = {t: self.consume(worker, t, "missing-before") for t in self.topics()}
        config = dict(self.connector_config, name="missing", **{"dcg.mode": "resume"})
        self.rest(worker, "/connectors", {"name": "missing", "config": config})
        def failed():
            state = self.rest(worker, "/connectors/missing/status")
            return state if any(t["state"] == "FAILED" for t in state["tasks"]) else None
        status = base.wait_for("missing offset rejection", failed, 60)
        (self.directory / "missing-offset-status.json").write_text(json.dumps(status, indent=2))
        self.check("missing offset cannot publish or silently reinitialize", "Missing durable F/W" in json.dumps(status)
                   and all(self.consume(worker, t, "missing-after") == rows for t, rows in before.items()), "missing-offset-status.json; control/missing-*.json")
        command = list(self.plan_command)
        command[3] = str(self.control / "replacement-window")
        result = subprocess.run(command, text=True, capture_output=True, check=True)
        (self.directory / "replacement-plan.json").write_text(result.stdout)
        config.update(name="missing-init", **{"dcg.mode": "initialize", "dcg.initialization.window": "/control/replacement-window"})
        self.rest(worker, "/connectors", {"name": "missing-init", "config": config})
        def refused():
            state = self.rest(worker, "/connectors/missing-init/status")
            return state if any(t["state"] == "FAILED" for t in state["tasks"]) else None
        status = base.wait_for("existing generation rejects new initialization", refused, 60)
        (self.directory / "missing-initialize-status.json").write_text(json.dumps(status, indent=2))
        self.check("new operator plan cannot replace missing offsets over existing output/history", "Existing output/history cannot initialize" in json.dumps(status)
                   and all(self.consume(worker, t, "missing-init-after") == rows for t, rows in before.items()), "missing-initialize-status.json; control/")

    def source_failure_evidence(self):
        # Read-only, bounded evidence from this controller-owned fixture before
        # cleanup. No exception here may replace the original source failure.
        if not self.fixture.created:
            return
        lib = self.library
        try:
            self.fixture.sql(f"SELECT SEQUENCE_NUMBER,JOURNAL_CODE,JOURNAL_ENTRY_TYPE,OBJECT,OBJECT_TYPE,COMMIT_CYCLE,COUNT_OR_RRN,THREAD,INDICATOR_FLAG,HEX(SUBSTR(ENTRY_DATA,1,4096)) AS DATA_PREFIX FROM TABLE(QSYS2.DISPLAY_JOURNAL('{lib}','QSQJRN',STARTING_RECEIVER_LIBRARY=>'{lib}',STARTING_RECEIVER_NAME=>'*CURRENT',ENDING_RECEIVER_LIBRARY=>'{lib}',ENDING_RECEIVER_NAME=>'*CURRENT')) X FETCH FIRST 501 ROWS ONLY", "failure-journal.json")
            self.report["failure_journal_evidence"] = "native/failure-journal.json; current receiver, at most 501 entries, payload prefixes only"
        except Exception as error:
            self.report["failure_journal_evidence_error"] = self.fixture.redact(str(error))
        self.save()

    def partial_start(self, worker):
        def exposed():
            self.healthy(worker)
            return self.rows(worker, "A", "partial-wait")
        base.wait_for("first committed snapshot chunk", exposed, 600)
        self.command("docker", "kill", "--signal=KILL", worker)
        replacement = self.start_worker("native", "w2")
        def failed():
            status = self.rest(replacement, "/connectors/native/status")
            return status if any(t["state"] == "FAILED" for t in status["tasks"]) else None
        status = base.wait_for("partial initialization rejected", failed, 90)
        (self.directory / "partial-status.json").write_text(json.dumps(status, indent=2))
        saved = self.checkpoint(replacement, "native", "partial")
        before = {t: self.rows(replacement, t, "partial-before") for t in self.capture}
        count = sum(len(v) for v in before.values())
        self.check("partial snapshot has no initialized F/W", 0 < count < len(self.extra_ids) + 12 and saved and saved.get("initialization.version") == "chunks-1" and "offset.version" not in saved,
                   "control/partial-offsets-native.json; partial-status.json")
        self.check("replacement cannot publish from a partial first start", all(self.rows(replacement, t, "partial-after") == rows for t, rows in before.items()), "control/partial-*.json")
        self.check("partial first start cannot install binding history", self.consume(replacement, "packaged-history", "partial-history") == [], "control/partial-history-*.json")
        self.report["status"] = "passed"; self.save()


def main():
    if len(sys.argv) not in [3, 4]:
        raise SystemExit("Usage: dcg-packaged-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd")
    experiment = PackagedExperiment(Path(sys.argv[1]), sys.argv[2], sys.argv[3] if len(sys.argv) == 4 else "smoke")
    try:
        experiment.stage(); experiment.start_broker(); experiment.run_native()
    except BaseException as error:
        experiment.report.update(status="failed", error=experiment.fixture.redact(str(error))); experiment.save()
        experiment.source_failure_evidence()
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed", experiment.report
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
