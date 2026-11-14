#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded planned first-start window: owned IBM i test library and local Kafka only."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("normal", ROOT / "tools/dcg-native-normal-task-probe.py")
normal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(normal)
base = normal.base
CLASS = "io.debezium.connector.db2as400.As400PlannedNativeProbeConnector"
ORIGINAL = normal.CLASS
normal.CLASS = CLASS
normal.continuous.CLASS = CLASS
normal.native.CLASS = CLASS
normal.native.local.ownership.CLASS = CLASS


def properties(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if line and not line.startswith("#"))


class PlannedExperiment(normal.NativeNormalTaskExperiment):
    def __init__(self, directory, library, rows, mode):
        super().__init__(directory, library)
        self.chunked = mode.startswith("chunk-")
        mode = mode.removeprefix("chunk-")
        assert mode in ["success", "timeout", "revoke", "crash", "bounds"]
        assert 10 <= rows <= (10000 if self.chunked else 2500)
        self.rows = rows
        self.mode = mode
        self.publication_limit = 100000 if self.chunked else 20000
        self.ids = [1, 2, 3] + list(range(10001, 10001 + rows - 3))
        self.window = self.control / "native.window"
        self.report.update(mode=mode, chunked=self.chunked, baseline_rows=3 * rows, operator_window="explicit scoped plan; native locks/census/bindings; controller owns all fixture producers")

    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400PlannedNativeProbeConnector", "As400NativeSnapshotScanner"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        for name in ["tools/dcg-planned-start-probe.py", "tools/dcg-start-window"]:
            self.report["harness_sha256"][name] = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        self.save()

    def rest(self, worker, endpoint, payload=None, tolerate=False):
        if payload and isinstance(payload, dict) and payload.get("config", {}).get("connector.class") == CLASS:
            config = payload["config"]
            config.update({"max.batch.size": "128", "max.queue.size": "1024",
                           "topic.naming.strategy": ORIGINAL + "$Topics",
                           "producer.override.interceptor.classes": ORIGINAL + "$ContinuousInterceptor"})
            # Open the measured operator window after fixture load, worker startup
            # and topic provisioning. No writer/maintenance process runs now.
            command = [sys.executable, str(ROOT / "tools/dcg-start-window"), "prepare", str(self.window),
                       "--operator", "isolated-fixture-controller", "--journal", self.library + "/QSQJRN",
                       "--captured", ",".join(self.library + "." + t for t in ["A", "B", "M"]),
                       "--controlled", ",".join(self.library + "." + t for t in ["A", "B", "M", "U"]),
                       "--seconds", "35" if self.mode == "timeout" else "600", "--rows", "1000" if self.mode == "bounds" else str(3 * self.rows) if self.chunked else "9000",
                       "--bytes", "16000000", "--all-journal-writers-stopped", "--maintenance-stopped", "--operator-managed-retention"]
            if self.chunked:
                command += ["--chunk-rows", "256", "--chunk-bytes", "1000000"]
            else:
                command += ["--legacy-list-plan"]
            result = subprocess.run(command, text=True, capture_output=True, check=True)
            (self.directory / "operator-plan.json").write_text(result.stdout)
            (self.control / "native.hold-checkpoint-durable").unlink()
            (self.directory / "planned-connector-config.json").write_text(json.dumps(config, indent=2))
            # Parent transforms would overwrite the explicit inherited class names.
            return normal.native.NativeExperiment.rest(self, worker, endpoint, payload, tolerate=tolerate)
        return super().rest(worker, endpoint, payload, tolerate=tolerate)

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        topics = [("offsets-" + scenario, None)] if offsets else [("events-" + scenario + suffix, table)
                    for suffix, table in [("", None), ("-A", "A"), ("-B", "B"), ("-M", "M"), ("-tx", None), (".initialization", None)]]
        combined = []
        for topic, table in topics:
            name = topic + "-" + label + "-" + isolation + ".json"
            self.command("docker", "exec", worker, "java", "-cp", "/probe/*:/opt/kafka/libs/*", CLASS,
                         "broker:9092", topic, isolation, "/control/" + name, timeout=35)
            rows = json.loads((self.control / name).read_text())
            for row in rows:
                row["value"] = None if row["value"] is None else json.loads(row["value"])
                row["key"] = None if row["key"] is None else json.loads(row["key"])
                if table:
                    row["fixture_table"] = table
            combined.extend(rows)
        (self.control / (label + ("-offsets" if offsets else "-combined") + "-" + isolation + ".json")).write_text(json.dumps(combined, indent=2))
        return combined

    def seed_large(self):
        started = time.monotonic()
        for table in ["A", "B", "M"]:
            for offset in range(3, len(self.ids), 100):
                values = []
                for identifier in self.ids[offset:offset + 100]:
                    suffix = ",12,123.45,1234.567,'ABC','Ä![]',BX'00010203',BX'00FF',9007199254740993,1.25,-2.5" if table == "M" else ""
                    values.append("(" + str(identifier) + suffix + ")")
                self.fixture.sql("INSERT INTO " + self.library + "." + table + " VALUES " + ",".join(values), "large-seed-" + table + "-" + str(offset) + ".log", True)
            result = self.fixture.sql(f"SELECT COUNT(*) AS N FROM {self.library}.{table}", "large-count-" + table + ".json")
            assert result == [{"N": self.rows}], result
        self.report["load_seconds_excluded_from_window"] = time.monotonic() - started
        self.save()

    def marker(self, generation, suffix):
        if suffix != "handoff":
            return super().marker(generation, suffix)

        def found():
            path = self.control / (generation + ".handoff.json")
            if path.exists():
                return path
            self.healthy(generation)
        # Larger baselines have a separately bounded ten-minute window. The
        # nine-row harness's four-minute marker deadline is not that contract.
        return base.wait_for("planned native reader handoff", found, 600)

    def final_fault(self, worker, generation):
        self.marker(generation, "failed")
        # Failure is expected here; the ordinary marker helper intentionally
        # raises on a failed task before a later marker appears.
        base.wait_for("native handles drain after expected failure", lambda: (self.control / (generation + ".drained.json")).exists(), 120)
        # Native handles finish draining before the policy wrapper persists its
        # final quarantine outcome. Observe both, not just the native marker.
        status = base.wait_for("operator quarantine after native drain", lambda: self.quarantined(), 15)
        failure = json.loads((self.control / (generation + ".failed.json")).read_text())["error"]
        expected = {"timeout": "expired", "revoke": "revoked", "bounds": "total budget" if self.chunked else "row bound"}[self.mode]
        # The coordinator can close the native session before poll observes its
        # original exception. In that race poll reports "session stopped" while
        # ErrorHandler retains the materialization failure in the worker log.
        # Require the intended cause in actual worker evidence; a generic stop
        # alone is never evidence that the requested fault was exercised.
        def intended_cause():
            log = self.command("docker", "logs", worker)
            (self.directory / "expected-failure-worker.log").write_text(log)
            return expected in failure or expected in log
        base.wait_for("intended initialization failure in worker evidence", intended_cause, 15)
        self.check("requested initialization fault reached its intended guard", intended_cause(), "expected-failure-worker.log; control/" + generation + ".failed.json")
        evidence_worker = worker
        if self.chunked:
            # Stabilize partial-prefix evidence before comparing a replacement.
            self.stop(worker)
            evidence_worker = self.controller
        cp = self.checkpoint(evidence_worker, "native", "failed")
        rows = self.snapshot(evidence_worker, "native", "failed")
        self.check("failed initialization quarantines enrollment without history/F/W or reader handoff", status["phase"] == "QUARANTINED"
                   and (self.window / "enrolled").exists() and not self.history(evidence_worker, "native", "failed")
                   and (cp is None or cp.get("initialization.phase") is not None)
                   and not (self.control / (generation + ".handoff.json")).exists(), "native.window/; control/")
        if self.mode != "crash" and not self.chunked:
            self.check("timeout/revocation/bounds produce zero data without permits", all(not self.data(rows, t) for t in ["A", "B", "M"]), "control/")
        if self.chunked:
            count = sum(len(self.data(rows, t)) for t in ["A", "B", "M"])
            self.check("chunk failure leaves only the expected incomplete prefix", count == 0 if self.mode == "timeout" else 0 < count < 3 * self.rows,
                       "control/failed-combined-read_committed.json")
        self.report["window_result"] = status
        if not self.chunked:
            self.stop(worker)
        worker = self.start_worker("native", "w2")
        failed = self.generation("native", "w2")
        self.marker(failed, "failed")
        self.check("fresh worker cannot reuse failed first-start generation", self.snapshot(worker, "native", "rejected") == rows
                   and self.checkpoint(worker, "native", "rejected") == cp
                   and not (self.control / (failed + ".handoff.json")).exists(), "control/")
        self.stop(worker)

    def quarantined(self):
        path = self.window / "status.properties"
        status = properties(path) if path.exists() else None
        return status if status and status["phase"] == "QUARANTINED" else None

    def run_native(self):
        self.fixture.setup()
        self.report["source_connected"] = True
        self.seed_large()
        self.controller = self.container("controller", ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.plugin}:/probe:ro",
                    "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 2400"])
        self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, self.controller)
        worker = self.start_worker("native", "w1")
        self.create(worker, "native")
        generation = self.generation("native", "w1")
        if self.chunked and self.mode in ["success", "bounds"]:
            self.signal(generation, "permit", self.publication_limit)
        if self.mode in ["bounds", "timeout"]:
            self.final_fault(worker, generation)
        else:
            if self.chunked and self.mode in ["crash", "revoke"]:
                self.signal(generation, "permit", 257 if self.mode == "revoke" else 128)
                base.wait_for("pending native chunk in durable offsets", lambda: (self.checkpoint(worker, "native", "chunk-pending") or {}).get("initialization.phase") in ["chunk-data", "chunk-complete"], 120)
            else:
                self.marker(generation, "snapshot-prepared")
            if self.mode == "revoke":
                result = subprocess.run([sys.executable, str(ROOT / "tools/dcg-start-window"), "revoke", str(self.window)], text=True, capture_output=True, check=True)
                (self.directory / "operator-revoke.json").write_text(result.stdout)
                self.final_fault(worker, generation)
            elif self.mode == "crash":
                self.signal(generation, "permit", 128)
                base.wait_for("partial snapshot committed", lambda: (self.checkpoint(worker, "native", "partial") or {}).get("initialization.phase") in (["chunk-data", "chunk-complete"] if self.chunked else ["snapshot"]), 120)
                self.stop(worker)
                # The original producer can still commit while evidence clients
                # read different topics. Establish the comparison after killing
                # it, using an independent controller JVM, before replacement.
                before = self.snapshot(self.controller, "native", "post-crash")
                cp = self.checkpoint(self.controller, "native", "post-crash")
                self.check("crash leaves an incomplete committed snapshot and pending offset", 0 < sum(len(self.data(before, t)) for t in ["A", "B", "M"]) < 3 * self.rows
                           and cp.get("initialization.phase") in (["chunk-data", "chunk-complete"] if self.chunked else ["snapshot"]) and not self.history(self.controller, "native", "post-crash"), "control/")
                worker = self.start_worker("native", "w2")
                replacement = self.generation("native", "w2")
                self.marker(replacement, "failed")
                failure = json.loads((self.control / (replacement + ".failed.json")).read_text())["error"]
                self.check("crash after partial snapshot rejects pending offset without replay or fabricated F/W", self.snapshot(worker, "native", "crash-rejected") == before
                           and self.checkpoint(worker, "native", "crash-rejected") == cp and not self.history(worker, "native", "crash-rejected")
                           and "buffered offset" in failure and (self.window / "enrolled").exists()
                           and not (self.control / (replacement + ".handoff.json")).exists(), "control/")
                self.report["window_result"] = properties(self.window / "status.properties")
                self.report["window_result"]["outcome"] = "interrupted generation remains enrolled; no successful handoff"
                self.stop(worker)
            else:
                self.signal(generation, "permit", self.publication_limit)
                self.marker(generation, "handoff")
                status = properties(self.window / "status.properties")
                self.check("operator window ends only after native snapshot/history/F/W and reader handoff", status["phase"] == "HANDED_OFF"
                           and len(self.history(worker, "native", "initialized")) == 1, "native.window/; control/")
                self.report["window_result"] = status
                rows = self.snapshot(worker, "native", "initialized")
                expected = [("r", i) for i in self.ids]
                self.check("larger snapshot has every native row exactly once with complete keys", all(self.data(rows, t) == expected for t in ["A", "B", "M"])
                           and all(r["key"] == {"ID": r["value"]["after"]["ID"]} for r in rows if r.get("fixture_table")), "control/initialized-combined-read_committed.json")
                self.check("larger snapshot preserves exact native mixed-column values", all(self.decode_mixed(r["value"]["after"]) == self.expected_mixed(i)
                           for r, i in zip(self.mixed(rows), self.ids)), "control/initialized-combined-read_committed.json")
                # Handoff completed. Remove active plan to prove ordinary recovery
                # does not need another operator window, even with an open clear.
                (self.window / "plan.properties").rename(self.window / "completed-plan.properties")
                process, transaction = self.start_transaction("truncate-commit")
                rows = self.rows_until(worker, generation, "open-clear", lambda rs: ("c", 8) in self.data(rs, "A"))
                self.check("unrelated CDC continues while the clear/reload remains uncommitted", self.data(rows, "B") == expected and process.poll() is None, "control/")
                saved = self.checkpoint(worker, "native", "before-restart")
                self.stop(worker)
                worker = self.start_worker("native", "w2")
                generation = self.generation("native", "w2")
                self.signal(generation, "permit", self.publication_limit)
                self.marker(generation, "handoff")
                self.check("ordinary restart recovers F/W without active window or new snapshot", json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == saved
                           and not (self.control / (generation + ".snapshot-prepared.json")).exists()
                           and not (self.window / "plan.properties").exists(), "control/")
                self.finish_transaction("truncate-commit", process, transaction)
                expected_b = expected + [("t", None)] + [("c", i) for i in [100, 101, 102]]
                rows = self.rows_until(worker, generation, "committed", lambda rs: self.data(rs, "B") == expected_b)
                self.check("restart preserves truncate-before-reload and avoids duplicate snapshot", self.data(rows, "A") == expected + [("c", 8)]
                           and self.data(rows, "M") == expected, "control/")
                self.stop_task(worker, "native")
                self.marker(generation, "drained")
                self.check("planned-path native reader and JDBC cleanup drain", any(e['event'] == 'stopRequested' for e in self.events(generation)), "control/")
                self.stop(worker)
        self.check("all fault hooks stayed within bounds", not list(self.control.glob("*.hook-timeout.json")), "control/")
        self.report["status"] = "passed"
        self.save()


def main():
    if len(sys.argv) != 5:
        raise SystemExit("Usage: dcg-planned-start-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd ROWS_PER_TABLE success|timeout|revoke|crash|bounds")
    experiment = PlannedExperiment(Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4])
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
            except Exception as diagnostic:
                experiment.report["failure_evidence_error"] = experiment.fixture.redact(str(diagnostic))
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed", experiment.report.get("error", "Cleanup did not pass")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
