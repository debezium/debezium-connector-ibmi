#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Real normal task startup/coordinator with owned edgeqa fixture and local Kafka."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("continuous", ROOT / "tools/dcg-continuous-probe.py")
continuous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(continuous)
native = continuous.native
base = continuous.base
CLASS = "io.debezium.connector.db2as400.As400NativeNormalTaskProbeConnector"
continuous.CLASS = CLASS
native.CLASS = CLASS
native.local.ownership.CLASS = CLASS


class NativeNormalTaskExperiment(continuous.ContinuousExperiment):
    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400NativeNormalTaskProbeConnector", "As400NativeTaskSession"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        for item in (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":"):
            if "/com/datadoghq/" in item:
                shutil.copy2(item, self.plugin / Path(item).name)
        self.report["scope"] = "Real As400ConnectorTask startup/snapshot/coordinator/queue/callbacks/continuous reader/stop; acquired native mixed schemas; controlled owned edgeqa journal; actual local Kafka"
        self.report["coordinator"] = "normal; no replacement coordinator, injected queue, independent initializer thread or seeded F/W"
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        for file in ["tools/dcg-native-normal-task-probe.py", "tools/dcg-native-normal-task-probe",
                     "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400NativeTaskSession.java",
                     "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400NativeNormalTaskProbeConnector.java"]:
            self.report["harness_sha256"][file] = hashlib.sha256((ROOT / file).read_bytes()).hexdigest()
        self.save()

    def create(self, worker, scenario):
        (self.control / (scenario + ".owned-fixture")).write_text(self.library)
        self.signal(scenario, "initialize-request")
        for suffix in ["-tx", ".initialization"]:
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                         "--create", "--topic", "events-" + scenario + suffix, "--partitions", "1", "--replication-factor", "1", "--config", "retention.ms=-1")
        return super().create(worker, scenario)

    def rest(self, worker, endpoint, payload=None, tolerate=False):
        if payload and isinstance(payload, dict) and payload.get("config", {}).get("connector.class") == CLASS:
            payload["config"]["topic.naming.strategy"] = CLASS + "$Topics"
            (self.directory / "native-normal-task-config.json").write_text(json.dumps(payload["config"], indent=2))
        return super().rest(worker, endpoint, payload, tolerate=tolerate)

    def snapshot(self, worker, scenario, label, offsets=False, isolation="read_committed"):
        rows = super().snapshot(worker, scenario, label, offsets, isolation)
        if not offsets:
            for suffix in ["-tx", ".initialization"]:
                rows.extend(base.Experiment.snapshot(self, worker, scenario + suffix, label, isolation=isolation))
            (self.control / (label + "-combined-" + isolation + ".json")).write_text(json.dumps(rows, indent=2))
        return rows


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-native-normal-task-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd (owned edgeqa fixture writes; local Kafka)")
    experiment = NativeNormalTaskExperiment(Path(sys.argv[1]), sys.argv[2])
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
                experiment.report["failure_evidence_error"] = experiment.fixture.redact(str(diagnostic_error))
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed", experiment.report.get("error", "Cleanup did not pass")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
