#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Local-only Connect startup behind an abandoned offset transaction; no source connector."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("base", ROOT / "tools/dcg-connect-probe.py")
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


class Experiment(base.Experiment):
    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "offset-audit.jar", "w") as jar:
            for name in ["As400PackagedProbeAudit", "As400OffsetStartupFault"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        self.report["scope"] = "Kafka-only abandoned-offset startup; no source task, native connection or publication authority"
        self.report["plugin_sha256"].update({p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()})
        self.report["harness_sha256"]["tools/dcg-offset-startup-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        config = self.directory / (name + ".properties")
        if config.exists():
            with config.open("a") as stream:
                stream.write("request.timeout.ms=5000\ndefault.api.timeout.ms=10000\nworker.sync.timeout.ms=20000\nrebalance.timeout.ms=30000\n")
        return super().container(name, args)

    def observer(self, role, command):
        return self.container(role, ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.plugin}:/probe:ro",
                                    "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", command])

    def state(self, label):
        self.command("docker", "exec", self.audit, "java", "-cp", "/probe/*:/opt/kafka/libs/*",
                     "io.debezium.connector.db2as400.As400PackagedProbeAudit", "offset-state", "/control/" + label + ".json", timeout=35)
        return json.loads((self.control / (label + ".json")).read_text())

    def wait_worker_membership(self, worker, identifier):
        if identifier != "native-blocked":
            return super().wait_worker_membership(worker, identifier)
        started = time.monotonic()
        sampled = False
        def joined():
            nonlocal sampled
            log = self.command("docker", "logs", worker)
            if time.monotonic() - started >= 100 and not sampled:
                blocked = self.state("blocked")
                threads = self.thread_dump(worker)
                (self.directory / "blocked-threads.txt").write_text(threads)
                self.check("at 100 seconds worker waits on offset startup with an ongoing broker transaction",
                           "Successfully joined group" not in log and blocked["transaction"]["state"] == "Ongoing"
                           and blocked["read_committed"] < blocked["read_uncommitted"]
                           and "KafkaBasedLog.readToLogEnd" in threads and "KafkaOffsetBackingStore.start" in threads,
                           "control/blocked.json; blocked-threads.txt")
                sampled = True
            return "Successfully joined group" in log
        base.wait_for("membership after broker timeout abort", joined, 240)
        self.report["membership_seconds"] = time.monotonic() - started
        finished = self.state("joined")
        self.check("broker abort alone releases startup within the bounded wait",
                   sampled and finished["transaction"]["state"] == "CompleteAbort"
                   and finished["read_committed"] == finished["read_uncommitted"], "control/joined.json")

    def run(self):
        worker = self.start_worker("native", "seed")
        self.stop(worker)
        self.audit = self.observer("observer", "sleep 600")
        producer = self.observer("fault", "exec java -cp '/probe/*:/opt/kafka/libs/*' io.debezium.connector.db2as400.As400OffsetStartupFault /control/open-offset-ready")
        base.wait_for("flushed open offset transaction", lambda: (self.control / "open-offset-ready").exists(), 30)
        self.command("docker", "kill", "--signal=KILL", producer)
        state = self.state("killed")
        self.check("killed producer leaves a real uncommitted offset gap",
                   state["transaction"]["state"] == "Ongoing" and state["read_committed"] < state["read_uncommitted"], "control/killed.json")
        worker = self.start_worker("native", "blocked")
        self.check("worker starts without a source connector or fake offset recovery", self.rest(worker, "/connectors") == [], "commands.log")
        self.stop(worker)
        self.report["status"] = "passed"
        self.save()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 -B tools/dcg-offset-startup-probe.py NEW_EVIDENCE_DIRECTORY (compiled test classes required)")
    experiment = Experiment(Path(sys.argv[1]))
    try:
        experiment.stage(); experiment.start_broker(); experiment.run()
    except BaseException as error:
        experiment.report.update(status="failed", error=str(error)); experiment.save()
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed" and experiment.report["cleanup"]["status"] == "passed", experiment.report
    print(experiment.directory / "report.json", flush=True)
