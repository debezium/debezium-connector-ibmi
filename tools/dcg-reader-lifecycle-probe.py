#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Local real-reader races; inherits every existing partition publication assertion."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("catchup_probe", ROOT / "tools/dcg-config-catchup-probe.py")
catchup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catchup)
ownership = catchup.ownership
ownership.CLASS = "io.debezium.connector.db2as400.As400ReaderLifecycleProbeConnector"


class ReaderExperiment(catchup.CatchupExperiment):
    def __init__(self, directory, outcome):
        super().__init__(directory, "aligned")
        self.outcome = outcome
        self.report["reader_outcome"] = outcome
        self.report["scope"] = "Real incremental RPC/decoder/task/dispatcher; controlled JT400 transport and native job handles; no IBM i"
        if outcome == "late-error":
            (self.control / "native-late-error").touch()
        self.save()

    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for pattern in ["As400ReaderFixture*.class", "As400ReaderLifecycleProbeConnector*.class"]:
                for path in classes.rglob(pattern):
                    jar.write(path, str(path.relative_to(classes)))
        dependencies = (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip().split(":")
        libraries = []
        for item in dependencies:
            if any(segment in item for segment in ["/org/mockito/", "/net/bytebuddy/", "/org/objenesis/"]):
                path = Path(item)
                shutil.copy2(path, self.plugin / path.name)
                libraries.append(path.name)
        self.check("test transport instrumentation is packaged", len(libraries) == 4, "plugin/")
        self.agent = next(self.plugin.glob("byte-buddy-agent-*.jar")).name
        sources = ["tools/dcg-reader-lifecycle-probe.py",
                   "journal-parsing/src/main/java/io/debezium/ibmi/db2/journal/retrieve/RetrieveJournal.java",
                   "debezium-connector-ibmi/src/main/java/io/debezium/connector/db2as400/As400RpcConnection.java"] + [
            "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/" + name + ".java"
            for name in ["As400ReaderFixture", "As400ReaderLifecycleProbeConnector", "As400ReaderLifecycleTest"]]
        self.report["harness_sha256"].update({name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["scope"] = "Real incremental RPC/decoder/task/dispatcher; controlled JT400 transport and native job handles; no IBM i"
        self.save()

    def container(self, name, args):
        if (self.directory / (name + ".properties")).exists():
            args = ["-e", "KAFKA_OPTS=-javaagent:/probe/" + self.agent, *args]
        return super().container(name, args)

    def snapshot_all(self, worker, scenario, label, isolation="read_committed"):
        rows = super().snapshot_all(worker, scenario, label, isolation)
        if label == "replacement-complete":
            # Install failure evidence before the additional native race assertions.
            self.failure_context = {"scenario": scenario, "worker": self.prefix + "-partition-w1",
                                    "accepted": self.checkpoint(worker, scenario, "reader-before-late"), "output": rows}
        return rows

    def signal(self, generation, suffix, value=""):
        if suffix == "send-release" and generation.startswith("partition-w1-"):
            self.marker(generation, "native-blocked")
            super().signal(generation, "native-disconnect")
            super().marker(generation, "native-disconnected")
        return super().signal(generation, suffix, value)

    def marker(self, generation, suffix):
        result = super().marker(generation, suffix)
        if suffix == "send-released" and generation.startswith("partition-w1-"):
            # Connect can wait for the producer call to return before doStop().
            # Parent has accepted replacement W=6, released the stale producer and
            # restored the network. Keep the native read held until REAL task close.
            ownership.base.wait_for("real old-reader close cancels its own job", lambda:
                any(e["event"] == "native-stop" for e in self.events(generation)), 60)
            super().signal(generation, "native-release")
            super().marker(generation, "native-finished")
            finished = json.loads((self.control / (generation + ".native-finished.json")).read_text())
            native = [e for e in self.events(generation) if e["event"] == "native"]
            self.check("late native completion retains previous enqueue checkpoint and stopped context",
                       finished["outcome"] == ("error" if self.outcome == "late-error" else "completed") and
                       finished["checkpoint"]["completed_through.sequence"] == "6" and finished["stopped"] and
                       finished["R"] == ("6" if self.outcome == "late-error" else "7"),
                       "control/" + generation + ".native-finished.json")
            self.check("old cancellation uses only its own simulated job; undeclared effects rejected",
                       any(e["fields"]["action"] == "job-end" for e in native) and
                       all(e["fields"]["owner"] == generation and e["fields"]["action"] != "undeclared-action" for e in native),
                       "control/" + generation + ".jsonl")
        return result

    def verify_readers(self):
        starts = list(self.control.glob("partition-*.start.json"))
        self.check("old, replacement and legitimate reassigned task have separate reader instances", len(starts) == 3, "control/")
        for start in starts:
            generation = json.loads(start.read_text())["generation"]
            self.marker(generation, "native-finished")
            native = [e["fields"] for e in self.events(generation) if e["event"] == "native"]
            self.check(generation + ": strict native inventory and completed reader cleanup",
                       any(e["action"] == "read-enter" for e in native) and
                       any(e["action"] == "job-logging" for e in native) and
                       any(e["action"] == "disconnect" for e in native) and
                       not any(e["action"] == "undeclared-action" for e in native), "control/" + generation + ".jsonl")
            copies = sum(e["action"] == "copy-connection" for e in native)
            cancellations = sum(e["action"] == "job-end" for e in native)
            temporary_closes = sum(e["action"] == "disconnect" and e["fields"].get("role") == "cancellation" for e in native)
            reader_opens = sum(e["action"] == "new-connection" for e in native)
            reader_closes = sum(e["action"] == "disconnect" and e["fields"].get("role") == "reader" for e in native)
            old = (self.control / (generation + ".native-disconnected.json")).exists()
            self.check(generation + ": cancellation never reconnects and closes its temporary connection exactly once",
                       reader_opens == reader_closes == 1 and
                       copies == cancellations == temporary_closes == (1 if old else 0),
                       "control/" + generation + ".jsonl")
        self.check("candidate has no custom publication authority or timed-out fault gates",
                   not list(self.control.glob("*.registry*")) and not list(self.control.glob("*.authorized-worker*")) and
                   not list(self.control.glob("*.hook-timeout.json")), "control/")


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in ["late-data", "late-error"]:
        raise SystemExit("Usage: dcg-reader-lifecycle-probe.py NEW_EVIDENCE_DIRECTORY late-data|late-error (compile tests first)")
    experiment = ReaderExperiment(Path(sys.argv[1]), sys.argv[2])
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.fencing(False)
        experiment.verify_readers()
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
        raise SystemExit("Cleanup failed; inspect report.json")
    print(str(experiment.directory / "report.json"), flush=True)


if __name__ == "__main__":
    main()
