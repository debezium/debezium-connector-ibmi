#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Real coordinator/streaming/task stop with a held RPC or metadata call; local only."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("reader_probe", ROOT / "tools/dcg-reader-lifecycle-probe.py")
reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reader)
reader.ownership.CLASS = "io.debezium.connector.db2as400.As400TaskStopProbeConnector"


class TaskStopExperiment(reader.ReaderExperiment):
    def __init__(self, directory, mode):
        super().__init__(directory, "late-error")
        self.mode = mode
        (self.control / "terminal-mode").write_text(mode)
        self.report["terminal_mode"] = mode
        self.report["scope"] = "Real coordinator start/stop/executor/context, streaming retry loop, task doStop and dispatcher; controlled RPC/JDBC; synthetic D/CG startup/consumer"
        self.save()

    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for pattern in ["As400TaskStopProbeConnector*.class", "As400TaskStopFixture*.class"]:
                for path in classes.rglob(pattern):
                    jar.write(path, str(path.relative_to(classes)))
        sources = ["tools/dcg-task-stop-probe.py"] + [
            "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/" + name + ".java"
            for name in ["As400TaskStopProbeConnector", "As400TaskStopFixture", "As400StreamingTaskStopTest"]] + [
            "debezium-connector-ibmi/src/main/java/io/debezium/connector/db2as400/" + name + ".java"
            for name in ["As400StreamingChangeEventSource", "As400ConnectorTask"]]
        self.report["harness_sha256"].update({name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources})
        self.report["scope"] = "Real coordinator start/stop/executor/context, streaming retry loop, task doStop and dispatcher; controlled RPC/JDBC; synthetic D/CG startup/consumer"
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.save()

    def marker(self, generation, suffix):
        # Retain the parent ownership scenario and all its broker/offset checks;
        # replace only the ordinary-close observer with terminal-drain evidence.
        result = reader.catchup.CatchupExperiment.marker(self, generation, suffix)
        if suffix == "send-released" and generation.startswith("partition-w1-"):
            reader.catchup.CatchupExperiment.marker(self, generation, "terminal-requested")
            self.check("terminal request is distinct from completed drain while old work is held",
                       (self.control / (generation + ".coordinator-stop-returned.json")).exists() and
                       not (self.control / (generation + ".terminal-complete.json")).exists() and
                       not (self.control / (generation + ".native-finished.json")).exists(), "control/" + generation + ".jsonl")
            saved = self.checkpoint(self.prefix + "-partition-w2", "partition", "terminal-pending")
            self.check("replacement commits W=6 while old task terminal stop is still pending",
                       saved["completed_through.sequence"] == "6", "control/partition-terminal-pending-offsets-read_committed.json")
            reader.catchup.CatchupExperiment.signal(self, generation, "native-release")
            reader.catchup.CatchupExperiment.marker(self, generation, "terminal-complete")
            reader.catchup.CatchupExperiment.marker(self, generation, "native-finished")
            finished = json.loads((self.control / (generation + ".native-finished.json")).read_text())
            self.check("late old completion cannot advance scan or F/W and sees stopped real coordinator context",
                       finished["outcome"] == "completed" and not finished["contextRunning"] and
                       finished["terminalRequested"] and finished["R"] == "6" and
                       finished["checkpoint"]["completed_through.sequence"] == "6",
                       "control/" + generation + ".native-finished.json")
        return result

    def stop(self, worker):
        # Keep the inherited SIGKILL fault. Process death is not a completed
        # Java drain: record it explicitly instead of waiting for impossible
        # cleanup callbacks from the killed replacement.
        super().stop(worker)
        state = json.loads(self.command("docker", "inspect", "--format", "{{json .State}}", worker))
        self.report.setdefault("worker_terminations", {})[worker] = state
        self.save()

    def verify_readers(self):
        starts = list(self.control.glob("partition-*.start.json"))
        self.check("old, replacement and reassigned tasks have separate reader/JDBC instances", len(starts) == 3, "control/")
        for start in starts:
            generation = json.loads(start.read_text())["generation"]
            if generation.startswith("partition-w2-"):
                state = self.report["worker_terminations"][self.prefix + "-partition-w2"]
                native = [e["fields"] for e in self.events(generation) if e["event"] == "native"]
                self.check(generation + ": abruptly killed replacement is process termination, not a completed Java drain",
                           not state["Running"] and state["ExitCode"] == 137 and
                           not (self.control / (generation + ".terminal-complete.json")).exists() and
                           all(e["owner"] == generation and e["action"] not in ["job-end", "undeclared-action", "jdbc-connect-enter"] for e in native),
                           "report.json:worker_terminations; control/" + generation + ".jsonl")
                continue
            self.marker(generation, "terminal-complete")
            self.marker(generation, "native-finished")
            complete = json.loads((self.control / (generation + ".terminal-complete.json")).read_text())
            finished = json.loads((self.control / (generation + ".native-finished.json")).read_text())
            events = self.events(generation)
            native = [e["fields"] for e in events if e["event"] == "native"]
            self.check(generation + ": actual task cleanup drains and disconnects RPC/JDBC",
                       complete["rpcDisconnected"] and complete["jdbcDisconnected"] and not finished["contextRunning"] and
                       any(e["action"] == "jdbc-close-exit" for e in native) and
                       not any(e["action"] in ["undeclared-action", "jdbc-connect-enter"] for e in native), "control/" + generation + ".jsonl")
            old = (self.control / (generation + ".native-disconnected.json")).exists()
            expected = int(old and self.mode.startswith("rpc"))
            self.check(generation + ": only the captured active RPC job is cancelled, never a metadata/replacement job",
                       sum(e["action"] == "job-end" for e in native) == expected and
                       sum(e["action"] == "copy-connection" for e in native) == expected and
                       sum(e["action"] == "disconnect" and e["fields"].get("role") == "cancellation" for e in native) == expected and
                       all(e["owner"] == generation for e in native), "control/" + generation + ".jsonl")
            stopped = next(i for i, e in enumerate(events) if e["event"] == "stop")
            self.check(generation + ": completed stop is followed by no new source work",
                       not any(e["event"] == "native" and e["fields"]["action"] in [
                           "new-connection", "connect", "read-enter", "position-metadata", "receiver-metadata", "jdbc-connect-enter"]
                           for e in events[stopped + 1:]), "control/" + generation + ".jsonl")
        self.check("no custom publication authority and no expired fault gates",
                   not list(self.control.glob("*.registry*")) and not list(self.control.glob("*.authorized-worker*")) and
                   not list(self.control.glob("*.hook-timeout.json")), "control/")


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in ["rpc-error", "metadata-error"]:
        raise SystemExit("Usage: dcg-task-stop-probe.py NEW_EVIDENCE_DIRECTORY rpc-error|metadata-error")
    experiment = TaskStopExperiment(Path(sys.argv[1]), sys.argv[2])
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
