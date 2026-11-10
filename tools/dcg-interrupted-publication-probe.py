#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Kill an owned normal worker after a committed truncate, during native batch output."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("sustained", ROOT / "tools/dcg-sustained-probe.py")
sustained = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sustained)
base = sustained.base
rrn = sustained.rrn


class InterruptedPublication(rrn.RrnExperiment):
    cycle_rows = 5000
    cycle_transaction = sustained.SustainedExperiment.cycle_transaction
    project = staticmethod(sustained.SustainedExperiment.project)

    def offset_state(self, label):
        self.command("docker", "exec", self.controller, "java", "-cp", "/audit/*:/opt/kafka/libs/*",
                     "io.debezium.connector.db2as400.As400PackagedProbeAudit", "offset-state", "/control/" + label + ".json", timeout=35)
        return json.loads((self.control / (label + ".json")).read_text())

    def wait_worker_membership(self, worker, identifier):
        if identifier != "native-publication-replacement":
            return super().wait_worker_membership(worker, identifier)
        started = time.monotonic()
        observed = False
        # An abruptly killed source producer can leave an offsets transaction
        # open for 180 seconds, plus the broker's 10-second abort sweep. Keep the
        # original 100-second reproduction and measure this distinct wait.
        def joined():
            nonlocal observed
            if "Successfully joined group" in self.command("docker", "logs", worker):
                return True
            if not observed and time.monotonic() - started >= 100:
                self.offset_state("replacement-blocked-offset-state")
                threads = self.thread_dump(worker)
                (self.directory / "replacement-blocked-threads.txt").write_text(self.fixture.redact(threads))
                observed = True
            return False
        base.wait_for("replacement membership after transaction expiration", joined, 240)
        self.report["replacement_membership_seconds"] = time.monotonic() - started
        self.offset_state("replacement-joined-offset-state")
        self.save()

    def verify_projection(self, worker):
        super().verify_projection(worker)
        before = self.rows(worker, "B", "interrupted-before")
        previous = self.checkpoint(worker, "native", "interrupted-incoming")
        expected_a = self.data(self.rows(worker, "A", "interrupted-A")) + [("c", 100001)]
        process, directory = self.cycle_transaction(1, "truncate-commit")
        self.await_data(worker, "A", expected_a, "interrupted-open-progress")
        def durable_open_progress():
            self.healthy(worker)
            offset = self.checkpoint(worker, "native", "interrupted-held")
            return offset if int(offset["completed_through.sequence"]) > int(previous["completed_through.sequence"]) else None
        held = base.wait_for("unrelated progress durably acknowledged", durable_open_progress, 90)
        self.check("5000-row open reload does not block unrelated committed output", process.poll() is None
                   and self.rows(worker, "B", "interrupted-still-open") == before, "control/interrupted-*.json")
        trigger = self.control / "committed-truncate-trigger.json"
        errors = []
        killed = threading.Event()
        cancel = threading.Event()
        def kill_after_trigger():
            try:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline and not trigger.exists() and not cancel.is_set():
                    time.sleep(.01)
                if cancel.is_set():
                    return
                if not trigger.exists():
                    raise RuntimeError("No committed truncate trigger")
                self.command("docker", "kill", "--signal=KILL", worker)
                killed.set()
            except BaseException as error:
                errors.append(str(error))
        killer = threading.Thread(target=kill_after_trigger, daemon=True)
        killer.start()
        log = (self.directory / "truncate-trigger.log").open("w")
        audit = subprocess.Popen(["docker", "exec", self.controller, "java", "-cp", "/audit/*:/opt/kafka/libs/*",
                                  "io.debezium.connector.db2as400.As400PackagedProbeAudit", "truncate-trigger", "broker:9092",
                                  f"packaged.{self.library}.B", str(before[-1]["offset"] + 1), "/control/" + trigger.name], stdout=log, stderr=subprocess.STDOUT)
        self.children.append(audit)
        try:
            base.wait_for("independent truncate consumer ready", lambda: Path(str(trigger) + ".ready").exists(), 25)
            self.finish_transaction("truncate-commit", process, directory)
            killer.join(timeout=125)
            assert killed.is_set() and not errors, errors
            assert audit.wait(timeout=5) == 0
        finally:
            cancel.set()
            killer.join(timeout=2)
            log.close()
        partial = self.rows(self.controller, "B", "interrupted-prefix")
        tail = self.data(partial)[len(self.data(before)):]
        self.check("worker died after committed truncate and before complete native reload",
                   1 <= len(tail) < self.cycle_rows + 1 and tail == [("t", None)] + [("c", i) for i in range(10000, 10000 + len(tail) - 1)],
                   "control/committed-truncate-trigger.json; control/interrupted-prefix-*.json")
        saved = self.checkpoint(self.controller, "native", "interrupted-durable")
        self.offset_state("interrupted-offset-state")
        self.check("interrupted native batch retains previous durable completed-output boundary",
                   saved["completed_through.sequence"] == held["completed_through.sequence"]
                   and saved["completed_through.receiver"] == held["completed_through.receiver"]
                   and int(saved["replay_from.sequence"]) <= int(saved["completed_through.sequence"]), "control/interrupted-*-offsets-native.json")
        expected_extra = self.data(self.rows(self.controller, "EXTRA", "interrupted-extra-before")) + [("c", 777777)]
        self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES (777777)", "interrupted-later.log", True)
        replacement = self.start_worker("native", "publication-replacement")
        expected = self.data(partial) + [("t", None)] + [("c", i) for i in range(10000, 10000 + self.cycle_rows)]
        completed = self.await_data(replacement, "B", expected, "interrupted-replayed")
        self.check("fresh normal task replays the complete native clear/reload after committed prefix",
                   self.data(completed) == expected, "control/interrupted-replayed-*.json")
        self.await_data(replacement, "EXTRA", expected_extra, "interrupted-later-progress")
        final = self.checkpoint(replacement, "native", "interrupted-final")
        self.check("recovered reader catches up to later output and advances durable W",
                   int(final["completed_through.sequence"]) > int(saved["completed_through.sequence"]), "control/interrupted-final-offsets-native.json")
        state = self.project(completed)
        actual = self.fixture.sql(f"SELECT CAST(RRN(T) AS BIGINT) AS R,ID FROM {self.library}.B T ORDER BY RRN(T)", "interrupted-native-B.json")
        self.check("replayed native table converges despite the committed duplicate clear prefix",
                   sorted((k, v["ID"]) for k, v in state.items()) == [(r["R"], r["ID"]) for r in actual],
                   "native/interrupted-native-B.json; control/interrupted-replayed-*.json")
        before_u = self.data(self.rows(replacement, "U", "native-clear-before"))
        self.fixture.sql(f"CALL QSYS2.QCMDEXC('CLRPFM FILE({self.library}/U) MBR(U)')", "native-clear.log", True)
        self.fixture.sql(f"INSERT INTO {self.library}.U VALUES (71),(72)", "native-clear-reload.log", True)
        cleared = self.await_data(replacement, "U", before_u + [("t", None), ("c", 71), ("c", 72)], "native-clear-after")
        receiver = next(r for r in self.fixture.receivers("native-clear-receiver.json") if r["STATUS"] == "ATTACHED")
        assert receiver["JOURNAL_RECEIVER_NAME"] == final["completed_through.receiver"]
        audit, native_log = self.java(self.controller, ["io.debezium.connector.db2as400.As400PackagedProbeAudit", "native", self.library,
                                     receiver["JOURNAL_RECEIVER_NAME"], final["completed_through.sequence"], str(receiver["LAST_SEQUENCE_NUMBER"]),
                                     "/control/native-clear-raw.json"], "native-clear-audit")
        assert audit.wait(timeout=35) == 0, str(native_log)
        raw = json.loads((self.control / "native-clear-raw.json").read_text())
        native_u = self.fixture.sql(f"SELECT CAST(RRN(T) AS BIGINT) AS R,ID FROM {self.library}.U T ORDER BY RRN(T)", "native-clear-final-U.json")
        self.check("normal configured task also preserves native F/CR clear and reload after recovery",
                   any(r["code"] == "F" and r["type"] == "CR" for r in raw)
                   and sorted((k, v["ID"]) for k, v in self.project(cleared).items()) == [(r["R"], r["ID"]) for r in native_u],
                   "control/native-clear-*.json; native/native-clear-final-U.json")
        self.report["interrupted_publication"] = {"reload_rows": self.cycle_rows, "committed_prefix_rows": len(tail) - 1,
                                                   "durable_before": held, "durable_interrupted": saved, "durable_after": final}
        self.save()
        return replacement


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-interrupted-publication-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd")
    experiment = InterruptedPublication(Path(sys.argv[1]), sys.argv[2])
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
