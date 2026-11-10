#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Finite repeated native workload through the ordinary packaged source connector."""
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("rrn", ROOT / "tools/dcg-rrn-probe.py")
rrn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rrn)
base = rrn.packaged.base


class ReuseFixture(rrn.RrnFixture):
    def setup(self):
        super().setup()
        # Establish the file attribute before native binding acquisition, not
        # through an unqualified live metadata transition after initialization.
        self.sql(f"CALL QSYS2.QCMDEXC('CHGPF FILE({self.library}/U) REUSEDLT(*YES)')", "enable-reuse-U.log", True)
        self.owned_objects = self.inventory("objects-sustained-ready.json")


class SustainedExperiment(rrn.RrnExperiment):
    container_lifetime_seconds = 4800
    cycles = 8
    cycle_rows = 1000
    minimum_cycle_seconds = 45

    def __init__(self, directory, library):
        super().__init__(directory, library)
        self.fixture.__class__ = ReuseFixture
        self.report["sustained"] = {"cycles": self.cycles, "reload_rows_per_cycle": self.cycle_rows,
                                    "minimum_cycle_seconds": self.minimum_cycle_seconds, "cycle_start_deadline_seconds": 2700, "observations": []}
        self.save()

    def cycle_transaction(self, iteration, mode):
        label = "cycle-" + str(iteration) + "-" + mode
        directory = self.control / label
        directory.mkdir()
        (directory / "qualified").write_text("native-" + self.library)
        process, log = self.java(self.controller, ["io.debezium.connector.db2as400.As400NativeConnectTransactions", self.library,
                                 "/control/" + label, mode, str(self.cycle_rows), str(iteration)], label)
        self.native_transactions.append((mode, process, directory))
        def opened():
            if process.poll() is not None:
                raise RuntimeError("Cycle controller exited; see " + str(log))
            return (directory / (mode + "-open.json")).exists()
        base.wait_for(label + " opens", opened, 90)
        return process, directory

    def running(self, worker):
        return any(t["state"] == "RUNNING" for t in self.healthy(worker)["tasks"])

    @staticmethod
    def project(records):
        state = {}
        for record in records:
            value = record["value"]
            if value is None:
                continue
            if value["op"] == "t":
                assert record["key"] is None
                state.clear()
            elif value["op"] == "d":
                assert record["key"] == {"__IBMI_RRN": value["before"]["__IBMI_RRN"]}
                state.pop(record["key"]["__IBMI_RRN"], None)
            elif value["op"] in ["r", "c", "u"]:
                assert record["key"] == {"__IBMI_RRN": value["after"]["__IBMI_RRN"]}
                if value["before"] is not None:
                    assert record["key"] == {"__IBMI_RRN": value["before"]["__IBMI_RRN"]}
                state[record["key"]["__IBMI_RRN"]] = value["after"]
            else:
                raise AssertionError("Unknown source operation")
        return state

    def verify_projection(self, worker):
        super().verify_projection(worker)
        started = time.monotonic()
        previous = self.checkpoint(worker, "native", "sustained-incoming")
        baseline_counts = {t: sum(r["value"] is not None and r["value"]["op"] == "r" for r in self.rows(worker, t, "sustained-baseline")) for t in self.capture}
        expected_b = self.data(self.rows(worker, "B", "sustained-B-incoming"))
        expected_extra = self.data(self.rows(worker, "EXTRA", "sustained-extra-incoming"))
        expected_a = self.data(self.rows(worker, "A", "sustained-A-incoming"))
        for iteration in range(1, self.cycles + 1):
            assert time.monotonic() - started < 2700, "Sustained workload deadline exceeded"
            mode = ["truncate-commit", "delete-rollback", "truncate-rollback", "delete-commit"][(iteration - 1) % 4]
            label = "sustained-" + str(iteration)
            cycle_started_at = time.monotonic()
            process, directory = self.cycle_transaction(iteration, mode)
            expected_a.append(("c", 100000 + iteration))
            self.await_data(worker, "A", expected_a, label + "-open-progress")
            self.check(label + ": unrelated CDC progresses with 1000 uncommitted reload rows",
                       process.poll() is None and self.data(self.rows(worker, "B", label + "-open")) == expected_b, "control/" + label + "-*.json")
            if iteration in [2, 6]:
                self.command("docker", "kill", "--signal=KILL", worker)
                worker = self.start_worker("native", "sustained-" + str(iteration))
                base.wait_for(label + " replacement starts", lambda: self.running(worker), 90)
            if iteration == 4:
                self.command("docker", "pause", self.broker)
                try:
                    # Only this experiment's local Kafka broker is unavailable.
                    self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES ({900000 + iteration})", label + "-during-kafka-outage.log", True)
                    expected_extra.append(("c", 900000 + iteration))
                    time.sleep(5)
                finally:
                    self.command("docker", "unpause", self.broker)
            extra_ids = list(range(iteration * 10000, iteration * 10000 + 50))
            self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES " + ",".join("(" + str(i) + ")" for i in extra_ids), label + "-extra.log", True)
            expected_extra += [("c", i) for i in extra_ids]
            self.await_data(worker, "EXTRA", expected_extra, label + "-extra-progress")
            self.fixture.sql(f"UPDATE {self.library}.M SET S={-100 - iteration} WHERE ID=1", label + "-mixed-update.log", True)
            previous_second_id = 2 if iteration == 1 else 10000 + iteration - 1
            previous_first_id = 1 if iteration == 1 else 20000 + iteration - 1
            self.fixture.sql(f"DELETE FROM {self.library}.U WHERE ID={previous_second_id}", label + "-reuse-delete.log", True)
            self.fixture.sql(f"INSERT INTO {self.library}.U VALUES ({10000 + iteration})", label + "-reuse-insert.log", True)
            self.fixture.sql(f"UPDATE {self.library}.U SET ID={20000 + iteration} WHERE ID={previous_first_id}", label + "-business-key-update.log", True)
            native_u = self.fixture.sql(f"SELECT CAST(RRN(U) AS BIGINT) AS R,ID FROM {self.library}.U U ORDER BY RRN(U)", label + "-native-U.json")
            self.check(label + ": IBM i reused the deleted physical slot", native_u == [{"R": 1, "ID": 20000 + iteration}, {"R": 2, "ID": 10000 + iteration}, {"R": 3, "ID": 3}], "native/" + label + "-native-U.json")
            self.fixture.rotate()
            for path in self.fixture.directory.glob("rotation-*.json"):
                shutil.copyfile(path, self.fixture.directory / (label + "-" + path.name))
            # Includes controller launch/preparation; native open duration is
            # separately observable from its open/finish evidence-file times.
            while time.monotonic() - cycle_started_at < self.minimum_cycle_seconds:
                self.healthy(worker)
                time.sleep(1)
            held = self.checkpoint(worker, "native", label + "-held")
            self.check(label + ": clear remains withheld through the open window and rotation",
                       process.poll() is None and self.data(self.rows(worker, "B", label + "-still-open")) == expected_b
                       and int(held["replay_from.sequence"]) <= int(held["completed_through.sequence"])
                       and not any(k in held for k in ["scan", "R", "position"]), "control/" + label + "-*.json")
            self.finish_transaction(mode, process, directory)
            if mode.endswith("commit"):
                expected_b += [("t", None)] + [("c", i) for i in range(iteration * 10000, iteration * 10000 + self.cycle_rows)]
            self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES ({800000 + iteration})", label + "-terminal-marker.log", True)
            expected_extra.append(("c", 800000 + iteration))
            self.await_data(worker, "EXTRA", expected_extra, label + "-terminal-progress")
            self.await_data(worker, "B", expected_b, label + "-terminal-B")
            saved = self.checkpoint(worker, "native", label + "-completed")
            self.check(label + ": completed output advances with valid F/W and terminal ordering",
                       int(saved["completed_through.sequence"]) > int(previous["completed_through.sequence"])
                       and int(saved["replay_from.sequence"]) <= int(saved["completed_through.sequence"]), "control/" + label + "-*.json")
            previous = saved
            for table in self.capture:
                records = self.rows(worker, table, label + "-projection")
                state = self.project(records)
                actual = self.fixture.sql(f"SELECT CAST(RRN(T) AS BIGINT) AS R,ID FROM {self.library}.{table} T ORDER BY RRN(T)", label + "-native-" + table + ".json")
                self.check(label + ": native row identity and output agree for " + table,
                           sorted((k, v["ID"]) for k, v in state.items()) == [(r["R"], r["ID"]) for r in actual]
                           and sum(r["value"] is not None and r["value"]["op"] == "r" for r in records) == baseline_counts[table],
                           "control/" + label + "-projection-*.json; native/" + label + "-native-" + table + ".json")
                if table == "M":
                    expected = rrn.packaged.continuous.ContinuousExperiment.expected_mixed(1, True)
                    expected["S"] = -100 - iteration
                    self.check(label + ": every mixed-column value remains exact", self.mixed_value(state[1]) == expected
                               and self.mixed_value(state[3]) == rrn.packaged.continuous.ContinuousExperiment.expected_mixed(3), "control/" + label + "-projection-*.json")
            stats = self.command("docker", "stats", "--no-stream", "--format", "{{json .}}", worker, self.broker)
            observation = {"iteration": iteration, "mode": mode, "elapsed_seconds": round(time.monotonic() - started, 3),
                           "checkpoint": saved, "resources": [json.loads(line) for line in stats.splitlines()]}
            self.report["sustained"]["observations"].append(observation)
            self.save()
        self.report["sustained"]["elapsed_seconds"] = round(time.monotonic() - started, 3)
        self.check("all sustained cycles complete with source ordering, recovery and unrelated progress intact", True, "report.json; control/sustained-*.json")
        return worker


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-sustained-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd")
    experiment = SustainedExperiment(Path(sys.argv[1]), sys.argv[2])
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
