#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Native no-primary-key fixture -> packaged source -> independent Kafka audit."""
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("packaged", ROOT / "tools/dcg-packaged-probe.py")
packaged = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packaged)


class RrnFixture(packaged.PackagedFixture):
    def setup(self):
        super().setup()
        for table in self.TABLES:
            self.sql(f"ALTER TABLE {self.library}.{table} DROP PRIMARY KEY", "remove-key-" + table + ".log", True)
            self.sql(f"CALL QSYS2.QCMDEXC('CHGPF FILE({self.library}/{table}) REUSEDLT(*NO)')", "disable-reuse-" + table + ".log", True)
        # A hole before streaming makes the native RRN differ from the business ID.
        self.sql(f"INSERT INTO {self.library}.A VALUES (-1)", "gap-insert.log", True)
        self.sql(f"DELETE FROM {self.library}.A WHERE ID=-1", "gap-delete.log", True)
        keys = self.sql(f"SELECT TABLE_NAME FROM QSYS2.SYSCST WHERE TABLE_SCHEMA='{self.library}' AND CONSTRAINT_TYPE='PRIMARY KEY'", "no-primary-keys.json")
        assert keys == []
        self.owned_objects = self.inventory("objects-rrn-ready.json")


class RrnExperiment(packaged.PackagedExperiment):
    key_field = "__IBMI_RRN"

    def __init__(self, directory, library):
        super().__init__(directory, library)
        self.fixture.__class__ = RrnFixture
        self.report["key_mode"] = "rrn"
        self.save()

    def configure(self, config):
        config["dcg.key.mode"] = "rrn"

    def expected_key(self, value):
        rrn = value["__IBMI_RRN"]
        assert type(rrn) is int and rrn > 0
        return {"__IBMI_RRN": rrn}

    def mixed_value(self, value):
        physical = dict(value)
        assert physical.pop("__IBMI_RRN") > 0
        return super().mixed_value(physical)

    def verify_projection(self, worker):
        # This is a source-output oracle, not a deployed sink dependency.
        receiver = next(r for r in self.fixture.receivers("statistics-receiver.json") if r["STATUS"] == "ATTACHED")
        process, log = self.java(self.controller, ["io.debezium.connector.db2as400.As400PackagedProbeAudit", "native", self.library,
                receiver["JOURNAL_RECEIVER_NAME"], str(receiver["FIRST_SEQUENCE_NUMBER"]), str(receiver["LAST_SEQUENCE_NUMBER"]),
                "/control/statistics-native.json"], "statistics-native")
        if process.wait(timeout=35) != 0:
            raise RuntimeError("Native statistics audit failed: " + str(log))
        raw = json.loads((self.control / "statistics-native.json").read_text())
        self.check("source continues through actual native F/CB statistics requests", any(r["code"] == "F" and r["type"] == "CB" for r in raw),
                   "control/statistics-native.json")
        original = self.rows(worker, "EXTRA", "duplicate-before")
        self.fixture.sql(f"INSERT INTO {self.library}.EXTRA VALUES (21),(21)", "duplicate-values.log", True)
        duplicated = self.await_data(worker, "EXTRA", self.data(original) + [("c", 21), ("c", 21)], "duplicate-values")
        keys = [r["key"][self.key_field] for r in duplicated if r["value"] and r["value"]["after"] and r["value"]["after"]["ID"] == 21]
        self.check("identical source values retain three distinct physical row identities", len(keys) == len(set(keys)) == 3, "control/duplicate-values-*.json")
        for table in self.capture:
            state = {}
            records = self.rows(worker, table, "rrn-final")
            for row in records:
                event = row["value"]
                if event is None:
                    continue
                if event["op"] == "t":
                    assert row["key"] is None
                    state.clear()
                elif event["op"] == "d":
                    assert row["key"] == self.expected_key(event["before"])
                    state.pop(row["key"][self.key_field], None)
                else:
                    assert row["key"] == self.expected_key(event["after"])
                    state[row["key"][self.key_field]] = event["after"]["ID"]
            actual = self.fixture.sql(f'SELECT CAST(RRN(T) AS BIGINT) AS R,ID FROM {self.library}.{table} T ORDER BY RRN(T)', "rrn-native-" + table + ".json")
            self.check("RRN output agrees with native identity after restart, rotation and changes for " + table,
                       sorted(state.items()) == [(r["R"], r["ID"]) for r in actual], "control/rrn-final-*.json; native/rrn-native-" + table + ".json")
        rows = self.rows(worker, "A", "rrn-gaps")
        eight = next(r for r in rows if r["value"] and r["value"]["after"] and r["value"]["after"]["ID"] == 8)
        self.check("native deleted slot cannot turn business value into row identity", eight["key"] == {"__IBMI_RRN": 5}, "control/rrn-gaps-*.json")


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-rrn-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd")
    experiment = RrnExperiment(Path(sys.argv[1]), sys.argv[2])
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
