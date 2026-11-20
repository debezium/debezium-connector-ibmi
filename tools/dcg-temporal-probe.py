#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Native temporal variant of the packaged connector experiment; same guards and cleanup."""
import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

spec = importlib.util.spec_from_file_location("packaged", Path(__file__).with_name("dcg-packaged-probe.py"))
packaged = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packaged)

FIELDS = ["DT", "TM", "TS0", "TS3", "TS6", "TS9"]


class TemporalFixture(packaged.PackagedFixture):
    def setup(self):
        super().setup()
        lib = self.library
        self.sql(f"ALTER TABLE {lib}.M ADD COLUMN DT DATE ADD COLUMN TM TIME ADD COLUMN TS0 TIMESTAMP(0) ADD COLUMN TS3 TIMESTAMP(3) ADD COLUMN TS6 TIMESTAMP(6) ADD COLUMN TS9 TIMESTAMP(9)", "add-temporal.log", True)
        self.owned_objects = self.inventory("objects-temporal.json")
        self.sql(f"UPDATE {lib}.M SET DT=DATE('2024-02-29'),TM=TIME('23:59:59'),TS0=TIMESTAMP('2024-02-29 23:59:59'),TS3=TIMESTAMP('2024-02-29 23:59:59.123'),TS6=TIMESTAMP('2024-02-29 23:59:59.123456'),TS9=CAST('2024-02-29 23:59:59.123456789' AS TIMESTAMP(9)) WHERE ID=1", "temporal-seed-1.log", True)
        self.sql(f"UPDATE {lib}.M SET DT=DATE('0001-01-01'),TM=TIME('24:00:00'),TS0=TIMESTAMP('0001-01-01 00:00:00'),TS3=TIMESTAMP('0001-01-01 00:00:00.123'),TS6=TIMESTAMP('0001-01-01 00:00:00.123456'),TS9=CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP(9)) WHERE ID=3", "temporal-seed-3.log", True)
        self.sql(f"SELECT COLUMN_NAME,DATA_TYPE,LENGTH,STORAGE,BUFFER_LENGTH,BUFFER_POSITION,CCSID,DATETIME_PRECISION,DATE_FORMAT,DATE_SEPARATOR,TIME_FORMAT,TIME_SEPARATOR FROM QSYS2.SYSCOLUMNS2 WHERE TABLE_SCHEMA='{lib}' AND TABLE_NAME='M' ORDER BY ORDINAL_POSITION", "temporal-columns.json")


def expected_temporal(identifier):
    if identifier >= 200:
        identifier -= 200
    epoch = datetime.datetime(1970, 1, 1)
    if identifier == 2:
        return dict.fromkeys(FIELDS)
    date = datetime.datetime(2024, 2, 29, 23, 59, 59) if identifier == 1 else datetime.datetime(1, 1, 1)
    seconds = (date - epoch).days * 86400 + (date - epoch).seconds
    return {"DT": (date.date() - epoch.date()).days,
            "TM": 86399000000 if identifier == 1 else 86400000000,
            "TS0": seconds * 1000, "TS3": seconds * 1000 + 123,
            "TS6": seconds * 1000000 + 123456,
            "TS9": seconds * 1000000000 + 123456789 if identifier == 1 else -1}


class TemporalExperiment(packaged.PackagedExperiment):
    def __init__(self, directory, library):
        super().__init__(directory, library)
        self.fixture.__class__ = TemporalFixture
        self.report["temporal_columns"] = FIELDS
        self.save()

    def stage(self):
        super().stage()
        self.report["harness_sha256"]["tools/dcg-temporal-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def expected_mixed(self, identifier, changed=False):
        return dict(super().expected_mixed(identifier, changed), **expected_temporal(identifier))

    def verify_projection(self, worker):
        before = self.data(self.rows(worker, "M", "temporal-before"))
        unrelated = self.data(self.rows(worker, "A", "temporal-unrelated-before"))
        for mode, marker in [("temporal-commit", 208), ("temporal-rollback", 209)]:
            process, directory = self.start_transaction(mode)
            unrelated += [("c", marker)]
            self.await_data(worker, "A", unrelated, mode + "-unrelated")
            self.check(mode + ": temporal clear withheld while unrelated CDC advances", process.poll() is None
                       and self.data(self.rows(worker, "M", mode + "-held")) == before, "control/")
            if mode == "temporal-commit":
                saved = self.checkpoint(worker, "native", "temporal-open")
                self.command("docker", "kill", "--signal=KILL", worker)
                worker = self.start_worker("native", "w3")
                packaged.base.wait_for("temporal replacement running", lambda: any(t["state"] == "RUNNING" for t in self.healthy(worker)["tasks"]), 90)
                self.fixture.sql(f"INSERT INTO {self.library}.A VALUES (210)", "temporal-replacement-progress.log", True)
                unrelated += [("c", 210)]
                self.await_data(worker, "A", unrelated, "temporal-replacement-progress")
                resumed = self.checkpoint(worker, "native", "temporal-resumed")
                self.check("temporal restart preserves F and advances W without exposing open clear", process.poll() is None
                           and all(saved[k] == resumed[k] for k in saved if k.startswith("replay_from."))
                           and int(resumed["completed_through.sequence"]) >= int(saved["completed_through.sequence"])
                           and self.data(self.rows(worker, "M", "temporal-resumed")) == before, "control/")
            self.finish_transaction(mode, process, directory)
            # A new unrelated record proves the reader crossed the terminal boundary.
            self.fixture.sql(f"INSERT INTO {self.library}.A VALUES ({marker + 10})", mode + "-terminal-progress.log", True)
            unrelated += [("c", marker + 10)]
            self.await_data(worker, "A", unrelated, mode + "-terminal-progress")
            if mode == "temporal-commit":
                before += [("t", None)] + [("c", i) for i in [201, 202, 203]]
            self.await_data(worker, "M", before, mode + "-outcome")
            self.check(mode + ": exact temporal output follows commit or rollback", True, "control/")
        receiver = next(r for r in self.fixture.receivers("temporal-receiver.json") if r["STATUS"] == "ATTACHED")
        process, log = self.java(self.controller, ["io.debezium.connector.db2as400.As400PackagedProbeAudit", "native", self.library,
                receiver["JOURNAL_RECEIVER_NAME"], str(receiver["FIRST_SEQUENCE_NUMBER"]), str(receiver["LAST_SEQUENCE_NUMBER"]),
                "/control/temporal-raw.json"], "temporal-native-audit")
        if process.wait(timeout=45) != 0:
            raise RuntimeError("Temporal raw audit failed: " + str(log))
        raw = json.loads((self.control / "temporal-raw.json").read_text())
        self.check("raw temporal transaction evidence retained", sum(r["code"] == "D" and r["type"] == "CG" for r in raw) == 2, "control/temporal-raw.json")
        super().verify_projection(worker)
        rows = self.rows(worker, "M", "temporal-wire")
        for record in rows:
            value = record["value"]
            if value is None:
                continue
            for image in ["before", "after"]:
                row = value.get(image)
                if row is not None:
                    self.check("temporal " + value["op"] + "/" + image + " ID=" + str(row["ID"]),
                               {k: row[k] for k in FIELDS} == expected_temporal(row["ID"]), "control/temporal-wire-*.json")
        return worker


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-temporal-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd")
    experiment = TemporalExperiment(Path(sys.argv[1]), sys.argv[2])
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.run_native()
    except BaseException as error:
        experiment.report.update(status="failed", error=experiment.fixture.redact(str(error)))
        experiment.save()
        experiment.source_failure_evidence()
        raise
    finally:
        experiment.cleanup()
    assert experiment.report["status"] == "passed"
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
