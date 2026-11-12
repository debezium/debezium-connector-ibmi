#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Opt-in native regression: batched scope, excluded layouts, keyless rows and RRN.

Uses the ordinary packaged connector and local Kafka. Requires IBMI_TEST_CONFIG
and a new owned DCNCdddd library; never selects or changes an existing fixture.
"""
import importlib.util
import json
from pathlib import Path
import sys
import time
import traceback

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("public_scope", Path(__file__).with_name("dcg-public-online-probe.py"))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Fixture(public.packaged.continuous.MixedFixture):
    def setup(self):
        super().setup()
        self.sql(f"ALTER TABLE {self.library}.M DROP PRIMARY KEY", "remove-primary-key.log", True)
        self.owned_objects = self.inventory("objects-keyless.json")
        self.sql(f"ALTER TABLE {self.library}.U ADD COLUMN TEXT_VALUE GRAPHIC(10)", "excluded-layout.log", True)
        self.owned_objects = self.inventory("objects-excluded.json")
        self.TABLES = list(self.TABLES)
        for index in range(52):
            table = f"F{index:04d}"
            self.sql(f"CREATE TABLE {self.library}.{table} (ID INTEGER NOT NULL)", f"create-{table}.log", True)
            self.TABLES.append(table)
            self.owned_objects = self.inventory(f"objects-{table}.json")
        counts = self.sql(f"SELECT JOURNALED_FILES,JOURNALED_MEMBERS FROM QSYS2.JOURNAL_INFO "
                          f"WHERE JOURNAL_LIBRARY='{self.library}' AND JOURNAL_NAME='QSQJRN'", "scope-counts.json")
        assert counts == [{"JOURNALED_FILES": 56, "JOURNALED_MEMBERS": 56}], counts


class Experiment(public.Experiment):
    audit_class = "io.debezium.connector.db2as400.As400PackagedProbeAudit"
    audit_rows_per_write = 1
    audit_interval_seconds = 15

    def wait_with_writer(self, label, predicate, seconds, writer):
        started = time.monotonic()
        last = started

        def observe():
            nonlocal last
            if time.monotonic() - last >= self.audit_interval_seconds:
                key = 1001 + len(self.writer_rows)
                keys = list(range(key, key + self.audit_rows_per_write))
                values = ",".join(f"({value})" for value in keys)
                self.capture.execute(writer, "ongoing-unrelated-write", f"INSERT INTO {self.library}.A VALUES {values}")
                self.writer_rows.extend(keys)
                last = time.monotonic()
            return predicate()

        result = public.base.wait_for(label, observe, seconds)
        self.report.setdefault("phase_waits", []).append({"phase": label, "seconds": time.monotonic() - started, "limit_seconds": seconds})
        self.save()
        return result

    def inserted(self, table, key, label):
        def observed():
            records = [r for r in self.table_records(table, label)
                       if r["value"] and r["value"]["op"] == "c" and r["value"]["after"]["ID"] == key]
            if len(records) > 1:
                raise ValueError("Duplicate fixture insert")
            return records[0] if records else None
        return public.base.wait_for(label, observed, 150)

    def raw_interval(self, start, label):
        if self.audit_rows_per_write == 1:
            return super().raw_interval(start, label)

        def observe(session):
            end = public.live.prospective.boundary.point(session[0], self.library)
            sql = public.live.prospective.boundary.AUDIT_SQL.replace("FETCH FIRST 2001", "FETCH FIRST 2000")
            lower = int(start["sequence"])
            pages, count = [], 0
            while lower < int(end["sequence"]):
                params = [self.library, "QSQJRN", start["library"], start["receiver"], lower + 1,
                          end["library"], end["receiver"], end["sequence"], lower]
                rows = public.live.prospective.census.rows(session[0], sql, params, 2000)
                for row in rows:
                    sequence = int(row["SEQUENCE_NUMBER"])
                    if sequence <= lower or sequence > int(end["sequence"]) or row["DATA_LENGTH"] > 16000:
                        raise ValueError("Invalid or truncated native audit page")
                    lower = sequence
                if not rows:
                    raise ValueError("Incomplete native audit endpoint")
                count += len(rows)
                if count > 60000:
                    raise ValueError("Fixture audit exceeds its evidence budget")
                page = f"{label}-raw-{len(pages):04d}.json"
                public.live.prospective.census.save(self.directory / page, {"sql": sql, "parameters": params, "rows": rows})
                pages.append(page)
            public.live.prospective.census.save(self.directory / (label + "-raw.json"),
                                               {"from_exclusive": start, "through": end, "entries": count, "pages": pages})
        return self.read_native(observe)

    def exercise(self):
        exp = self.capture
        exp.deadline = None
        worker = self.start_worker("automatic", "w1")
        baseline = self.setup_connector(worker)
        self.audit_start = self.point()
        self.check("Keyless legacy snapshot preserves null keys and has no journal RRN",
                   all(r["key"] is None and r["value"]["source"]["rrn"] is None for r in baseline["M"]), "control/")
        self.request(worker, "automatic", "PUT", "/resume")
        writer = exp.connection()
        self.writer_rows = []
        exp.execute(writer, "legacy-keyless", f"INSERT INTO {self.library}.M (ID) VALUES (20)")
        legacy = self.inserted("M", 20, "legacy-keyless")
        self.check("Legacy row exposes string RRN without changing its null key",
                   legacy["key"] is None and int(legacy["value"]["source"]["rrn"]) > 0, "control/")
        public.base.wait_for("legacy durable row", lambda: int(self.checkpoint(self.observer_container, "automatic", "legacy-row")
                                                              .get("offset.event_sequence", 0)) >= int(legacy["value"]["source"]["sequence"]), 90)
        self.request(worker, "automatic", "PUT", "/stop")
        public.base.wait_for("legacy stopped", lambda: self.rest(worker, "/connectors/automatic/status")["connector"]["state"] == "STOPPED", 60)
        # U sorts into the second allocation group. A refusal must release the
        # already-acquired first group while ordinary CDC remains available.
        busy = exp.connection(transaction=True)
        exp.execute(busy, "busy-excluded", f"INSERT INTO {self.library}.U (ID,TEXT_VALUE) VALUES (90,GRAPHIC('held'))")
        self.request(worker, "automatic", "PUT", "/config", self.configuration(True))
        self.request(worker, "automatic", "PUT", "/resume")
        exp.execute(writer, "unrelated-legacy", f"INSERT INTO {self.library}.A VALUES (1000)")
        self.inserted("A", 1000, "unrelated-legacy")
        self.wait_with_writer("native refusal", lambda: "boundary" in self.logs[worker].read_text()
                              and "deferred" in self.logs[worker].read_text(), 600, writer)
        allocations = self.fixture.sql(f"SELECT JOB_NAME,SYSTEM_OBJECT_NAME FROM QSYS2.OBJECT_LOCK_INFO "
                                       f"WHERE SYSTEM_OBJECT_SCHEMA='{self.library}' AND LOCK_STATE='*EXCLRD' AND LOCK_STATUS='HELD'",
                                       "busy-allocations.json")
        self.check("Later-batch refusal releases earlier allocations and keeps legacy CDC progressing",
                   not allocations and "offset.version" not in self.checkpoint(self.observer_container, "automatic", "busy"), "busy-allocations.json")
        exp.execute(busy, "rollback"); self.close_session(busy)
        self.wait_with_writer("handoff-complete", lambda: "Transactional truncate test: handoff-complete" in self.logs[worker].read_text(), 360, writer)
        def all_ongoing_rows():
            records = self.table_records("A", "ongoing-unrelated-write")
            expected = set(self.writer_rows)
            actual = [r["value"]["after"]["ID"] for r in records
                      if r["value"] and r["value"]["op"] == "c" and r["value"]["after"]["ID"] in expected]
            if len(actual) != len(set(actual)):
                raise ValueError("Duplicate ongoing fixture row")
            return set(actual) == expected
        public.base.wait_for("all ongoing rows delivered", all_ongoing_rows, 150)
        if self.audit_rows_per_write > 1:
            import re
            counts = [int(value) for value in re.findall(r"Validated transactional truncate boundary across ([0-9]+) raw journal entries",
                                                       self.logs[worker].read_text())]
            self.check("Native boundary audit crosses the old 2000-entry ceiling while writers continue",
                       bool(counts) and max(counts) > 2000, self.logs[worker].name)
            self.report["validated_audit_entries"] = counts
        self.check("56-file scope activates with an excluded GRAPHIC layout and keyless captured table", True, self.logs[worker].name)
        exp.execute(writer, "excluded-autocommit", f"INSERT INTO {self.library}.U (ID,TEXT_VALUE) VALUES (91,GRAPHIC('ignored'))")
        transaction = exp.connection(transaction=True)
        before = self.table_records("M", "before-clear")
        exp.execute(transaction, "keyless-clear", f"TRUNCATE TABLE {self.library}.M REUSE STORAGE IGNORE DELETE TRIGGERS")
        exp.execute(transaction, "keyless-reload", f"INSERT INTO {self.library}.M (ID) VALUES (220)")
        exp.execute(writer, "unrelated-open", f"INSERT INTO {self.library}.A VALUES (310)")
        self.row_progress(self.inserted("A", 310, "unrelated-open"), "unrelated-open")
        self.check("Unrelated table continues while keyless clear is open", self.table_records("M", "open") == before, "control/")
        history = self.records(self.observer_container, "bindings-automatic", "history-before-restart")
        self.command("docker", "kill", worker)
        replacement = self.start_worker("automatic", "w2")
        self.lifecycle(replacement, "recovered")
        self.check("Restart recovers identity-only bindings without a new snapshot or history",
                   self.table_records("M", "recovered") == before
                   and self.records(self.observer_container, "bindings-automatic", "history-recovered") == history, "control/")
        start = self.point(); exp.execute(transaction, "commit")
        self.progressed(self.terminal(start, transaction[1], "CM", "keyless-commit"), "keyless-commit")
        after = self.table_records("M", "committed")
        tail = after[len(before):]
        self.check("Committed keyless truncate precedes reload with row RRN only",
                   [r["value"]["op"] for r in tail] == ["t", "c"] and all(r["key"] is None for r in tail)
                   and tail[0]["value"]["source"]["rrn"] is None and tail[1]["value"]["source"]["rrn"] == "1", "control/")
        exp.execute(transaction, "rollback-clear", f"TRUNCATE TABLE {self.library}.M REUSE STORAGE IGNORE DELETE TRIGGERS")
        exp.execute(transaction, "rollback-reload", f"INSERT INTO {self.library}.M (ID) VALUES (221)")
        start = self.point(); exp.execute(transaction, "rollback")
        self.progressed(self.terminal(start, transaction[1], "RB", "keyless-rollback"), "keyless-rollback")
        self.check("Keyless rollback emits neither truncate nor row", self.table_records("M", "rolled-back") == after, "control/")
        exp.execute(transaction, "excluded-clear", f"TRUNCATE TABLE {self.library}.U REUSE STORAGE IGNORE DELETE TRIGGERS")
        exp.execute(transaction, "excluded-reload", f"INSERT INTO {self.library}.U (ID,TEXT_VALUE) VALUES (92,GRAPHIC('ignored'))")
        exp.execute(transaction, "included-same-transaction", f"INSERT INTO {self.library}.A VALUES (311)")
        exp.execute(transaction, "commit")
        self.row_progress(self.inserted("A", 311, "mixed-selection"), "mixed-selection")
        topics = self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list").splitlines()
        self.check("Excluded column values and clears never create an output topic",
                   f"test.{self.fixture.server_name}.{self.library}.U" not in topics, "control/")
        self.raw_interval(self.audit_start, "scope-transactions")
        self.close_session(transaction); self.close_session(writer)
        self.request(replacement, "automatic", "PUT", "/stop"); self.lifecycle(replacement, "drained")
        self.fixture.verify_rows({"A": [1, 2, 3, 10, 310, 311, 1000] + self.writer_rows, "B": [1, 2, 3], "M": [220], "U": [92]})
        self.report.update(status="passed", journaled_files=56, keyless_table="M", excluded_unsupported_layout="GRAPHIC")
        self.save()


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] != "--audit-load":
        raise SystemExit("Usage: dcg-online-scope-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd [--audit-load]")
    import pyodbc
    pyodbc.pooling = False
    root = Path(sys.argv[1]).resolve(); root.mkdir(exist_ok=False)
    capture = public.live.prospective.census.Experiment(root / "native", sys.argv[2], 1)
    capture.fixture.__class__ = Fixture
    experiment = None
    try:
        capture.fixture.setup()
        experiment = Experiment(root / "connect", capture)
        if len(sys.argv) == 4:
            experiment.audit_rows_per_write = 100
            experiment.audit_interval_seconds = 1
        experiment.stage(); experiment.start_broker(); experiment.observer(); experiment.exercise()
        capture.report.update(status="passed", source_connected=True)
    except BaseException as error:
        (root / "failure.txt").write_text(capture.fixture.redact(traceback.format_exc()))
        capture.report.update(status="failed", error=capture.fixture.redact(str(error)))
        if experiment:
            experiment.report.update(status="failed", error=capture.fixture.redact(str(error)))
            experiment.save(); experiment.retain_failure_evidence()
    finally:
        public.live.cleanup_owned(experiment, capture)
    if not experiment or experiment.report.get("status") != "passed" or capture.report.get("fixture_cleanup", {}).get("status") != "passed":
        raise SystemExit("Native scope validation failed; see retained evidence and cleanup result")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
