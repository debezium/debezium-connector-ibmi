#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Excluded native CLOB tables coexist with captured CDC; isolated IBM i and Kafka only.

This validates identity/transaction handling, not capture of external LOB values.
The ordinary plugin reads and releases its own native pointer handles.
"""
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("lob_public", Path(__file__).with_name("dcg-public-online-probe.py"))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)


class Fixture(public.packaged.continuous.MixedFixture):
    def setup(self):
        super().setup()
        self.TABLES = [*self.TABLES, "X", "S"]
        self.sql(f"CREATE TABLE {self.library}.X (ID INTEGER NOT NULL PRIMARY KEY, BODY CLOB(1M) ALLOCATE(1024))",
                 "create-excluded-lob.log", True)
        self.owned_objects = self.inventory("objects-with-excluded-lob.json")
        self.sql(f"SELECT SYSTEM_TABLE_NAME,CONTAINS_LOB,FORMAT_NAME,FORMAT_LEVEL_ID,RECORD_LENGTH,NUMBER_FIELDS "
                 f"FROM QSYS2.SYSFILES WHERE SYSTEM_TABLE_SCHEMA='{self.library}' AND SYSTEM_TABLE_NAME='X'", "lob-format.json")
        self.sql(f"CALL QSYS2.QCMDEXC('CRTSRCPF FILE({self.library}/S) RCDLEN(112) MBR(*FILE) CCSID(37)')", "create-source-file.log", True)
        self.owned_objects = self.inventory("objects-with-source-file.json")
        journal = self.sql(f"SELECT JOURNAL_LIBRARY,JOURNAL_NAME,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{self.library}','*FILE','S')) X",
                           "source-journaling-before.json")
        if journal == [{"JOURNAL_LIBRARY": None, "JOURNAL_NAME": None, "JOURNAL_IMAGES": None}]:
            self.sql(f"CALL QSYS2.QCMDEXC('STRJRNPF FILE({self.library}/S) JRN({self.library}/QSQJRN) IMAGES(*BOTH) OMTJRNE(*NONE)')",
                     "journal-source-file.log", True)
        assert self.sql(f"SELECT JOURNAL_LIBRARY,JOURNAL_NAME,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{self.library}','*FILE','S')) X",
                        "source-journaling.json") == [{"JOURNAL_LIBRARY": self.library, "JOURNAL_NAME": "QSQJRN", "JOURNAL_IMAGES": "*BOTH"}]
        self.sql(f"SELECT NATIVE_TYPE,FILE_TYPE,NUMBER_MEMBERS,FORMAT_NAME,RECORD_LENGTH,NUMBER_FIELDS "
                 f"FROM QSYS2.SYSFILES WHERE SYSTEM_TABLE_SCHEMA='{self.library}' AND SYSTEM_TABLE_NAME='S'", "source-format.json")


class Experiment(public.Experiment):
    def start_worker(self, scenario, role):
        self.current_worker = super().start_worker(scenario, role)
        return self.current_worker

    def inserted(self, table, key, label):
        def observed():
            if any(task["state"] == "FAILED" for task in self.rest(self.current_worker, "/connectors/automatic/status")["tasks"]):
                raise RuntimeError("Native task failed; preserve worker log and independent broker evidence")
            rows = [row for row in self.table_records(table, label)
                    if row["value"] and row["value"]["op"] == "c" and row["key"]["ID"] == key]
            if len(rows) > 1:
                raise ValueError("Duplicate fixture output")
            return rows[0] if rows else None
        return public.base.wait_for(label, observed, 150)

    def configuration(self, enabled):
        config = super().configuration(enabled)
        if enabled:
            config["truncate.buffer.storage"] = "disk" if self.source_offsets else "memory"
        return config

    def captured(self, table, label):
        return [row for row in self.table_records(table, label) if row["value"] and row["value"]["op"] != "r"]

    def alter_fixture(self):
        session = self.capture.connection()
        try:
            # INTEGER -> REAL requires IBM i's data-loss acknowledgement. Like
            # the schema-refresh probe, scope it to this owned job and restore
            # it before close; never change a shared QAQQINI or reply list.
            self.capture.execute(session, "alter-options-start", "CALL QSYS2.OVERRIDE_QAQQINI(1)")
            try:
                self.capture.execute(session, "alter-options", "CALL QSYS2.OVERRIDE_QAQQINI(2, 'SUPPRESS_INQUIRY_MESSAGES', '*YES')")
                self.capture.execute(session, "alter-captured", f"ALTER TABLE {self.library}.A ALTER COLUMN ID SET DATA TYPE REAL")
            finally:
                self.capture.execute(session, "alter-options-reset", "CALL QSYS2.OVERRIDE_QAQQINI(3)")
        finally:
            self.close_session(session)

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker("automatic", "w1")
        baseline = self.setup_connector(worker)
        writer = self.capture.connection()
        self.request(worker, "automatic", "PUT", "/resume")
        self.capture.execute(writer, "legacy-lob", f"INSERT INTO {self.library}.X VALUES (1,REPEAT(CAST('x' AS CLOB(1)),60000))")
        self.capture.execute(writer, "legacy-source-file", f"INSERT INTO {self.library}.S VALUES (1,0,'legacy')")
        self.capture.execute(writer, "legacy-row", f"INSERT INTO {self.library}.A VALUES (700)")
        legacy = self.inserted("A", 700, "legacy-row")
        public.base.wait_for("legacy durable output", lambda: int((self.checkpoint(self.observer_container, "automatic", "legacy") or {})
                            .get("offset.event_sequence", 0)) >= int(legacy["value"]["source"]["sequence"]), 90)
        self.request(worker, "automatic", "PUT", "/config", self.configuration(True))
        self.lifecycle(worker, "handoff-complete")
        self.capture.execute(writer, "source-file-update", f"UPDATE {self.library}.S SET SRCDTA='buffered' WHERE SRCSEQ=1")
        for label, sql in [
                ("lob-update", f"UPDATE {self.library}.X SET BODY=REPEAT(CAST('y' AS CLOB(1)),60000) WHERE ID=1"),
                ("lob-delete", f"DELETE FROM {self.library}.X WHERE ID=1"),
                ("lob-null", f"INSERT INTO {self.library}.X VALUES (2,NULL)"),
                ("lob-empty", f"UPDATE {self.library}.X SET BODY=CAST('' AS CLOB(1M)) WHERE ID=2")]:
            self.capture.execute(writer, label, sql)
        tx = self.capture.connection(transaction=True)
        self.capture.execute(tx, "mixed-lob", f"INSERT INTO {self.library}.X VALUES (3,REPEAT(CAST('c' AS CLOB(1)),60000))")
        self.capture.execute(tx, "mixed-row", f"INSERT INTO {self.library}.A VALUES (701)")
        self.capture.execute(writer, "unrelated", f"INSERT INTO {self.library}.B VALUES (702)")
        saved = self.row_progress(self.inserted("B", 702, "unrelated"), "open-mixed")
        self.check("Excluded LOB work retains recovery history while unrelated captured rows progress",
                   int(saved["replay_from.sequence"]) < int(saved["completed_through.sequence"])
                   and all(row["key"]["ID"] != 701 for row in self.captured("A", "before-commit")), "control/")
        history = self.records(self.observer_container, "bindings-automatic", "history-before-restart")
        self.command("docker", "kill", worker)
        worker = self.start_worker("automatic", "w2")
        self.lifecycle(worker, "recovered")
        self.capture.execute(writer, "source-file-delete", f"DELETE FROM {self.library}.S WHERE SRCSEQ=1")
        self.check("Replacement recovers immutable bindings without resnapshotting excluded or captured tables",
                   self.records(self.observer_container, "bindings-automatic", "history-after-restart") == history
                   and all(sum(row["value"] and row["value"]["op"] == "r" for row in self.table_records(t, "recovered-" + t))
                           == sum(row["value"] and row["value"]["op"] == "r" for row in before) for t, before in baseline.items()), "control/")
        self.capture.execute(tx, "commit")
        self.row_progress(self.inserted("A", 701, "committed"), "committed")
        self.capture.execute(tx, "rollback-lob", f"UPDATE {self.library}.X SET BODY=REPEAT(CAST('r' AS CLOB(1)),60000) WHERE ID=3")
        self.capture.execute(tx, "rollback-row", f"INSERT INTO {self.library}.A VALUES (703)")
        self.capture.execute(tx, "rollback")
        self.capture.execute(tx, "excluded-clear", f"TRUNCATE TABLE {self.library}.X REUSE STORAGE IGNORE DELETE TRIGGERS")
        self.capture.execute(tx, "excluded-reload", f"INSERT INTO {self.library}.X VALUES (4,CAST('reload' AS CLOB(1M)))")
        self.capture.execute(tx, "commit")
        self.capture.execute(tx, "captured-clear", f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS")
        self.capture.execute(tx, "captured-reload", f"INSERT INTO {self.library}.B VALUES (704)")
        self.capture.execute(writer, "lob-during-clear", f"INSERT INTO {self.library}.X VALUES (5,REPEAT(CAST('z' AS CLOB(1)),60000))")
        self.capture.execute(writer, "row-during-clear", f"INSERT INTO {self.library}.A VALUES (705)")
        self.row_progress(self.inserted("A", 705, "row-during-clear"), "row-during-clear")
        self.check("Open captured clear and unrelated LOB writes do not stop other captured CDC",
                   all(row["value"]["op"] != "t" for row in self.captured("B", "open-clear")), "control/")
        self.capture.execute(tx, "commit")
        self.row_progress(self.inserted("B", 704, "captured-commit"), "captured-commit")
        self.check("Only committed selected rows and the captured table clear are published",
                   [row["key"]["ID"] for row in self.captured("A", "final-A")] == [10, 700, 701, 705]
                   and [row["value"]["op"] for row in self.captured("B", "final-B")] == ["c", "t", "c"], "control/")
        topics = self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list")
        self.check("Excluded CLOB and source physical files have no data topics",
                   all(f"test.{self.fixture.server_name}.{self.library}.{table}" not in topics.splitlines() for table in ["X", "S"]), "commands.log")
        self.request(worker, "automatic", "PUT", "/stop")
        public.base.wait_for("stopped before observed ALTER", lambda: self.rest(worker, "/connectors/automatic/status")["connector"]["state"] == "STOPPED", 60)
        self.lifecycle(worker, "drained")
        # Freeze the reader, not application writers: both ALTER and the unrelated
        # pointer row must be in its next complete metadata-observation interval.
        start = self.point()
        self.alter_fixture()
        self.capture.execute(writer, "lob-during-schema-observation", f"INSERT INTO {self.library}.X VALUES (6,REPEAT(CAST('s' AS CLOB(1)),6000))")
        self.capture.execute(writer, "new-schema-row", f"INSERT INTO {self.library}.A VALUES (706)")
        raw = self.raw_interval(start, "schema-and-excluded-lob")
        ddl = [row for row in raw if row["JOURNAL_CODE"] == "D" and row["JOURNAL_ENTRY_TYPE"] == "CG"]
        lob = [row for row in raw if row["JOURNAL_CODE"] == "R" and row["JOURNAL_ENTRY_TYPE"] in ["PT", "PX"]
               and row["OBJECT"][:10].strip() == "X"]
        self.request(worker, "automatic", "PUT", "/resume")
        row = self.inserted("A", 706, "new-schema-row")
        self.row_progress(row, "schema-and-lob-progress")
        self.check("A committed ALTER qualifies across verified excluded LOB rows in its retained interval",
                   len(ddl) == len(lob) == 1 and int(ddl[0]["SEQUENCE_NUMBER"]) < int(lob[0]["SEQUENCE_NUMBER"])
                   < int(row["value"]["source"]["sequence"]), "schema-and-excluded-lob-raw.json")
        history = self.records(self.observer_container, "bindings-automatic", "schema-history-before-replacement")
        self.command("docker", "kill", worker)
        worker = self.start_worker("automatic", "w3")
        self.lifecycle(worker, "recovered")
        self.capture.execute(writer, "replacement-schema-row", f"INSERT INTO {self.library}.A VALUES (707)")
        self.row_progress(self.inserted("A", 707, "replacement-schema-row"), "replacement-schema-row")
        self.check("Replacement reuses durable schema history and emits only new rows",
                   self.records(self.observer_container, "bindings-automatic", "schema-history-after-replacement") == history
                   and [row["key"]["ID"] for row in self.captured("A", "after-schema-replacement")] == [10, 700, 701, 705, 706, 707], "control/")
        self.close_session(tx)
        self.close_session(writer)
        self.request(worker, "automatic", "PUT", "/stop")
        self.lifecycle(worker, "drained")
        self.fixture.verify_rows({"A": [1, 2, 3, 10, 700, 701, 705, 706, 707], "B": [704], "X": [4, 5, 6]})
        self.report.update(status="passed", scope="Excluded native pointer rows, mixed commit/rollback, restart and captured clear progress",
                           buffer_storage="disk" if self.source_offsets else "memory",
                           lob_value_capture=False, external_lob_values_read=False)
        self.save()


if __name__ == "__main__":
    public.main(Experiment, Fixture)
