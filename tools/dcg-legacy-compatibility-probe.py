#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Legacy/native output parity using an owned IBM i fixture and local Kafka.

No resnapshot, heartbeat requirement, invented cross-partition order, or test
provider on the worker. Uses the public runner's ownership and cleanup checks.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import traceback

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("public_compatibility", Path(__file__).with_name("dcg-public-online-probe.py"))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)
LONG_TABLE = "MIXED_COMPATIBILITY_TABLE"


class Fixture(public.packaged.continuous.native.fixture_module.NativeFixture):
    ccsid_override = False

    def sql(self, query, label, ddl=False, timeout=35):
        if getattr(self, "long_table", False):
            query = query.replace(self.library + ".M ", self.library + "." + LONG_TABLE + " ")
        return super().sql(query, label, ddl, timeout)

    def setup(self):
        super().setup()
        self.sql(f"DROP TABLE {self.library}.M RESTRICT", "drop-original-M.log", True)
        self.owned_objects = self.inventory("objects-before-long-table.json")
        self.sql(f'CREATE TABLE {self.library}.{LONG_TABLE} FOR SYSTEM NAME M '
                 '(ID INTEGER NOT NULL PRIMARY KEY, D DECIMAL(9,3), C CHAR(5) CCSID 37, '
                 '"odd-name" VARCHAR(8) CCSID 37, GC GRAPHIC(4) CCSID 1200, VG VARGRAPHIC(4) CCSID 1200, '
                 'UC VARCHAR(12) CCSID 1208, FB CHAR(4) FOR BIT DATA, FV VARCHAR(8) FOR BIT DATA'
                 + (', TEXT_OVERRIDE VARCHAR(5) CCSID 37' if self.ccsid_override else '') + ')', "create-long-table.log", True)
        self.long_table = True
        self.owned_objects = self.inventory("objects-long-table-created.json")
        self.sql(f"SELECT * FROM QSYS2.SYSCOLUMNS2 WHERE TABLE_SCHEMA='{self.library}' AND TABLE_NAME='{LONG_TABLE}' ORDER BY ORDINAL_POSITION",
                 "compatibility-columns.json")
        self.setup_dds_key()

    def setup_dds_key(self):
        # All source preparation is in this one session's QTEMP. The only
        # persistent object is K in the newly owned fixture library.
        lines = ["     A                                      UNIQUE", "     A          R KREC",
                 "     A            ID             9B 0       ALWNULL", "     A          K ID"]
        self.save("dds-source.json", lines)
        insert = "INSERT INTO QTEMP.QDDSSRC (SRCSEQ,SRCDAT,SRCDTA) VALUES " + ",".join(
            f"({i},0,'{line}')" for i, line in enumerate(lines, 1))
        self.TABLES = [*self.TABLES, "K"]
        self.sql("BEGIN NOT ATOMIC\n"
                 "CALL QSYS2.QCMDEXC('CRTSRCPF FILE(QTEMP/QDDSSRC) RCDLEN(112) MBR(*FILE) CCSID(37)');\n"
                 "EXECUTE IMMEDIATE '" + insert.replace("'", "''") + "';\n"
                 f"CALL QSYS2.QCMDEXC('CRTPF FILE({self.library}/K) SRCFILE(QTEMP/QDDSSRC) SRCMBR(QDDSSRC) MAXMBRS(1) OPTION(*NOSRC *NOLIST)');\n"
                 "CALL QSYS2.QCMDEXC('DLTF FILE(QTEMP/QDDSSRC)');\nEND", "create-dds.log", True)
        self.owned_objects = self.inventory("objects-dds-created.json")
        journal = self.sql(f"SELECT JOURNAL_LIBRARY,JOURNAL_NAME,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{self.library}','*FILE','K')) X",
                           "dds-journaling-before.json")
        if journal == [{"JOURNAL_LIBRARY": None, "JOURNAL_NAME": None, "JOURNAL_IMAGES": None}]:
            self.sql(f"CALL QSYS2.QCMDEXC('STRJRNPF FILE({self.library}/K) JRN({self.library}/QSQJRN) IMAGES(*BOTH) OMTJRNE(*NONE)')",
                     "journal-dds.log", True)
        assert self.sql(f"SELECT JOURNAL_LIBRARY,JOURNAL_NAME,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{self.library}','*FILE','K')) X",
                        "dds-journaling.json") == [{"JOURNAL_LIBRARY": self.library, "JOURNAL_NAME": "QSQJRN", "JOURNAL_IMAGES": "*BOTH"}]
        assert self.sql(f"SELECT CONSTRAINT_NAME FROM QSYS2.SYSCST WHERE TABLE_SCHEMA='{self.library}' AND TABLE_NAME='K' "
                        "AND CONSTRAINT_TYPE='PRIMARY KEY'", "dds-no-sql-primary-key.json") == []
        assert self.sql(f"SELECT TRIM(DBKFLD) AS KEY_FIELD,DBKPOS AS ORDINAL_POSITION FROM QSYS.QADBKATR "
                        f"WHERE DBKLIB='{self.library}' AND DBKFIL='K' ORDER BY DBKPOS", "dds-physical-key.json") == [
                            {"KEY_FIELD": "ID", "ORDINAL_POSITION": 1}]


class Experiment(public.Experiment):
    output_options = False

    def configuration(self, enabled):
        config = super().configuration(enabled)
        config.update({"snapshot.mode": "no_data", "heartbeat.interval.ms": "0", "tombstones.on.delete": "false",
                       "decimal.handling.mode": "double", "binary.handling.mode": "hex", "field.name.adjustment.mode": "avro",
                       "column.exclude.list": self.library + "." + LONG_TABLE + ".odd-name",
                       "column.mask.with.4.chars": self.library + "." + LONG_TABLE + ".C",
                       "message.key.columns": self.library + "." + LONG_TABLE + ":ID,C",
                       "table.include.list": ",".join(self.library + "." + t for t in ["A", "B", LONG_TABLE, "K"])})
        if self.output_options:
            config.update({"topic.delimiter": "-", "topic.heartbeat.prefix": "beats", "topic.transaction": "transactions",
                           "datatype.propagate.source.type": ".*", "key.converter.schemas.enable": "true", "value.converter.schemas.enable": "true",
                           "snapshot.select.statement.overrides": self.library + ".A",
                           "snapshot.select.statement.overrides." + self.library + ".A": "SELECT * FROM " + self.library + ".A WHERE ID < 0"})
        if self.fixture.ccsid_override:
            config.update({"from.ccsid": "37", "to.ccsid": "500", "header.converter": "org.apache.kafka.connect.json.JsonConverter",
                           "header.converter.schemas.enable": "true"})
        return config

    def data_topic(self, table):
        table = LONG_TABLE if table == "M" else table
        return ("-" if self.output_options else ".").join(["test", self.fixture.server_name, self.library, table])

    def table_records(self, table, label):
        return self.records(self.observer_container, self.data_topic(table), label)

    def records(self, worker, topic, label):
        # The independent audit reads every partition and records its partition
        # number. Sorting those records is not evidence of global Kafka ordering.
        self.command("docker", "exec", worker, "java", "-cp", "/audit/*:/opt/kafka/libs/*",
                     "io.debezium.connector.db2as400.As400PackagedProbeAudit", "broker:9092", topic,
                     "read_committed", "/control/" + label + ".json", timeout=35)
        rows = json.loads((self.control / (label + ".json")).read_text())
        for row in rows:
            row["topic"] = topic  # Exact topic consumed by the independent audit.
            for field in ["key", "value"]:
                row[field] = json.loads(row[field]) if row[field] is not None else None
                if self.output_options and row[field] is not None and topic.startswith("test-"):
                    row[field + "_schema"] = row[field]["schema"]
                    row[field] = row[field]["payload"]
        return rows

    def setup_connector(self, worker):
        for topic in [self.data_topic(t) for t in ["A", "B", "M", "K"]] + [public.CONTROL, "bindings-automatic"]:
            history = topic == "bindings-automatic"
            self.command("docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092",
                         "--create", "--topic", topic, "--partitions", "3" if topic.startswith(("test.", "test-")) else "1", "--replication-factor", "1",
                         *(["--config", "cleanup.policy=delete", "--config", "retention.ms=-1"] if history else []))
        config = self.configuration(False)
        (self.directory / "legacy-connector.json").write_text(json.dumps(config, indent=2))
        (self.control / "automatic.owned-fixture").write_text(self.library)
        self.rest(worker, "/connectors", {"name": "automatic", "config": config})
        def initialized():
            status = self.rest(worker, "/connectors/automatic/status")
            if any(t["state"] == "FAILED" for t in status["tasks"]):
                raise RuntimeError("Legacy no_data startup failed; inspect retained worker log")
            return "Snapshot - Final stage" in self.logs[worker].read_text() or "Snapshot ended" in self.logs[worker].read_text()
        public.base.wait_for("legacy no_data startup", initialized, 120)
        self.fixture.sql(f"INSERT INTO {self.library}.A VALUES (10)", "legacy-row.log", True)
        row = self.inserted("A", 10, "legacy-row")
        public.base.wait_for("durable legacy offset", lambda: int((self.checkpoint(self.observer_container, "automatic", "legacy") or {})
                            .get("offset.event_sequence", 0)) >= int(row["value"]["source"]["sequence"]), 90)

    def insert_mixed(self, session, key, label):
        # UTF-16 hex literals avoid lossy conversion of SQL statement text by
        # an EBCDIC ODBC job before the Unicode column ever sees the value.
        self.capture.execute(session, label, f"INSERT INTO {self.library}.{LONG_TABLE} VALUES "
                             f"({key},1234.567,'ABC','hidden',UX'00C4754C',UX'754C00C4',UX'00C4754C',X'000180FF',X'00FF'"
                             + (",CAST(X'BABBB05A40' AS VARCHAR(5) CCSID 37)" if self.fixture.ccsid_override else '') + ")")
        stored = self.fixture.sql(f"SELECT HEX(GC) AS GC_BYTES, HEX(VG) AS VG_BYTES, HEX(UC) AS UC_BYTES, "
                                  f"HEX(FB) AS FB_BYTES, HEX(FV) AS FV_BYTES FROM {self.library}.{LONG_TABLE} WHERE ID={key}",
                                  label + "-stored-bytes.json")
        self.check("Stored Unicode and binary bytes are exact for " + label,
                   stored == [{"GC_BYTES": "00C4754C00200020", "VG_BYTES": "754C00C4", "UC_BYTES": "C384E7958C",
                               "FB_BYTES": "000180FF", "FV_BYTES": "00FF"}], "../native/native/" + label + "-stored-bytes.json")
        return self.inserted("M", key, label)

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker("automatic", "w1")
        self.setup_connector(worker)
        writer = self.capture.connection()
        self.capture.execute(writer, "legacy-dds", f"INSERT INTO {self.library}.K VALUES (20)")
        legacy_dds = self.inserted("K", 20, "legacy-dds")
        legacy = self.insert_mixed(writer, 20, "legacy-mixed")
        self.audit_start = self.point()
        public.base.wait_for("legacy mixed offset", lambda: int((self.checkpoint(self.observer_container, "automatic", "legacy-mixed") or {})
                            .get("offset.event_sequence", 0)) >= int(legacy["value"]["source"]["sequence"]), 90)
        config = self.configuration(True)
        (self.directory / "online-connector.json").write_text(json.dumps(config, indent=2))
        self.request(worker, "automatic", "PUT", "/config", config)
        self.lifecycle(worker, "handoff-complete")
        self.capture.execute(writer, "native-dds", f"INSERT INTO {self.library}.K VALUES (30)")
        native_dds = self.inserted("K", 30, "native-dds")
        self.check("Physical DDS keys survive handoff without an SQL primary key",
                   legacy_dds["key"] == {"ID": 20} and native_dds["key"] == {"ID": 30}
                   and native_dds["value"]["after"] == {"ID": 30}, "control/")
        native = self.insert_mixed(writer, 30, "native-mixed")
        self.row_progress(native, "native-mixed")
        if self.output_options:
            fields = next(f for f in native["value_schema"]["fields"] if f["field"] == "after")["fields"]
            self.check("Native type propagation preserves the actual legacy schemas and renamed topics",
                       native["key_schema"] == legacy["key_schema"] and native["value_schema"] == legacy["value_schema"]
                       and native_dds["key_schema"] == legacy_dds["key_schema"] and native_dds["value_schema"] == legacy_dds["value_schema"]
                       and any("__debezium.source.column.type" in f.get("parameters", {}) for f in fields), "control/")
        before, after = (dict(r["value"]["after"]) for r in [legacy, native])
        before.pop("ID"); after.pop("ID")
        self.check("Existing column mapping, decimal, Unicode and binary values survive native handoff", before == after
                   and before["C"] == "****" and before["D"] == 1234.567 and "odd-name" not in before and "odd_name" not in before
                   and before["GC"] == "Ä界  " and before["VG"] == "界Ä" and before["UC"] == "Ä界"
                   and before["FB"] == "AAGA/w==" and before["FV"] == "AP8="
                   and {k: v for k, v in legacy["key"].items() if k != "ID"} == {k: v for k, v in native["key"].items() if k != "ID"}, "control/")
        if self.fixture.ccsid_override:
            self.check("Explicit legacy CCSID substitution preserves text without changing binary or Unicode columns",
                       before["TEXT_OVERRIDE"] == after["TEXT_OVERRIDE"] == "¬|¢] ", "control/")
        self.check("Native output retains logical SQL table topic, physical RRN and existing no_data baseline",
                   native["topic"] == self.data_topic("M") and int(native["value"]["source"]["rrn"]) > 0
                   and all(r["value"]["op"] != "r" for r in self.table_records("M", "no-resnapshot")), "control/")
        self.capture.execute(writer, "native-delete", f"DELETE FROM {self.library}.{LONG_TABLE} WHERE ID=20")
        deleted = public.base.wait_for("native delete", lambda: next((r for r in self.table_records("M", "deleted")
                            if r["value"] and r["value"]["op"] == "d" and r["key"]["ID"] == 20), None), 90)
        self.row_progress(deleted, "native-delete")
        self.check("Disabled tombstones stay disabled after migration", all(r["value"] is not None for r in self.table_records("M", "no-tombstone")), "control/")
        baseline = self.table_records("B", "before-clear")
        transaction = self.capture.connection(transaction=True)
        self.capture.execute(transaction, "held-clear", f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS")
        self.capture.execute(transaction, "held-reload", f"INSERT INTO {self.library}.B VALUES (200)")
        self.capture.execute(writer, "unrelated-open", f"INSERT INTO {self.library}.A VALUES (200)")
        self.row_progress(self.inserted("A", 200, "unrelated-open"), "unrelated-open")
        self.check("Unrelated CDC advances with no heartbeat while clear remains invisible", self.table_records("B", "open-clear") == baseline, "control/")
        history = self.records(self.observer_container, "bindings-automatic", "history-before-restart")
        self.command("docker", "kill", worker)
        worker = self.start_worker("automatic", "w2")
        self.lifecycle(worker, "recovered")
        self.check("Restart uses immutable history without resnapshot", self.records(self.observer_container, "bindings-automatic", "history-after-restart") == history, "control/")
        if self.fixture.ccsid_override:
            recovered = self.insert_mixed(writer, 31, "recovered-ccsid")
            self.row_progress(recovered, "recovered-ccsid")
            self.check("Restart retains the saved CCSID interpretation and legacy schema",
                       recovered["value"]["after"]["TEXT_OVERRIDE"] == "¬|¢] "
                       and recovered["value_schema"] == legacy["value_schema"], "control/")
        start = self.point(); self.capture.execute(transaction, "rollback")
        self.progressed(self.terminal(start, transaction[1], "RB", "rollback"), "rollback")
        self.check("Rollback advances durable F/W without a public heartbeat or output", self.table_records("B", "rollback") == baseline, "control/")
        self.capture.execute(transaction, "committed-clear", f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS")
        self.capture.execute(transaction, "committed-reload", f"INSERT INTO {self.library}.B VALUES (201)")
        start = self.point(); self.capture.execute(transaction, "commit")
        self.progressed(self.terminal(start, transaction[1], "CM", "commit"), "commit")
        committed = self.table_records("B", "committed")
        self.check("Three-partition output contains each committed truncate and reload exactly once",
                   sorted(r["value"]["op"] for r in committed) == ["c", "t"]
                   and next(r for r in committed if r["value"]["op"] == "c")["value"]["after"]["ID"] == 201, "control/")
        self.check("No heartbeat topic is needed for native durable progress", ("beats-test" if self.output_options else "__debezium-heartbeat.test") not in self.command(
                   "docker", "exec", self.broker, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "broker:9092", "--list").splitlines(), "commands.log")
        self.raw_interval(self.audit_start, "compatibility-transactions")
        self.close_session(transaction); self.close_session(writer)
        self.request(worker, "automatic", "PUT", "/stop"); self.lifecycle(worker, "drained")
        self.fixture.verify_rows({"A": [1, 2, 3, 10, 200], "B": [201], "M": [30, 31] if self.fixture.ccsid_override else [30],
                                  "U": [1, 2, 3], "K": [20, 30]})
        self.report.update(status="passed", output_partitions=3, extended_output_options=self.output_options, cross_partition_order="not guaranteed by Kafka",
                           harness_sha256={**self.report["harness_sha256"], "tools/dcg-legacy-compatibility-probe.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        self.save()


def main():
    if len(sys.argv) not in (3, 4) or len(sys.argv) == 4 and sys.argv[3] not in ("output-options", "ccsid"):
        raise SystemExit("Usage: dcg-legacy-compatibility-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd [output-options|ccsid]")
    root = Path(sys.argv[1]).resolve(); root.mkdir(exist_ok=False)
    capture = public.live.prospective.census.Experiment(root / "native", sys.argv[2], 1)
    capture.fixture.__class__ = Fixture
    capture.fixture.ccsid_override = len(sys.argv) == 4 and sys.argv[3] == "ccsid"
    experiment = None
    try:
        capture.fixture.setup()
        experiment = Experiment(root / "connect", capture)
        experiment.output_options = len(sys.argv) == 4
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
        raise SystemExit("Native compatibility validation failed; see retained evidence and cleanup result")
    print(experiment.directory / "report.json", flush=True)


if __name__ == "__main__":
    main()
