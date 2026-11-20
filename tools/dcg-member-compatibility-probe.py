#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Opt-in multi-member row/recovery compatibility, with an isolated library and Kafka.

Uses the ordinary plugin and its public configuration. Member names remain an
internal recovery identity; this does not introduce new output keys or topics.
"""
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("member_compatibility", Path(__file__).with_name("dcg-legacy-compatibility-probe.py"))
compat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compat)
public = compat.public


class Fixture(compat.Fixture):
    def setup(self):
        super().setup()
        self.sql(f"CALL QSYS2.QCMDEXC('CHGPF FILE({self.library}/K) MAXMBRS(3)')", "allow-members.log", True)
        self.sql(f"CALL QSYS2.QCMDEXC('ADDPFM FILE({self.library}/K) MBR(SECOND)')", "add-member.log", True)
        self.sql(f"SELECT SYSTEM_TABLE_NAME,SYSTEM_TABLE_MEMBER FROM QSYS2.SYSPARTITIONSTAT "
                 f"WHERE SYSTEM_TABLE_SCHEMA='{self.library}' AND SYSTEM_TABLE_NAME='K'", "members.json")
        self.sql(f"SELECT JOURNALED_FILES,JOURNALED_MEMBERS FROM QSYS2.JOURNAL_INFO "
                 f"WHERE JOURNAL_LIBRARY='{self.library}' AND JOURNAL_NAME='QSQJRN'", "journal-counts.json")
        self.sql(f"CREATE INDEX {self.library}.IXA ON {self.library}.A(ID DESC)", "create-existing-index.log", True)
        self.owned_objects = self.inventory("objects-with-existing-index.json")

        for label, command in [
                ("area", f"CRTDTAARA DTAARA({self.library}/AREA) TYPE(*CHAR) LEN(16) VALUE(INITIAL)"),
                ("queue", f"CRTDTAQ DTAQ({self.library}/QUEUE) MAXLEN(16)"),
                ("journal-area", f"STRJRNOBJ OBJ({self.library}/AREA) OBJTYPE(*DTAARA) JRN({self.library}/QSQJRN)"),
                ("journal-queue", f"STRJRNOBJ OBJ({self.library}/QUEUE) OBJTYPE(*DTAQ) JRN({self.library}/QSQJRN)"),
                ("journal-access-path", f"STRJRNAP FILE({self.library}/A) JRN({self.library}/QSQJRN)")]:
            self.sql(f"CALL QSYS2.QCMDEXC('{command}')", "create-" + label + ".log", True)
            self.owned_objects = self.inventory("objects-with-" + label + ".json")

    def cleanup(self):
        for name, typ, command in [("AREA", "*DTAARA", "DLTDTAARA DTAARA"), ("QUEUE", "*DTAQ", "DLTDTAQ DTAQ")]:
            if any(o["OBJNAME"] == name and o["OBJTYPE"] == typ for o in (self.owned_objects or [])):
                assert self.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{self.library}','{name}','{typ}',0)) X",
                                "cleanup-locks-" + name + ".json") == []
                self.sql(f"CALL QSYS2.QCMDEXC('{command}({self.library}/{name})')", "drop-owned-" + name + ".log", True)
                self.owned_objects = [o for o in self.owned_objects if not (o["OBJNAME"] == name and o["OBJTYPE"] == typ)]
        for name in ["IXA", "IXB"]:
            if any(o["OBJNAME"] == name and o["OBJTYPE"] == "*FILE" for o in (self.owned_objects or [])):
                assert self.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{self.library}','{name}','*FILE',0)) X",
                                "cleanup-locks-" + name + ".json") == []
                self.sql(f"DROP INDEX {self.library}.{name}", "drop-owned-index-" + name + ".log", True)
                self.owned_objects = [o for o in self.owned_objects if not (o["OBJNAME"] == name and o["OBJTYPE"] == "*FILE")]
        return super().cleanup()


class Experiment(compat.Experiment):
    def configuration(self, enabled):
        config = super().configuration(enabled)
        if enabled:
            config["truncate.buffer.storage"] = "disk" if self.source_offsets else "memory"
        return config

    def member_write(self, writer, member, value, label):
        self.capture.execute(writer, label, f"INSERT INTO {self.library}.K VALUES ({value})" if member == "K"
                             else f"INSERT INTO QTEMP.{member} VALUES ({value})")
        return self.inserted("K", value, label)

    def exercise(self):
        self.capture.deadline = None
        worker = self.start_worker("automatic", "w1")
        self.setup_connector(worker)
        writer = self.capture.connection()
        self.capture.execute(writer, "member-alias", f"CREATE ALIAS QTEMP.SECOND FOR {self.library}.K(SECOND)")
        first = self.member_write(writer, "K", 700, "legacy-first")
        second = self.member_write(writer, "SECOND", 701, "legacy-second")
        self.check("Legacy records from both members use the same table topic and key shape",
                   first["topic"] == second["topic"] == self.data_topic("K") and first["key"] == {"ID": 700}
                   and second["key"] == {"ID": 701}, "control/")
        public.base.wait_for("legacy member offset", lambda: int((self.checkpoint(self.observer_container, "automatic", "legacy-member") or {})
                            .get("offset.event_sequence", 0)) >= int(second["value"]["source"]["sequence"]), 90)
        self.audit_start = self.point()
        self.request(worker, "automatic", "PUT", "/config", self.configuration(True))
        self.lifecycle(worker, "handoff-complete")
        self.capture.execute(writer, "create-index-during-streaming", f"CREATE INDEX {self.library}.IXB ON {self.library}.B(ID DESC)")
        self.fixture.owned_objects = self.fixture.inventory("objects-with-new-index.json")
        self.capture.execute(writer, "change-non-row-resource", f"CALL QSYS2.QCMDEXC('CHGDTAARA DTAARA({self.library}/AREA) VALUE(CHANGED)')")
        self.capture.execute(writer, "end-access-path", f"CALL QSYS2.QCMDEXC('ENDJRNAP FILE({self.library}/A) JRN({self.library}/QSQJRN)')")
        self.capture.execute(writer, "restart-access-path", f"CALL QSYS2.QCMDEXC('STRJRNAP FILE({self.library}/A) JRN({self.library}/QSQJRN)')")
        for member, value in [("K", 710), ("SECOND", 711)]:
            record = self.member_write(writer, member, value, "native-" + member)
            self.row_progress(record, "native-" + member)
        self.capture.execute(writer, "add-third-member", f"CALL QSYS2.QCMDEXC('ADDPFM FILE({self.library}/K) MBR(THIRD)')")
        self.capture.execute(writer, "third-member-alias", f"CREATE ALIAS QTEMP.THIRD FOR {self.library}.K(THIRD)")
        added = self.member_write(writer, "THIRD", 712, "new-member")
        self.row_progress(added, "new-member")
        self.check("A member added during streaming is discovered without resnapshotting or changing output keys",
                   added["topic"] == first["topic"] and added["key"] == {"ID": 712} and added["value"]["op"] == "c", "control/")
        history = self.records(self.observer_container, "bindings-automatic", "history-before-restart")
        self.command("docker", "kill", worker)
        worker = self.start_worker("automatic", "w2")
        self.lifecycle(worker, "recovered")
        self.check("Replacement recovers complete member bindings without appending history or resnapshotting",
                   self.records(self.observer_container, "bindings-automatic", "history-after-restart") == history,
                   "control/")
        self.capture.execute(writer, "drop-index-after-restart", f"DROP INDEX {self.library}.IXB")
        self.fixture.owned_objects = [o for o in self.fixture.owned_objects if not (o["OBJNAME"] == "IXB" and o["OBJTYPE"] == "*FILE")]
        for member, value in [("K", 720), ("SECOND", 721), ("THIRD", 722)]:
            record = self.member_write(writer, member, value, "recovered-" + member)
            self.row_progress(record, "recovered-" + member)
        records = self.table_records("K", "recovered-members")
        self.check("Each member row appears once with the existing key, topic and create operation",
                   len(records) == 8 and sorted(r["value"]["after"]["ID"] for r in records) == [700, 701, 710, 711, 712, 720, 721, 722]
                   and all(r["value"]["op"] == "c" and r["key"] == {"ID": r["value"]["after"]["ID"]} for r in records), "control/")
        self.check("Logical files, data areas, data queues and access paths coexist with physical row capture",
                   self.rest(worker, "/connectors/automatic/status")["tasks"][0]["state"] == "RUNNING", "control/")
        update = self.capture.connection(transaction=True)
        for member in ["SECOND", "THIRD"]:
            self.capture.execute(update, "update-alias-" + member, f"CREATE ALIAS QTEMP.{member} FOR {self.library}.K({member})")
        for target, old in [(self.library + ".K", 700), ("QTEMP.SECOND", 701), ("QTEMP.THIRD", 712)]:
            self.capture.execute(update, "member-key-update-" + str(old), f"UPDATE {target} SET ID={old + 100} WHERE ID={old}")
        start = self.point()
        self.capture.execute(update, "commit")
        self.progressed(self.terminal(start, update[1], "CM", "member-updates"), "member-updates")
        changed = self.table_records("K", "member-key-updates")
        for old in [700, 701, 712]:
            deleted = [r for r in changed if r["key"] == {"ID": old} and r["value"]["op"] == "d"]
            inserted = [r for r in changed if r["key"] == {"ID": old + 100} and r["value"]["op"] == "c"]
            assert len(deleted) == len(inserted) == 1
            assert deleted[0]["value"]["before"] == {"ID": old} and inserted[0]["value"]["after"] == {"ID": old + 100}
            assert int(deleted[0]["value"]["source"]["rrn"]) == int(inserted[0]["value"]["source"]["rrn"]) == 1
        self.check("One transaction preserves each member's before-image and primary-key change",
                   len(changed) == 14, "control/member-key-updates.json")
        self.close_session(update)
        transaction = self.capture.connection(transaction=True)
        self.capture.execute(transaction, "held-clear", f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS")
        self.capture.execute(transaction, "held-reload", f"INSERT INTO {self.library}.B VALUES (730)")
        unrelated = self.member_write(writer, "SECOND", 731, "unrelated-member")
        self.row_progress(unrelated, "unrelated-member")
        self.check("Member row capture progresses while a different table has an open clear", self.table_records("B", "open-clear") == [], "control/")
        start = self.point()
        self.capture.execute(transaction, "commit")
        self.progressed(self.terminal(start, transaction[1], "CM", "commit"), "commit")
        self.check("Transactional truncate still commits for the single-member table",
                   sorted(r["value"]["op"] for r in self.table_records("B", "committed")) == ["c", "t"], "control/")
        self.close_session(transaction)
        self.raw_interval(self.audit_start, "member-transactions")
        start = self.point()
        self.capture.execute(writer, "member-clear", f"CALL QSYS2.QCMDEXC('CLRPFM FILE({self.library}/K) MBR(SECOND)')")
        public.base.wait_for("safe member-clear rejection", lambda: self.failed(worker, "automatic", "member clear cannot be emitted"), 120)
        clear = [row for row in self.raw_interval(start, "member-clear")
                 if row["JOURNAL_CODE"] == "F" and row["JOURNAL_ENTRY_TYPE"] == "CR"]
        self.check("The rejected member clear cannot advance completed output or erase other members downstream",
                   len(clear) == 1 and int(self.checkpoint(self.observer_container, "automatic", "rejected-clear")["completed_through.sequence"])
                   < int(clear[0]["SEQUENCE_NUMBER"]) and all(r["value"]["op"] != "t" for r in self.table_records("K", "no-table-truncate")), "control/")
        self.close_session(writer)
        self.request(worker, "automatic", "PUT", "/stop")
        self.lifecycle(worker, "drained")
        self.fixture.verify_rows({"K": [710, 720, 800], "B": [730]})
        self.report.update(status="passed", scope="Native multi-member rows, dynamic member discovery, immutable restart history and unsafe member-clear rejection",
                           buffer_storage="disk" if self.source_offsets else "memory",
                           output_contract="Existing table topics and keys; no new member discriminator; snapshot still reads the original first member")
        self.save()


if __name__ == "__main__":
    public.main(Experiment, Fixture)
