#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit, isolated IBM i fixture management. Never selects a server implicitly."""
import datetime
import json
import os
from pathlib import Path
import re
import time
import stat
from types import SimpleNamespace

def load_config():
    path = os.environ.get("IBMI_TEST_CONFIG")
    if not path:
        raise ValueError("Set IBMI_TEST_CONFIG to an owner-only JSON file; native tests never select a default host")
    path = Path(path)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("IBMI_TEST_CONFIG must be an owner-only regular file (0600)")
    config = json.loads(path.read_text())
    required = {"host", "user", "password", "server", "allow_isolated_fixture_writes"}
    allowed = required | {"odbc_driver"}
    if not required <= config.keys() or config.keys() - allowed or config["allow_isolated_fixture_writes"] is not True:
        raise ValueError("Explicit host/user/password/server and allow_isolated_fixture_writes=true required")
    if any(not isinstance(config[k], str) or not config[k] for k in ["host", "user", "password", "server"]):
        raise ValueError("Nonempty native test credentials and expected server required")
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,9}", config["server"]):
        raise ValueError("Exact expected IBM i server name required")
    return config


def odbc_value(value):
    return "{" + str(value).replace("}", "}}") + "}"


class NativeFixture:
    TABLES = ["A", "B", "M", "U"]
    FLAGS = {"JOB_NAME": "*JOB", "USER_NAME": "*USR", "PROGRAM_NAME": "*PGM", "PROGRAM_LIBRARY": "*PGMLIB",
             "SYSTEM_SEQUENCE_NUMBER": "*SYSSEQ", "REMOTE_ADDRESS": "*RMTADR", "THREAD_ID": "*THD",
             "LOGICAL_UNIT_OF_WORK_ID": "*LUW", "TRANSACTION_ID": "*XID"}

    def __init__(self, directory, library):
        if not re.fullmatch(r"DCNC[0-9]{4}", library):
            raise ValueError("Explicit native Connect fixture name required")
        self.directory = Path(directory)
        self.directory.mkdir()
        self.library = library
        self.created = False
        self.owned_objects = None
        config = load_config()
        self.target = SimpleNamespace(host=config["host"])
        self.credentials = SimpleNamespace(username=config["user"], password=config["password"])
        self.server_name = config["server"]
        self.driver = config.get("odbc_driver", "IBM i Access ODBC Driver")
        self.counter = 0

    def redact(self, text):
        for secret in sorted({self.target.host, self.credentials.username, self.credentials.password}, key=len, reverse=True):
            text = text.replace(secret, "<redacted>")
        return text

    def properties(self):
        values = {"database.hostname": self.target.host, "database.user": self.credentials.username, "database.password": self.credentials.password, "database.dbname": self.server_name}
        return "".join(k + "=" + "".join("\\u%04x" % ord(c) for c in v) + "\n" for k, v in values.items())

    def save(self, name, value):
        (self.directory / name).write_text(json.dumps(value, indent=2) + "\n")

    def sql(self, query, label, ddl=False, timeout=35):
        # Optional public dependency; importing the runner never opens a source.
        # A dedicated connection per call matches the original helper's lifecycle.
        import pyodbc
        connection_string = ";".join(k + "=" + odbc_value(v) for k, v in {
            "DRIVER": self.driver, "SYSTEM": self.target.host, "UID": self.credentials.username,
            "PWD": self.credentials.password, "NAM": "0", "CMT": "0"}.items())
        try:
            connection = pyodbc.connect(connection_string, timeout=timeout, autocommit=True)
            try:
                connection.timeout = timeout
                cursor = connection.cursor()
                try:
                    cursor.execute(query)
                    result = [] if cursor.description is None else [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]
                finally:
                    cursor.close()
            finally:
                connection.close()
        except Exception as error:
            output = self.redact(str(error))
            (self.directory / label).write_text(output)
            raise RuntimeError("Native SQL failed; see " + label + ": " + output[:500]) from None
        (self.directory / label).write_text(self.redact(json.dumps(result, default=str, indent=2)) + "\n")
        return None if ddl else result

    def server(self):
        assert self.sql("SELECT CURRENT_SERVER AS SERVER FROM SYSIBM.SYSDUMMY1", "server.json") == [{"SERVER": self.server_name}]

    def inventory(self, label):
        return self.sql(f"SELECT OBJNAME,OBJTYPE,OBJATTRIBUTE,OBJCREATED FROM TABLE(QSYS2.OBJECT_STATISTICS('{self.library}','*ALL')) X ORDER BY OBJNAME,OBJTYPE", label)

    def receivers(self, label):
        fields = ",".join("FIXED_LENGTH_DATA_INCLUDES_" + x for x in self.FLAGS)
        return self.sql(f"SELECT JOURNAL_RECEIVER_NAME,STATUS,FIRST_SEQUENCE_NUMBER,LAST_SEQUENCE_NUMBER,PENDING_TRANSACTIONS,{fields} FROM QSYS2.JOURNAL_RECEIVER_INFO WHERE JOURNAL_LIBRARY='{self.library}' AND JOURNAL_NAME='QSQJRN' ORDER BY FIRST_SEQUENCE_NUMBER", label)

    def setup(self):
        self.server()
        lib = self.library
        assert self.sql(f"SELECT SCHEMA_NAME FROM QSYS2.SYSSCHEMAS WHERE SCHEMA_NAME='{lib}'", "schema-before.json") == []
        assert self.sql(f"SELECT OBJNAME FROM TABLE(QSYS2.OBJECT_STATISTICS('QSYS','*LIB','{lib}')) X", "library-before.json") == []
        self.sql(f"CREATE SCHEMA {lib}", "create-schema.log", True)
        self.created = True
        self.save("owner.json", {"library": lib, "server": self.server_name, "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                 "scope": "every writer, DDL, maintenance and receiver retention controlled by this isolated experiment"})
        self.owned_objects = self.inventory("objects-schema-created.json")
        before = self.receivers("receivers-before.json")
        assert len(before) == 1 and before[0]["STATUS"] == "ATTACHED"
        journal = self.sql(f"SELECT RECEIVER_MAXIMUM_SIZE,MANAGE_RECEIVER_OPTION,DELETE_RECEIVER_OPTION,REMOVE_FIXED_LENGTH_DETAIL FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY='{lib}' AND JOURNAL_NAME='QSQJRN'", "journal-before.json")[0]
        assert journal["REMOVE_FIXED_LENGTH_DETAIL"] == "NO"
        maxopt = journal["RECEIVER_MAXIMUM_SIZE"].lstrip("*")
        assert maxopt in ["MAXOPT2", "MAXOPT3"]
        assert before[0]["FIXED_LENGTH_DATA_INCLUDES_JOB_NAME"] == "YES"
        assert before[0]["FIXED_LENGTH_DATA_INCLUDES_LOGICAL_UNIT_OF_WORK_ID"] == "NO"
        assert before[0]["FIXED_LENGTH_DATA_INCLUDES_TRANSACTION_ID"] == "NO"
        options = [v for k, v in self.FLAGS.items() if before[0]["FIXED_LENGTH_DATA_INCLUDES_" + k] == "YES" or k == "THREAD_ID"]
        retention = " DLTRCV(*NO)" if journal["MANAGE_RECEIVER_OPTION"].lstrip("*") == "SYSTEM" else ""
        command = f"CHGJRN JRN({lib}/QSQJRN) JRNRCV(*GEN) SEQOPT(*CONT) FIXLENDTA({' '.join(options)}) RCVSIZOPT(*{maxopt})" + retention
        self.save("recording-plan.json", {"command": command, "existing_fixed_fields_preserved": True, "operator_retention": "all owned receivers retained through final verification"})
        self.sql("CALL QSYS2.QCMDEXC('" + command + "')", "recording-change.log", True)
        self.owned_objects = self.inventory("objects-receivers-created.json")
        after = self.receivers("receivers-configured.json")
        assert len(after) == 2 and all(x["STATUS"] != "DELETED" for x in after)
        current = [x for x in after if x["STATUS"] == "ATTACHED"]
        assert len(current) == 1
        for key in self.FLAGS:
            assert current[0]["FIXED_LENGTH_DATA_INCLUDES_" + key] == ("YES" if key == "THREAD_ID" else before[0]["FIXED_LENGTH_DATA_INCLUDES_" + key])
        profile = self.sql(f"SELECT DELETE_RECEIVER_OPTION,REMOVE_INTERNAL_ENTRIES,REMOVE_FIXED_LENGTH_DETAIL FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY='{lib}' AND JOURNAL_NAME='QSQJRN'", "journal-configured.json")[0]
        assert all(str(x).lstrip("*") == "NO" for x in profile.values()), profile
        for table in self.TABLES:
            self.sql(f"CREATE TABLE {lib}.{table} (ID INTEGER NOT NULL PRIMARY KEY)", "create-" + table + ".log", True)
            self.owned_objects = self.inventory("objects-created-" + table + ".json")
            self.sql(f"INSERT INTO {lib}.{table} VALUES (1),(2),(3)", "seed-" + table + ".log", True)
        self.owned_objects = self.inventory("objects-created.json")
        journaling = self.sql(f"SELECT OBJNAME,JOURNAL_NAME,JOURNAL_LIBRARY,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{lib}','*FILE')) X WHERE OBJNAME IN ('A','B','M','U') ORDER BY OBJNAME", "journaling.json")
        assert len(journaling) == 4
        assert all(x["JOURNAL_NAME"] == "QSQJRN" and x["JOURNAL_LIBRARY"] == lib and x["JOURNAL_IMAGES"] == "*BOTH" for x in journaling)
        self.save("setup.json", {"status": "passed", "library": lib, "tables": self.TABLES, "receiver": current[0]["JOURNAL_RECEIVER_NAME"]})

    def rotate(self):
        before = self.receivers("rotation-before.json")
        command = json.loads((self.directory / "recording-plan.json").read_text())["command"]
        self.sql("CALL QSYS2.QCMDEXC('" + command + "')", "rotation.log", True)
        rotated_objects = self.inventory("objects-rotated.json")
        after = self.receivers("rotation-after.json")
        assert len(after) == len(before) + 1 and all(r["STATUS"] != "DELETED" for r in after)
        old = next(r for r in before if r["STATUS"] == "ATTACHED")
        current = next(r for r in after if r["STATUS"] == "ATTACHED")
        assert old["JOURNAL_RECEIVER_NAME"] != current["JOURNAL_RECEIVER_NAME"]
        assert all(current["FIXED_LENGTH_DATA_INCLUDES_" + key] == old["FIXED_LENGTH_DATA_INCLUDES_" + key] for key in self.FLAGS)
        profile = self.sql(f"SELECT DELETE_RECEIVER_OPTION,REMOVE_INTERNAL_ENTRIES,REMOVE_FIXED_LENGTH_DETAIL FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY='{self.library}' AND JOURNAL_NAME='QSQJRN'", "rotation-profile.json")[0]
        assert all(str(v).lstrip('*') == 'NO' for v in profile.values())
        self.owned_objects = self.rotation_inventory(self.owned_objects, rotated_objects, current["JOURNAL_RECEIVER_NAME"])
        self.save("objects-owned-after-rotation.json", json.loads(json.dumps(self.owned_objects, default=str)))
        return old["JOURNAL_RECEIVER_NAME"], current["JOURNAL_RECEIVER_NAME"]

    @staticmethod
    def rotation_inventory(owned, observed, receiver):
        """Extend ownership only for the exact new receiver; never relearn files.

        OBJECT_STATISTICS can omit a locked file's detail during an open clear.
        Retain its original identity. Cleanup still requires every full identity
        to match after transactions and native readers drain.
        """
        key = lambda row: (row['OBJNAME'], row['OBJTYPE'])
        previous, current = ({key(row): row for row in rows} for rows in [owned, observed])
        added = (receiver, '*JRNRCV')
        if (len(previous) != len(owned) or len(current) != len(observed)
                or added in previous or set(current) != set(previous) | {added}):
            raise ValueError('Unexpected object change during owned receiver rotation')
        for identity, row in previous.items():
            seen = current[identity]
            for field in ['OBJATTRIBUTE', 'OBJCREATED']:
                if row[field] is None or seen[field] is not None and seen[field] != row[field]:
                    raise ValueError('Owned object identity changed during receiver rotation')
        created = current[added]
        if created['OBJATTRIBUTE'] is None or created['OBJCREATED'] is None:
            raise ValueError('Exact new receiver identity required')
        return [*owned, created]

    def verify_rows(self, expected):
        for table, values in expected.items():
            actual = self.sql(f"SELECT ID FROM {self.library}.{table} ORDER BY ID", "final-" + table + ".json")
            assert actual == [{"ID": x} for x in values], (table, actual)

    def cleanup(self):
        if not self.created:
            return {"status": "not-created"}
        self.server()
        lib = self.library
        owner = json.loads((self.directory / "owner.json").read_text())
        assert owner["library"] == lib and owner["server"] == self.server_name
        objects = self.inventory("objects-before-cleanup.json")
        identity = lambda x: (x["OBJNAME"], x["OBJTYPE"], x["OBJATTRIBUTE"], x["OBJCREATED"])
        assert {identity(x) for x in objects} == {identity(x) for x in self.owned_objects}, "Unexpected objects: retain fixture for inspection"
        assert all(r["PENDING_TRANSACTIONS"] == "NO" for r in self.receivers("receivers-before-cleanup.json"))
        assert self.sql(f"SELECT MESSAGE_ID FROM TABLE(QSYS2.MESSAGE_QUEUE_INFO(MESSAGE_FILTER => 'INQUIRY')) X WHERE MESSAGE_TEXT LIKE '%{lib}%'", "inquiries-before-cleanup.json") == []
        for table in self.TABLES:
            if not any(x["OBJNAME"] == table and x["OBJTYPE"] == "*FILE" for x in objects):
                continue
            assert self.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{lib}','{table}','*FILE',0)) X", "cleanup-locks-" + table + ".json") == []
            self.sql(f"DROP TABLE {lib}.{table} RESTRICT", "drop-" + table + ".log", True)
        assert self.sql(f"SELECT TABLE_NAME FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA='{lib}' AND TABLE_TYPE='T'", "tables-absent.json") == []
        assert self.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('{lib}','QSQJRN','*JRN',0)) X", "journal-locks-before-drop.json") == []
        resources = self.sql(f"SELECT JOURNALED_FILES,JOURNALED_MEMBERS,JOURNALED_DATA_AREAS,JOURNALED_DATA_QUEUES,JOURNALED_IFS_OBJECTS FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY='{lib}' AND JOURNAL_NAME='QSQJRN'", "journal-before-drop.json")
        assert len(resources) == 1 and all(v == 0 for v in resources[0].values())
        receivers = [x["OBJNAME"] for x in objects if x["OBJTYPE"] == "*JRNRCV"]
        # Execute the one owned RESTRICT drop on a background SQL connection so
        # exact CPA7025 inquiries can be tied to its library lock/job before reply.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.sql, f"DROP SCHEMA {lib} RESTRICT", "drop-schema.log", True, 90)
            replies = []
            deadline = time.monotonic() + 75
            while not future.done() and time.monotonic() < deadline:
                messages = self.sql(f"SELECT MESSAGE_ID,HEX(MESSAGE_KEY) AS MESSAGE_KEY_HEX,MESSAGE_TEXT,FROM_JOB FROM TABLE(QSYS2.MESSAGE_QUEUE_INFO(MESSAGE_FILTER => 'INQUIRY')) X WHERE MESSAGE_ID='CPA7025' AND MESSAGE_TEXT LIKE '%{lib}%'", "cleanup-inquiry.json")
                if messages:
                    assert len(messages) == 1
                    message = messages[0]
                    assert message["MESSAGE_TEXT"] in [f"Receiver {r} in {lib} never fully saved. (I C)" for r in receivers]
                    locks = self.sql(f"SELECT * FROM TABLE(QSYS2.OBJECT_LOCK_INFO('QSYS','{lib}','*LIB',0)) X", "cleanup-library-locks.json")
                    assert message["FROM_JOB"] in {x["JOB_NAME"] for x in locks if x["PROGRAM_NAME"] in ["QJODLTRC", "QSQDROP"]}
                    key = message["MESSAGE_KEY_HEX"]
                    assert re.fullmatch("[A-F0-9]{8}", key) and message["MESSAGE_TEXT"] not in [x["MESSAGE_TEXT"] for x in replies]
                    self.save("verified-inquiry-" + str(len(replies)) + ".json", {"message": message, "locks": locks})
                    self.sql("CALL QSYS2.QCMDEXC('SNDRPY MSGKEY(X''" + key + "'') MSGQ(QSYS/QSYSOPR) RPY(''I'')')", "reply-" + str(len(replies)) + ".log", True)
                    replies.append(message)
                time.sleep(.5)
            future.result(timeout=20)
        assert self.sql(f"SELECT SCHEMA_NAME FROM QSYS2.SYSSCHEMAS WHERE SCHEMA_NAME='{lib}'", "schema-absent.json") == []
        assert self.sql(f"SELECT OBJNAME FROM TABLE(QSYS2.OBJECT_STATISTICS('QSYS','*LIB','{lib}')) X", "library-absent.json") == []
        assert self.sql(f"SELECT MESSAGE_ID FROM TABLE(QSYS2.MESSAGE_QUEUE_INFO(MESSAGE_FILTER => 'INQUIRY')) X WHERE MESSAGE_TEXT LIKE '%{lib}%'", "inquiries-absent.json") == []
        result = {"status": "passed", "library": lib, "schema_absent": True, "library_absent": True, "inquiries_absent": True, "replies": replies}
        self.save("cleanup.json", result)
        return result
