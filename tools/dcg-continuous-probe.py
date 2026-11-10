#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded continuous D/CG experiment, real owned edgeqa fixture and local Connect."""
import base64
import decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("native_connect", ROOT / "tools/dcg-native-connect-probe.py")
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)
base = native.base
CLASS = "io.debezium.connector.db2as400.As400ContinuousConnectProbeConnector"
native.CLASS = CLASS
native.local.ownership.CLASS = CLASS


class MixedFixture(native.fixture_module.NativeFixture):
    def setup(self):
        super().setup()
        lib = self.library
        self.sql(f"DROP TABLE {lib}.M RESTRICT", "replace-mixed.log", True)
        self.owned_objects = self.inventory("objects-mixed-removed.json")
        self.sql(f"CREATE TABLE {lib}.M (ID INTEGER NOT NULL PRIMARY KEY, S SMALLINT, N NUMERIC(6,2), D DECIMAL(9,3), C CHAR(5) CCSID 37, V VARCHAR(7) CCSID 500, B BINARY(4), VB VARBINARY(8), L BIGINT, F REAL, G DOUBLE)", "create-mixed.log", True)
        self.owned_objects = self.inventory("objects-mixed-created.json")
        for i in [1, 2, 3]:
            self.sql(f"INSERT INTO {lib}.M VALUES ({i},12,123.45,1234.567,'ABC','Ä![]',BX'00010203',BX'00FF',9007199254740993,1.25,-2.5)", "seed-mixed-" + str(i) + ".log", True)
        journaling = self.sql(f"SELECT JOURNAL_NAME,JOURNAL_LIBRARY,JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS('{lib}','*FILE','M')) X", "mixed-journaling.json")
        assert journaling == [{"JOURNAL_NAME": "QSQJRN", "JOURNAL_LIBRARY": lib, "JOURNAL_IMAGES": "*BOTH"}]



class ContinuousExperiment(native.NativeExperiment):
    def __init__(self, directory, library):
        super().__init__(directory, library)
        # Same credential owner/safety checks, with a mixed-column fixture setup.
        self.fixture.__class__ = MixedFixture
        self.report["continuous"] = True
        self.report["read_requests"] = "none; autonomous incremental source poll"
        self.save()

    def stage(self):
        super().stage()
        classes = ROOT / "debezium-connector-ibmi/target/test-classes"
        with zipfile.ZipFile(self.plugin / "synthetic-probe.jar", "a") as jar:
            for name in ["As400ContinuousConnectProbeConnector", "As400ContinuousConnectSource"]:
                for path in classes.rglob(name + "*.class"):
                    jar.write(path, str(path.relative_to(classes)))
        files = [ROOT / "tools/dcg-continuous-probe.py", ROOT / "tools/dcg-continuous-probe"]
        files += list((ROOT / "debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400").glob("As400Continuous*.java"))
        files += list((ROOT / "debezium-connector-ibmi/src/main/java/io/debezium/connector/db2as400").glob("*.java"))
        self.report["harness_sha256"].update({str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
        self.report["plugin_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report["scope"] = "Native acquired mixed schemas; autonomous incremental F/W replay; separate R; receiver rotation; actual Connect and dispatcher; test-only entry point"
        self.save()

    def rest(self, worker, endpoint, payload=None, tolerate=False):
        if payload and isinstance(payload, dict) and payload.get("config", {}).get("connector.class") == CLASS:
            payload["config"]["producer.override.interceptor.classes"] = CLASS + "$ContinuousInterceptor"
        return super().rest(worker, endpoint, payload, tolerate=tolerate)

    def healthy(self, generation):
        failed = self.control / (generation + ".failed.json")
        if failed.exists():
            raise RuntimeError("Continuous native task failed: " + failed.read_text())

    def progress(self, worker, generation, label, predicate=lambda p: True):
        def found():
            self.healthy(generation)
            path = self.control / (generation + ".progress.json")
            try:
                progress = json.loads(path.read_text()) if path.exists() else None
            except json.JSONDecodeError:
                return None
            return progress if progress and predicate(progress) else None
        progress = base.wait_for("continuous " + label, found, 120)
        cp = self.checkpoint(worker, "native", label)
        # Autonomous polling may advance since the marker, never regress. Require
        # the exact marker checkpoint to appear in the independent durable log.
        offsets = self.snapshot(worker, "native", label + "-all", offsets=True)
        self.check(label + ": continuous progress acknowledged in actual Kafka offset log",
                   any(r["key"] == ["native", {"server": "test"}] and r["value"] == progress["checkpoint"] for r in offsets)
                   and int(cp["completed_through.sequence"]) >= int(progress["checkpoint"]["completed_through.sequence"]), "control/")
        self.report.setdefault("observations", []).append({"label": label, "generation": generation, **progress})
        self.save()
        return progress

    def rows_until(self, worker, generation, label, predicate):
        def found():
            self.healthy(generation)
            rows = self.snapshot(worker, "native", label)
            return rows if predicate(rows) else None
        return base.wait_for(label, found, 120)

    @staticmethod
    def mixed(rows):
        return [r for r in rows if r.get("fixture_table") == "M" and isinstance(r["value"], dict) and "op" in r["value"]]

    @staticmethod
    def expected_mixed(identifier, changed=False):
        return {"ID": identifier, "S": -123 if changed else 12, "N": decimal.Decimal('-12.34' if changed else '123.45'),
                "D": decimal.Decimal('0.001' if changed else '1234.567'), "C": 'XYZ  ' if changed else 'ABC  ',
                "V": None if changed else 'Ä![]', "B": b'\xff\x00\x80\x7f' if changed else b'\x00\x01\x02\x03',
                "VB": b'\x10\x00\xff' if changed else b'\x00\xff', "L": -9007199254740993 if changed else 9007199254740993,
                "F": -1.5 if changed else 1.25, "G": 3.25 if changed else -2.5}

    @staticmethod
    def decode_mixed(row):
        result = dict(row)
        for column, scale in [("N", 2), ("D", 3)]:
            value = result[column]
            result[column] = decimal.Decimal(int.from_bytes(base64.b64decode(value), 'big', signed=True)).scaleb(-scale)
        for column in ["B", "VB"]:
            result[column] = base64.b64decode(result[column])
        return result

    def raw_cases(self):
        raw = [r for path in self.control.glob("native-*/interval-*-raw.json") for r in json.loads(path.read_text())]
        clears = {(r['receiver'], r['sequence']): r for r in raw if r['code'] == 'D' and r['type'] == 'CG'}
        self.check("native subtype 8 TRUNCATE and subtype 3 optimized DELETE accepted through strict continuous replay",
                   {r['subtype'] for r in clears.values()} == {'3', '8'} and len(clears) == 4 and all(r['file'] == 'B' for r in clears.values()), "control/*/interval-*-raw.json")
        self.report["raw_clears"] = list(clears.values())
        for folder in self.control.glob('native-*'):
            if not folder.is_dir():
                continue
            entries = [r for path in sorted(folder.glob('interval-*-raw.json')) for r in json.loads(path.read_text())]
            positions = [(r['receiver'], r['sequence']) for r in entries]
            self.check(folder.name + ": no journal entry reprocessed across incremental polls", len(positions) == len(set(positions)), str(folder))
        self.save()

    def run_native(self):
        self.fixture.setup()
        self.report["source_connected"] = True
        self.save()
        self.controller = self.container("controller", ["--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{self.plugin}:/probe:ro",
                    "-v", f"{self.control}:/control", "--entrypoint", "/bin/sh", base.IMAGE, "-c", "sleep 2400"])
        self.command("docker", "network", "connect", "--gw-priority", "1", self.egress, self.controller)
        scenario = "native"
        worker = self.start_worker(scenario, "w1")
        self.create(worker, scenario)
        first = self.generation(scenario, "w1")
        self.signal(first, "ack-block", 9)
        self.signal(first, "permit", 10)
        self.marker(first, "ack-blocked")
        pending = self.checkpoint(worker, scenario, "snapshot-pending")
        self.check("native snapshot producer ack is insufficient for history/F/W", pending.get("initialization.phase") == "snapshot"
                   and not self.history(worker, scenario, "snapshot-pending") and not (self.control / (first + ".handoff.json")).exists(), "control/")
        rows = self.snapshot(worker, scenario, "snapshot-pending")
        self.check("actual native snapshot contains all rows in three tables", all(self.data(rows, t) == [("r", i) for i in [1, 2, 3]] for t in ["A", "B", "M"]), "control/")
        self.signal(first, "ack-release")
        self.marker(first, "history-durable")
        self.check("native binding history is durable before initial F/W", len(self.history(worker, scenario, "history-durable")) == 1
                   and self.checkpoint(worker, scenario, "history-durable").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(first, "ack-block", 10)
        (self.control / (first + ".ack-release")).unlink()
        (self.control / (first + ".ack-blocked.json")).unlink()
        self.signal(first, "permit", 11)
        self.marker(first, "ack-blocked")
        self.check("held initial offset acknowledgement prevents native reader handoff", not (self.control / (first + ".handoff.json")).exists()
                   and self.checkpoint(worker, scenario, "initial-pending").get("initialization.phase") == "baseline-complete", "control/")
        self.signal(first, "ack-release")
        self.marker(first, "checkpoint-durable")
        initial = self.checkpoint(worker, scenario, "initial")
        boundary = json.loads((self.control / first / "native-boundary.json").read_text())
        self.check("native initial F=W equals verified journal boundary exclusively", initial["replay_from.sequence"] == boundary["sequence"]
                   and initial["completed_through.sequence"] == boundary["sequence"] and initial["replay_from.inclusive"] == "false", "control/")
        self.stop(worker)
        worker = self.start_worker(scenario, "w2")
        generation = self.generation(scenario, "w2")
        self.marker(generation, "session-released")
        self.check("crash after durable initialization resumes without another snapshot", json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == initial
                   and not (self.control / (generation + ".snapshot-prepared.json")).exists(), "control/")
        mixed = self.mixed(rows)
        self.check("native snapshot preserves every acquired mixed column, decimal scale, CCSID text, binary, large integer and key",
                   len(mixed) == 3 and all(self.decode_mixed(r["value"]["after"]) == self.expected_mixed(i)
                   and r["key"] == {"ID": i} for r, i in zip(mixed, [1, 2, 3])), "control/snapshot-pending-combined-read_committed.json")
        self.signal(generation, "permit", 10000)
        self.progress(worker, generation, "initial-idle")
        process, directory = self.start_transaction("truncate-commit")
        rows = self.rows_until(worker, generation, "truncate-open", lambda rs: ("c", 8) in self.data(rs, "A"))
        opened = self.progress(worker, generation, "open-clear", lambda p: p["checkpoint"]["replay_from.inclusive"] == "true"
                               and int(p["checkpoint"]["replay_from.sequence"]) < int(p["checkpoint"]["completed_through.sequence"]))
        self.check("unrelated A:8 progresses while native clear/reload stays hidden", self.data(rows, "B") == [("r", i) for i in [1, 2, 3]]
                   and process.poll() is None, "control/")
        self.fixture.sql(f"UPDATE {self.library}.M SET S=-123,N=-12.34,D=0.001,C='XYZ',V=NULL,B=BX'FF00807F',VB=BX'1000FF',L=-9007199254740993,F=-1.5,G=3.25 WHERE ID=1", "mixed-update.log", True)
        rows = self.rows_until(worker, generation, "mixed-update", lambda rs: any(r["value"]["op"] == "u" for r in self.mixed(rs)))
        update = next(r for r in self.mixed(rows) if r["value"]["op"] == "u")
        self.check("mixed-column update progresses during open clear with exact before/after and source metadata", update["key"] == {"ID": 1}
                   and self.decode_mixed(update["value"]["before"]) == self.expected_mixed(1)
                   and self.decode_mixed(update["value"]["after"]) == self.expected_mixed(1, True)
                   and update["value"]["source"]["receiver_library"] == self.library
                   and update["value"]["source"]["receiver"] == json.loads((self.fixture.directory / "setup.json").read_text())["receiver"]
                   and int(update["value"]["source"]["sequence"]) > 0
                   and process.poll() is None, "control/mixed-update-combined-read_committed.json")
        old_receiver, new_receiver = self.fixture.rotate()
        self.fixture.sql(f"INSERT INTO {self.library}.A VALUES (81)", "independent-after-rotation.log", True)
        rows = self.rows_until(worker, generation, "after-rotation", lambda rs: ("c", 81) in self.data(rs, "A"))
        rotated = self.progress(worker, generation, "rotated", lambda p: p["checkpoint"]["completed_through.receiver"] == new_receiver)
        self.check("receiver rotation preserves old F while W and independent CDC advance on successor", rotated["checkpoint"]["replay_from.receiver"] == old_receiver
                   and self.data(rows, "B") == [("r", i) for i in [1, 2, 3]] and process.poll() is None, "native/rotation-*.json; control/")
        saved = self.checkpoint(worker, scenario, "pre-crash")
        self.stop(worker)
        worker = self.start_worker(scenario, "w3")
        generation = self.generation(scenario, "w3")
        self.marker(generation, "session-released")
        self.check("replacement loads durable cross-receiver F/W without snapshot or current catalog substitution",
                   json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == saved
                   and not (self.control / (generation + ".snapshot-prepared.json")).exists(), "control/")
        self.signal(generation, "permit", 10000)
        self.fixture.sql(f"INSERT INTO {self.library}.A VALUES (82)", "independent-after-restart.log", True)
        rows = self.rows_until(worker, generation, "restarted-open", lambda rs: ("c", 82) in self.data(rs, "A"))
        resumed = self.progress(worker, generation, "resumed-open", lambda p: int(p["checkpoint"]["completed_through.sequence"]) > int(saved["completed_through.sequence"]))
        self.check("replacement reconstructs open clear across receivers without replay duplicates", self.data(rows, "A") == [("r", i) for i in [1, 2, 3]] + [("c", i) for i in [8, 81, 82]]
                   and self.data(rows, "B") == [("r", i) for i in [1, 2, 3]] and len(self.mixed(rows)) == 4
                   and resumed["checkpoint"]["replay_from.sequence"] == saved["replay_from.sequence"] and process.poll() is None, "control/")
        self.signal(generation, "hold-truncate-send")
        self.finish_transaction("truncate-commit", process, directory)
        self.marker(generation, "send-blocked")
        blocked = json.loads((self.control / (generation + ".send-blocked.json")).read_text())
        self.check("fault blocks the actual native truncate producer send", blocked["op"] == "t", "control/")
        accepted = self.checkpoint(worker, scenario, "before-fence")
        self.command("docker", "pause", worker)
        replacement = self.start_worker(scenario, "w4")
        fresh = self.generation(scenario, "w4")
        self.marker(fresh, "session-released")
        self.signal(fresh, "permit", 10000)
        expected_b = [("r", i) for i in [1, 2, 3]] + [("t", None)] + [("c", i) for i in [100, 101, 102]]
        before = self.rows_until(replacement, fresh, "commit-reassigned", lambda rs: self.data(rs, "B") == expected_b)
        self.progress(replacement, fresh, "committed", lambda p: int(p["checkpoint"]["completed_through.sequence"]) > int(accepted["completed_through.sequence"]))
        # Drain the replacement before the stale send so the independent audit
        # can require exact output/offset equality; no idle marker can obscure it.
        self.stop_task(replacement, scenario)
        self.marker(fresh, "drained")
        before = self.snapshot(replacement, scenario, "commit-drained")
        committed = self.checkpoint(replacement, scenario, "commit-reassigned")
        self.check("reassigned task uses durable F/W and publishes truncate before reload", json.loads((self.control / (fresh + ".start.json")).read_text())["loaded"] == accepted,
                   "control/")
        self.signal(generation, "send-release")
        self.command("docker", "unpause", worker)
        self.marker(generation, "send-released")
        base.wait_for("old native task producer broker fenced", lambda: re.search(r"InvalidProducerEpochException|ProducerFencedException", self.command("docker", "logs", worker)), 90)
        after = self.snapshot(replacement, scenario, "after-fence")
        cp = self.checkpoint(replacement, scenario, "after-fence")
        self.check("broker fencing prevents stale output and offset regression", after == before and cp == committed, "control/; native-w3.native.log")
        self.stop(worker)
        self.stop_task(replacement, scenario)
        retired = [json.loads(p.read_text())["generation"] for p in self.control.glob(scenario + "-w4-*.start.json")]
        cp = self.checkpoint(replacement, scenario, "post-rebalance")
        self.request(replacement, scenario, "PUT", "/resume")
        generation = self.generation(scenario, "w4", exclude=retired)
        self.marker(generation, "session-released")
        self.check("legitimate post-rebalance generation resumes durable checkpoint", json.loads((self.control / (generation + ".start.json")).read_text())["loaded"] == cp, "control/")
        self.signal(generation, "permit", 10000)
        worker = replacement
        for mode, subtype, aid in [("delete-rollback", 3, 9), ("truncate-rollback", 8, 10), ("delete-commit", 3, 11)]:
            process, directory = self.start_transaction(mode)
            rows = self.rows_until(worker, generation, mode + "-open", lambda rs: ("c", aid) in self.data(rs, "A"))
            opened = self.progress(worker, generation, mode + "-open", lambda p: p["checkpoint"]["replay_from.inclusive"] == "true")
            self.check(mode + ": unrelated CDC progresses and clear stays withheld", self.data(rows, "B") == expected_b and process.poll() is None, "control/")
            self.finish_transaction(mode, process, directory)
            # The terminal may advance scan R without advancing completed-output W.
            self.progress(worker, generation, mode + "-terminal", lambda p: int(p['R'].get('sequence', '0')) > int(opened['R']['sequence']))
            if mode == 'delete-commit':
                expected_b += [("t", None)] + [("c", i) for i in [400, 401, 402]]
                rows = self.rows_until(worker, generation, mode + "-output", lambda rs: self.data(rs, "B") == expected_b)
            else:
                rows = self.snapshot(worker, scenario, mode + "-output")
            self.check(mode + ": output matches commit/rollback outcome", self.data(rows, "B") == expected_b, "control/")
        self.fixture.verify_rows({"A": [1, 2, 3, 8, 9, 10, 11, 81, 82], "B": [400, 401, 402], "M": [1, 2, 3], "U": [1, 2, 3]})
        self.stop_task(worker, scenario)
        self.marker(generation, "drained")
        self.check("terminal reader/JDBC drain observed separately from stop request", any(e['event'] == 'stopRequested' for e in self.events(generation))
                   and json.loads((self.control / (generation + '.drained.json')).read_text())['stopped'], 'control/')
        before_missing = self.snapshot(worker, scenario, 'before-missing-offset')
        history_before = self.history(worker, scenario, 'before-missing-offset')
        retired = [json.loads(p.read_text())["generation"] for p in self.control.glob(scenario + "-w4-*.start.json")]
        self.request(worker, scenario, 'PATCH', '/offsets', {"offsets": [{"partition": {"server": "test"}, "offset": None}]})
        self.request(worker, scenario, 'PUT', '/resume')
        missing = self.generation(scenario, 'w4', exclude=retired)
        self.marker(missing, 'failed')
        failure = json.loads((self.control / (missing + '.failed.json')).read_text())
        self.check("missing offset with existing history/output rejects with zero publication", 'Missing durable offset' in failure['error']
                   and self.snapshot(worker, scenario, 'after-missing-offset') == before_missing
                   and self.history(worker, scenario, 'after-missing-offset') == history_before
                   and self.checkpoint(worker, scenario, 'after-missing-offset') is None, 'control/')
        self.stop_task(worker, scenario)
        self.marker(missing, 'drained')
        self.stop(worker)
        self.raw_cases()
        self.check("continuous source needed no controller cuts or custom owner/receipt gates", not list(self.control.glob('*.read'))
                   and not list(self.control.glob('*.registry*')) and not list(self.control.glob('*.authorized-worker*')), 'control/')
        self.check("all fault hooks completed within bounds", not list(self.control.glob('*.hook-timeout.json')), 'control/')
        self.report['status'] = 'passed'


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: dcg-continuous-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd (explicit edgeqa fixture writes; local Kafka)")
    experiment = ContinuousExperiment(Path(sys.argv[1]), sys.argv[2])
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.run_native()
    except BaseException as error:
        experiment.report.update(status='failed', error=experiment.fixture.redact(str(error)))
        experiment.save()
        if experiment.report.get('running_scenario'):
            try:
                experiment.capture_failure()
            except Exception as diagnostic_error:
                experiment.report['failure_evidence_error'] = str(diagnostic_error)
        raise
    finally:
        experiment.cleanup()
    assert experiment.report['status'] == 'passed', experiment.report.get('error', 'Cleanup failed')
    print(experiment.directory / 'report.json', flush=True)


if __name__ == '__main__':
    main()
