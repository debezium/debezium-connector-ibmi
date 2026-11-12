#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit bounded census/log experiment. No Kafka publication or startup admission."""
import datetime
from collections import deque
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("native_fixture", ROOT / "tools/dcg-native-fixture.py")
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)

DEFINITIONS = """SELECT LOCK_SPACE_ID, JOB_NAME, LOGICAL_UNIT_OF_WORK_STATE, ASPGRP,
 RESOURCE_LOCATION, XA_TRANSACTION_BRANCH_STATE FROM QSYS2.DB_TRANSACTION_INFO"""
RESOURCES = """SELECT JOURNAL_LIBRARY, JOURNAL_NAME, COMMIT_CYCLE, PENDING_CHANGES,
 RECORD_ROLLBACK_PERCENT FROM TABLE(QSYS2.DB_TRANSACTION_JOURNAL_INFO(?)) T
 WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?"""
AUTHORITY = """SELECT QSYS2.SQL_CHECK_SPECIAL_AUTHORITY('*JOBCTL') AS JOBCTL,
 QSYS2.SQL_CHECK_FUNCTION_USAGE('QIBM_DB_SQLADM') AS SQLADM,
 QSYS2.SQL_CHECK_FUNCTION_USAGE('QIBM_DB_SYSMON') AS SYSMON FROM SYSIBM.SYSDUMMY1"""
MAX_DEFINITIONS = 256
MEMBERSHIP = """SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE,
 JOURNALED_COMMITMENT_DEFINITIONS FROM QSYS2.JOURNAL_INFO
 WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?"""
DIRECTORY = """SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE,
 NUMBER_JOURNAL_RECEIVERS, ATTACHED_JOURNAL_RECEIVER_LIBRARY, ATTACHED_JOURNAL_RECEIVER_NAME,
 JOURNAL_CACHE, REMOVE_INTERNAL_ENTRIES, REMOVE_FIXED_LENGTH_DETAIL
 FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?"""
PENDING_RECEIVERS = """SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_RECEIVER_LIBRARY,
 JOURNAL_RECEIVER_NAME, STATUS, FIRST_SEQUENCE_NUMBER, LAST_SEQUENCE_NUMBER,
 PREVIOUS_JOURNAL_RECEIVER_LIBRARY, PREVIOUS_JOURNAL_RECEIVER,
 NEXT_JOURNAL_RECEIVER_LIBRARY, NEXT_JOURNAL_RECEIVER, PENDING_TRANSACTIONS
 FROM QSYS2.JOURNAL_RECEIVER_INFO WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?
 ORDER BY FIRST_SEQUENCE_NUMBER"""
MAX_HISTORY_RECEIVERS = 16


def save(path, value):
    path.write_text(json.dumps(value, default=str, indent=2) + "\n")


def stamp():
    return {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns()}


def rows(connection, sql, parameters=(), maximum=128):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, *parameters)
        result = cursor.fetchmany(maximum + 1)
        if len(result) > maximum or cursor.fetchone() is not None:
            raise ValueError("Bounded SQL response overflow")
        if cursor.messages or getattr(connection, "messages", []):
            raise ValueError("SQL warning: incomplete observation")
        return [dict(zip([column[0] for column in cursor.description], row)) for row in result]
    finally:
        cursor.close()


def require_authority(result):
    if len(result) != 1 or set(result[0]) != {"JOBCTL", "SQLADM", "SYSMON"}:
        raise ValueError("Incomplete effective authority observation")
    values = list(result[0].values())
    if any(value not in [0, 1] for value in values) or not any(value == 1 for value in values):
        raise ValueError("Whole-system commitment visibility is unavailable")


def membership(connection, library):
    """Independent scalar observation, not an active-transaction count or proof.

    The caller supplies the bounded read-only connection and outer raw journal
    anchors. SQL errors/warnings propagate; they must never become a zero count.
    """
    result = {"started": stamp(), "sql": MEMBERSHIP, "parameters": [library, "QSQJRN"],
              "maximum_rows": 1, "startup_proof": False}
    try:
        result["rows"] = rows(connection, MEMBERSHIP, (library, "QSQJRN"), 1)
        membership_count(result, library)
        return result
    finally:
        result["finished"] = stamp()


def membership_count(observation, library):
    observed = observation["rows"]
    if len(observed) != 1:
        raise ValueError("Exactly one journal membership observation required")
    row = observed[0]
    if ((row["JOURNAL_LIBRARY"], row["JOURNAL_NAME"], row["JOURNAL_TYPE"], row["JOURNAL_STATE"])
            != (library, "QSQJRN", "*LOCAL", "*ACTIVE")):
        raise ValueError("Wrong journal or unsupported membership profile")
    count = row["JOURNALED_COMMITMENT_DEFINITIONS"]
    if type(count) is not int or not 0 <= count <= MAX_DEFINITIONS:
        raise ValueError("Unknown or excessive journal membership count")
    return count


def audit_membership(sample, before, after, bracket, library):
    """Additional test-only consistency check on an independently verified bracket.

    Equal counts alone are insufficient: balanced arrivals/departures can hide an
    omitted definition. Reject membership transitions, count every resource even
    with a null cycle/zero pending changes, and retain the existing native-history
    audit. This does not establish platform-wide API visibility/ordering or make
    historical schemas trustworthy, and cannot admit migration.
    """
    result = {"status": "rejected", "startup_proof": False, "visibility_proof": False}
    try:
        def interval(value):
            start, end = value["started"]["monotonic_ns"], value["finished"]["monotonic_ns"]
            if type(start) is not int or type(end) is not int or not 0 < start < end:
                raise ValueError("Malformed membership observation interval")
            return start, end

        lower, census, upper = interval(before), interval(sample), interval(after)
        if not lower[1] <= census[0] < census[1] <= upper[0] or sample["status"] != "observed":
            raise ValueError("Complete, ordered census bracket required")
        counts = [membership_count(value, library) for value in [before, after]]
        definitions = sample["definitions"]
        spaces = [definition["LOCK_SPACE_ID"] for definition in definitions]
        resources = sample["resources"]
        observed = [resource["lock_space"] for resource in resources]
        if (len(spaces) > MAX_DEFINITIONS or len(set(spaces)) != len(spaces)
                or any(not isinstance(space, str) or len(space) != 20 for space in spaces)
                or len(set(observed)) != len(observed) or not set(observed) <= set(spaces)):
            raise ValueError("Incomplete or duplicate definition identities")
        for resource in resources:
            if (resource["JOURNAL_LIBRARY"], resource["JOURNAL_NAME"]) != (library, "QSQJRN"):
                raise ValueError("Resource belongs to another journal")
        if counts != [len(resources), len(resources)]:
            raise ValueError("Journal membership count disagrees with census")
        if not bracket:
            raise ValueError("Retained raw journal bracket required")
        if any(entry["type"] in ["C/BC", "C/EC", "C/BA"] for entry in bracket):
            raise ValueError("Commitment membership changed or recovery occurred inside bracket")
        result.update(status="membership-consistent-unqualified", journaled_definitions=counts[0],
                      observed_resources=len(resources))
    except (ValueError, KeyError, TypeError, IndexError) as error:
        result["reason"] = str(error)
    return result


def receiver_history(connection, library, hook=lambda: None):
    """Read-only bounded alternative to enumerating commitment definitions.

    The pending flag describes transactions with entries in a receiver. It is
    not an assertion that an old legacy offset is a closed boundary. Callers
    must retain the surrounding unfiltered raw bracket and native identities.
    This diagnostic does not admit startup or replace the existing census.
    """
    result = {"started": stamp(), "startup_proof": False, "queries": []}
    deadline = time.monotonic() + 60

    def query(name, sql, maximum):
        if time.monotonic() >= deadline:
            raise TimeoutError("Receiver observation deadline exceeded")
        item = {"name": name, "sql": sql, "parameters": [library, "QSQJRN"],
                "maximum_rows": maximum, "started": stamp()}
        result["queries"].append(item)
        try:
            item["rows"] = rows(connection, sql, (library, "QSQJRN"), maximum)
            if time.monotonic() >= deadline:
                raise TimeoutError("Receiver observation deadline exceeded")
            return item["rows"]
        finally:
            item["finished"] = stamp()

    try:
        result["before"] = query("directory-before", DIRECTORY, 1)
        hook()
        result["receivers"] = query("pending-receivers", PENDING_RECEIVERS, MAX_HISTORY_RECEIVERS)
        result["after"] = query("directory-after", DIRECTORY, 1)
        result["status"] = "observed"
    except Exception as error:
        result.update(status="rejected", error=str(error))
    finally:
        result["finished"] = stamp()
    return result


def receiver_history_floor(sample, bracket, library):
    """Candidate back-read floor, not an inventory or startup certificate.

    A silent open cycle keeps its original receiver pending. A cycle ending
    during observation might clear that flag, so its original C/SC coordinate
    must also be recovered from the raw bracket. New cycles are in that bracket.
    Neither a zero pending-row count nor the lagging legacy offset is used as W.
    Native identity/history validation and original-start validation are still
    required before decoding; current metadata cannot qualify older schemas.
    """
    result = {"status": "rejected", "startup_proof": False, "visibility_proof": False}
    try:
        def require(condition, message):
            if not condition:
                raise ValueError(message)

        def integer(value):
            require(not isinstance(value, bool) and re.fullmatch(r"[0-9]+", str(value)) is not None,
                    "Unknown receiver sequence or count")
            number = int(value)
            require(0 <= number < 2 ** 64, "Receiver sequence exceeds native range")
            return number

        def name(row, prefix, name_column):
            lib, obj = row[prefix], row[name_column]
            require((lib is None) == (obj is None), "Partial receiver link")
            if lib is None:
                return None
            require(all(isinstance(v, str) and re.fullmatch(r"[A-Z#$@][A-Z0-9_#$@]{0,9}", v) for v in [lib, obj]),
                    "Invalid native receiver name")
            return lib, obj

        require(sample["status"] == "observed", "Incomplete receiver observation")
        before, after, receivers = sample["before"], sample["after"], sample["receivers"]
        require(len(before) == len(after) == 1 and before == after, "Journal directory changed during observation")
        journal = before[0]
        require((journal["JOURNAL_LIBRARY"], journal["JOURNAL_NAME"], journal["JOURNAL_TYPE"], journal["JOURNAL_STATE"])
                == (library, "QSQJRN", "*LOCAL", "*ACTIVE"), "Unsupported journal")
        require(all(journal[field] == "NO" for field in ["JOURNAL_CACHE", "REMOVE_INTERNAL_ENTRIES", "REMOVE_FIXED_LENGTH_DETAIL"]),
                "Cached or filtered journal cannot establish history")
        require(1 <= len(receivers) <= MAX_HISTORY_RECEIVERS and integer(journal["NUMBER_JOURNAL_RECEIVERS"]) == len(receivers),
                "Incomplete receiver directory")
        attached = name(journal, "ATTACHED_JOURNAL_RECEIVER_LIBRARY", "ATTACHED_JOURNAL_RECEIVER_NAME")
        ranges, pending, previous = {}, [], None
        for row in receivers:
            require((row["JOURNAL_LIBRARY"], row["JOURNAL_NAME"]) == (library, "QSQJRN"), "Wrong receiver journal")
            key = name(row, "JOURNAL_RECEIVER_LIBRARY", "JOURNAL_RECEIVER_NAME")
            require(key is not None and key not in ranges, "Missing or duplicate receiver")
            first, last = integer(row["FIRST_SEQUENCE_NUMBER"]), integer(row["LAST_SEQUENCE_NUMBER"])
            require(0 < first <= last, "Unavailable or reset receiver range")
            require(row["STATUS"] in ["ONLINE", "SAVED", "ATTACHED"] and row["PENDING_TRANSACTIONS"] in ["YES", "NO"],
                    "Unavailable receiver or unknown pending state")
            prior = name(row, "PREVIOUS_JOURNAL_RECEIVER_LIBRARY", "PREVIOUS_JOURNAL_RECEIVER")
            following = name(row, "NEXT_JOURNAL_RECEIVER_LIBRARY", "NEXT_JOURNAL_RECEIVER")
            if previous is not None:
                require(prior == previous[0] and previous[2] == key and first > previous[1],
                        "Incomplete receiver links or sequence reset")
            ranges[key] = (first, last)
            if row["PENDING_TRANSACTIONS"] == "YES":
                pending.append(first)
            require((key == attached) == (row["STATUS"] == "ATTACHED"), "Inconsistent journal attachment")
            previous = key, last, following
        require(previous[0] == attached and previous[2] is None, "Receiver directory does not end at attachment")
        require(bracket, "Retained raw observation bracket required")
        sequences, cycles = [], []
        for entry in bracket:
            position = entry["position"]
            sequence = integer(position["sequence"])
            bounds = ranges.get((position["library"], position["receiver"]))
            # The independent raw upper anchor is collected after these rows;
            # the attached receiver may grow meanwhile. Its exact later bound
            # and incarnation belong to the existing native capture validator.
            require(bounds is not None and bounds[0] <= sequence
                    and (sequence <= bounds[1] or (position["library"], position["receiver"]) == attached),
                    "Bracket outside observed receiver directory")
            require(not sequences or sequences[-1] < sequence, "Repeated or unordered bracket")
            require(not entry["type"].startswith("J/") and entry["type"] not in ["C/BA", "C/CN"],
                    "Journal maintenance or abnormal recovery in observation bracket")
            sequences.append(sequence)
            cycle = integer(entry["cycle"])
            require(cycle <= sequence, "Cycle starts after its own record")
            if cycle:
                cycles.append(cycle)
        floor = min([sequences[0], *pending, *cycles])
        owners = [key for key, (first, last) in ranges.items() if first <= floor <= last]
        require(len(owners) == 1, "Required original cycle history is unavailable")
        result.update(status="receiver-floor-candidate", floor={"library": owners[0][0], "receiver": owners[0][1], "sequence": str(floor)},
                      pending_receivers=len(pending), bracket_cycles=sorted(set(cycles)))
    except (ValueError, KeyError, TypeError, IndexError) as error:
        result["reason"] = str(error)
    return result


def census(connection, library, hook=lambda definitions: None, before_definitions=lambda: None):
    result = {"status": "collecting", "started": stamp(), "queries": [], "resources": [], "startup_proof": False}
    deadline = time.monotonic() + 60

    def query(name, sql, params=(), maximum=128):
        if time.monotonic() >= deadline:
            raise TimeoutError("Census deadline exceeded")
        item = {"name": name, "sql": sql, "parameters": list(params), "maximum_rows": maximum, "started": stamp()}
        result["queries"].append(item)
        try:
            item["rows"] = rows(connection, sql, params, maximum)
            if time.monotonic() >= deadline:
                raise TimeoutError("Census deadline exceeded")
            return item["rows"]
        finally:
            item["finished"] = stamp()

    try:
        require_authority(query("authority-before", AUTHORITY, maximum=1))
        before_definitions()  # Outside the measured query; never pad its interval with a test gate.
        definitions = query("definitions", DEFINITIONS, maximum=MAX_DEFINITIONS)
        result["definitions"] = definitions
        spaces = [definition["LOCK_SPACE_ID"].rstrip() for definition in definitions]
        if any(len(space) != 20 for space in spaces) or len(set(spaces)) != len(spaces):
            raise ValueError("Incomplete or duplicate commitment definitions")
        hook(definitions)
        for definition, space in zip(definitions, spaces):
            observed = query("resource", RESOURCES, (space, library, "QSQJRN"), 1)
            if observed:
                row = observed[0]
                if (row["JOURNAL_LIBRARY"].rstrip(), row["JOURNAL_NAME"].rstrip()) != (library, "QSQJRN"):
                    raise ValueError("Wrong journal resource")
                if any(definition[field].rstrip() != expected for field, expected in
                       [("ASPGRP", "*SYSBAS"), ("RESOURCE_LOCATION", "LOCAL")]) or definition["XA_TRANSACTION_BRANCH_STATE"] is not None:
                    raise ValueError("Unsupported distributed or XA definition")
                cycle = row["COMMIT_CYCLE"]
                if cycle is not None and (int(cycle) != cycle or int(cycle) <= 0):
                    raise ValueError("Invalid native commit cycle")
                result["resources"].append({"lock_space": space, "job": definition["JOB_NAME"], **row})
        require_authority(query("authority-after", AUTHORITY, maximum=1))
        result["status"] = "observed"
    except Exception as error:
        result["status"] = "rejected"
        result["error"] = str(error)
    result["finished"] = stamp()
    return result


def audit_enumeration_overlap(sample, ledger, job):
    """Client-call timing evidence only, not the server's enumeration/commit instant."""
    result = {"status": "rejected", "startup_proof": False, "job": job, "contained_terminals": [],
              "timing_scope": "client monotonic call intervals; no server linearization claim"}
    try:
        queries = [q for q in sample["queries"] if q["name"] == "definitions"]
        if sample["status"] != "observed" or len(queries) != 1:
            raise ValueError("Complete single definition query required")

        def interval(item):
            start, end = item["started"]["monotonic_ns"], item["finished"]["monotonic_ns"]
            if type(start) is not int or type(end) is not int or not 0 < start < end:
                raise ValueError("Malformed operation interval")
            return start, end

        lower, upper = interval(queries[0])
        result["definition_interval_ns"] = [lower, upper]
        sessions = [x for x in ledger if x["operation"] == "connection" and x["job"] == job]
        if len(sessions) != 1 or sessions[0]["transaction"] is not True or sessions[0]["read_only"] is not False:
            raise ValueError("Unique transactional writer session required")
        commands = [(i, x) for i, x in enumerate(ledger) if x["job"] == job and x["operation"] not in ["connection", "job-query-option"]]
        if len(commands) % 2:
            raise ValueError("Incomplete concurrent writer cycle")
        previous = 0
        for pair in range(0, len(commands), 2):
            (insert_index, insert), (terminal_index, terminal) = commands[pair:pair + 2]
            a, b = interval(insert)
            c, d = interval(terminal)
            if (insert["operation"] != "insert" or terminal["operation"] not in ["commit", "rollback"]
                    or insert.get("status") != "completed" or terminal.get("status") != "completed" or not previous <= a < b <= c < d):
                raise ValueError("Failed or unordered concurrent writer cycle")
            previous = d
            if lower <= c and d <= upper:
                result["contained_terminals"].append({"insert_ledger_index": insert_index, "terminal_ledger_index": terminal_index,
                                                      "operation": terminal["operation"], "interval_ns": [c, d],
                                                      "whole_cycle_contained": lower <= a})
        result["completed_cycles"] = len(commands) // 2
        result["status"] = "client-overlap-observed" if result["contained_terminals"] else "not-observed"
    except (ValueError, KeyError, TypeError, IndexError) as error:
        result["reason"] = str(error)
    return result


def enumeration_covered(attempts):
    statuses = [item["enumeration_overlap"]["status"] for item in attempts if "enumeration_overlap" in item]
    return "client-overlap-observed" in statuses and all(status in ["client-overlap-observed", "not-observed"] for status in statuses)


def audit_inventory(sample, bracket, oracle, lower, upper):
    """Compare the census with a second owned capture, never with itself.

    This experiment permits only the fixture's SEQOPT(*CONT) chain. It rejects
    ambiguous starts/outcomes rather than treating sequence arithmetic as proof.
    Matching captures cannot prove neither omitted the same native work. The
    writer ledger still requires strict row replay before qualification.
    """
    if sample["status"] != "observed":
        return {"status": "rejected", "reason": "Incomplete census", "startup_proof": False}
    expected = [entry for entry in oracle if lower <= int(entry["position"]["sequence"]) <= upper]
    # Live controller inputs come from Java's raw-checksummed section comparison.
    # Older synthetic unit fixtures retain their original exact-byte comparison.
    identity = lambda entry: (entry["position"], "content" if "content_sha256" in entry else "raw", entry.get("content_sha256", entry["sha256"]))
    if list(map(identity, bracket)) != list(map(identity, expected)):
        return {"status": "rejected", "reason": "Journal bracket disagrees with independent full capture", "startup_proof": False}
    starts, terminals, seen = {}, {}, set()
    for entry in oracle:
        cycle, sequence = int(entry["cycle"]), int(entry["position"]["sequence"])
        if cycle <= 0:
            continue
        seen.add(cycle)
        if entry["type"] == "C/SC":
            if cycle in starts or cycle != sequence:
                raise ValueError("Missing, duplicate or reset cycle start")
            starts[cycle] = sequence
        if entry["type"] in ["C/CM", "C/RB"]:
            if cycle in terminals:
                raise ValueError("Ambiguous terminal lifecycle")
            terminals[cycle] = sequence
    if seen - set(starts):
        return {"status": "rejected", "reason": "Incomplete oracle history", "unavailable_starts": sorted(seen - set(starts)), "startup_proof": False}
    required = {cycle for cycle, start in starts.items() if start <= upper and terminals.get(cycle, upper + 1) >= lower}
    observed = {int(row["COMMIT_CYCLE"]) for row in sample["resources"] if row["COMMIT_CYCLE"] is not None}
    observed |= {int(entry["cycle"]) for entry in bracket if int(entry["cycle"]) > 0}
    absent_starts = observed - set(starts)
    missing = required - observed
    return {"status": "capture-consistent" if not missing and not absent_starts else "rejected",
            "required_cycles": sorted(required), "observed_cycles": sorted(observed),
            "missed_cycles": sorted(missing), "unavailable_starts": sorted(absent_starts),
            "oracle_completeness": "unqualified-pending-writer-ledger-replay", "startup_proof": False}


def audit_ledger(ledger, replay, library):
    """Independently compare controlled SQL commands with strict native replay.

    This deliberately supports only this insert/clear/savepoint fixture grammar.
    It neither parses arbitrary SQL nor certifies platform visibility guarantees.
    """
    def require(condition, message):
        if not condition:
            raise ValueError(message)

    try:
        require(replay.get("status") == "replayed" and replay.get("startup_proof") is False, "Strict replay did not complete")
        jobs, queues, batches = {}, {}, {}
        for item in ledger:
            if item["operation"] == "connection":
                job = item["job"].strip()
                require(job not in jobs, "Repeated writer session identity")
                jobs[job] = {"transaction": item["transaction"], "read_only": item["read_only"], "cycle": None, "changes": [], "savepoints": {}}
                queues[job] = deque()
        # IBM i also emits internal savepoints for statements and clear lifecycles.
        # The strict lifecycle/buffer validates their targets. Independently model
        # the explicit SQL savepoints below and compare the resulting output;
        # do not pretend each internal control maps to a writer SQL command.
        ignored = {"C/SC", "C/BC", "C/EC", "C/R1", "C/SB", "C/SQ", "C/SU", "R/BR", "R/DR", "R/UR"}
        for event in replay["events"]:
            require(event["job"] in jobs and not jobs[event["job"]]["read_only"], "Journal producer is absent from writer ledger")
            if event["type"] not in ignored:
                queues[event["job"]].append(event)
        for batch in replay["batches"]:
            require(batch["sequence"] not in batches, "Duplicate completed batch")
            batches[batch["sequence"]] = batch
        consumed = set()
        operations = 0
        for item in ledger:
            operation = item["operation"]
            if operation in ["connection", "job-query-option"]:
                continue
            require(item.get("status") == "completed", "Incomplete or failed writer operation")
            job = item["job"].strip()
            state = jobs[job]
            require(not state["read_only"], "Read-only observer performed a write")
            sql = item.get("sql") or ""
            if operation == "savepoint":
                match = re.fullmatch(r"SAVEPOINT ([A-Z][A-Z0-9]*) ON ROLLBACK RETAIN CURSORS", sql)
                require(state["transaction"] and match is not None, "Unsupported fixture savepoint")
                require(match[1] not in state["savepoints"], "Ambiguous savepoint name reuse")
                state["savepoints"][match[1]] = len(state["changes"])
                operations += 1
                continue
            if operation == "rollback-to-savepoint":
                match = re.fullmatch(r"ROLLBACK TO SAVEPOINT ([A-Z][A-Z0-9]*)", sql)
                require(state["transaction"] and match is not None and match[1] in state["savepoints"], "Unresolved fixture savepoint rollback")
                state["changes"] = state["changes"][:state["savepoints"][match[1]]]
                names = list(state["savepoints"])
                state["savepoints"] = {name: state["savepoints"][name] for name in names[:names.index(match[1])]}
                operations += 1
                continue
            if operation in ["commit", "rollback"] and state["cycle"] is None:
                continue  # Explicit redundant completion with no work since the last terminal.
            require(queues[job], "Writer operation is missing from the journal")
            event = queues[job].popleft()
            cycle = int(event["cycle"])
            if state["transaction"]:
                require(cycle > 0, "Transactional writer lost its native cycle")
                require(state["cycle"] in [None, cycle], "Writer cycle changed before its outcome")
                state["cycle"] = cycle
            else:
                require(cycle == 0 and operation in ["insert", "unrelated-insert"], "Unexpected autocommit workload")
            if operation in ["insert", "unrelated-insert"]:
                match = re.fullmatch(r"INSERT INTO ([A-Z0-9_]+)\.([A-Z0-9_]+) VALUES \(([0-9]+)\)", sql)
                require(match is not None and match[1] == library, "Unsupported fixture INSERT")
                require(event["type"] in ["R/PT", "R/PX"] and (event["library"], event["file"], event.get("id")) ==
                        (library, match[2], int(match[3])), "Inserted row identity/value does not match the writer")
                state["changes"].append({"operation": "c", "library": library, "file": match[2], "id": int(match[3]), "sequence": event["sequence"]})
            elif operation in ["clear", "truncate"]:
                truncate = re.fullmatch(r"TRUNCATE TABLE ([A-Z0-9_]+)\.([A-Z0-9_]+) REUSE STORAGE IGNORE DELETE TRIGGERS", sql)
                delete = re.fullmatch(r"DELETE FROM ([A-Z0-9_]+)\.([A-Z0-9_]+)", sql)
                match = truncate or delete
                require(match is not None and match[1] == library, "Unsupported fixture clear")
                require(event["type"] == "D/CG" and event.get("subtype") == ("8" if truncate else "3")
                        and (event["library"], event["file"]) == (library, match[2]), "Clear subtype/object does not match the writer")
                state["changes"].append({"operation": "t", "library": library, "file": match[2], "sequence": event["sequence"]})
            elif operation in ["commit", "rollback"]:
                require(event["type"] == ("C/CM" if operation == "commit" else "C/RB"), "Transaction outcome differs from the writer")
            else:
                raise ValueError("Unsupported writer operation")
            if operation == "commit" or not state["transaction"]:
                batch = batches.get(event["sequence"])
                require(batch is not None and batch["job"] == job and batch["cycle"] == event["cycle"]
                        and batch["changes"] == state["changes"], "Completed output differs from independently reconstructed writer changes")
                consumed.add(event["sequence"])
                state.update(cycle=None, changes=[], savepoints={})
            elif operation == "rollback":
                require(event["sequence"] not in batches, "Rolled-back transaction produced output")
                state.update(cycle=None, changes=[], savepoints={})
            operations += 1
        require(all(not queue for queue in queues.values()), "Unaccounted journal operations remain")
        require(all(state["cycle"] is None for state in jobs.values()), "Writer ledger ends before a transaction outcome")
        require(consumed == set(batches), "Unaccounted output batches remain")
        require(operations > 0, "Empty workload cannot qualify")
        return {"status": "matched", "operations": operations, "completed_batches": len(consumed), "startup_proof": False}
    except (ValueError, KeyError, TypeError, IndexError) as error:
        return {"status": "rejected", "reason": str(error), "startup_proof": False}


class Experiment:
    def __init__(self, directory, library, repetitions):
        if not re.fullmatch(r"DCNC[0-9]{4}", library) or not 1 <= repetitions <= 10:
            raise ValueError("Fresh DCNCdddd library and 1..10 repetitions required")
        self.directory, self.library, self.repetitions = Path(directory), library, repetitions
        self.directory.mkdir()
        self.fixture = native.NativeFixture(self.directory / "native", library)
        self.connections = []
        self.ledger = []
        self.attempts = []
        self.progress = []
        self.stop = threading.Event()
        self.background_error = []
        self.report = {"library": library, "status": "preparing", "startup_proof": False, "source_connected": False,
                       "publication": "none", "attempts": self.attempts, "ledger": self.ledger, "background_errors": self.background_error}
        self.deadline = None
        self.writer_thread = None
        self.churn_thread = None
        self.churn_stop = threading.Event()
        self.churn_go = threading.Event()
        self.churn_ready = threading.Event()
        self.churn_job = None
        self.churn_sequence = 2000000
        self.sequence = 10000

    def persist(self):
        save(self.directory / "report.json", self.report)

    def connection(self, transaction=False, read_only=False):
        import pyodbc
        config = native.load_config()
        fields = {"DRIVER": config.get("odbc_driver", "IBM i Access ODBC Driver"), "SYSTEM": config["host"],
                  "UID": config["user"], "PWD": config["password"], "NAM": "0", "CMT": "2" if transaction else "0"}
        attrs = {pyodbc.SQL_ATTR_ACCESS_MODE: 1} if read_only else {}  # ODBC SQL_MODE_READ_ONLY
        connection = pyodbc.connect(";".join(key + "=" + native.odbc_value(value) for key, value in fields.items()),
                                    timeout=15, autocommit=not transaction, attrs_before=attrs)
        try:
            connection.timeout = 5
            if rows(connection, "SELECT CURRENT_SERVER AS SERVER FROM SYSIBM.SYSDUMMY1", maximum=1) != [{"SERVER": config["server"]}]:
                raise ValueError("Wrong server")
            if transaction:
                # This override belongs only to this fixture's new job, never a
                # system QAQQINI table. The job is closed during fixture cleanup.
                with closing(connection.cursor()) as cursor:
                    cursor.execute("CALL QSYS2.OVERRIDE_QAQQINI(1)")
                    cursor.execute("CALL QSYS2.OVERRIDE_QAQQINI(2, 'SQL_FAST_DELETE_ROW_COUNT', '1')")
            job = rows(connection, "SELECT QSYS2.JOB_NAME AS JOB FROM SYSIBM.SYSDUMMY1", maximum=1)[0]["JOB"]
            self.ledger.append({"operation": "connection", "job": job, "transaction": transaction, "read_only": read_only, **stamp()})
            if transaction:
                self.ledger.append({"operation": "job-query-option", "job": job, "SQL_FAST_DELETE_ROW_COUNT": "1",
                                    "scope": "new fixture session only", **stamp()})
            self.connections.append(connection)
            return connection, job
        except BaseException:
            connection.close()
            raise

    def execute(self, session, operation, sql=None):
        connection, job = session
        item = {"operation": operation, "job": job, "sql": sql, "started": stamp()}
        self.ledger.append(item)
        try:
            if operation == "commit":
                connection.commit()
            elif operation == "rollback":
                connection.rollback()
            else:
                # pyodbc's cursor context manager commits on successful exit.
                # Only the explicit commit/rollback ledger operation may end this cycle.
                with closing(connection.cursor()) as cursor:
                    cursor.execute(sql)
                    if cursor.messages:
                        raise ValueError("Writer SQL warning")
            item["status"] = "completed"
        except Exception as error:
            item.update(status="failed", error=self.fixture.redact(str(error)))
            raise
        finally:
            item["finished"] = stamp()

    def insert(self, session, table):
        self.sequence += 1
        self.execute(session, "insert", f"INSERT INTO {self.library}.{table} VALUES ({self.sequence})")

    def capture(self, label, mode, *arguments):
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise TimeoutError("Ten-minute fixture workload budget exhausted")
        command = self.java_command("As400OnlineCensusCapture", self.library, self.directory / label, mode, *arguments)
        process = subprocess.run(command, input=self.fixture.properties(), text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, timeout=120 if mode == "short-boundary" else 80)
        (self.directory / (label + ".log")).write_text(self.fixture.redact(process.stdout))
        if process.returncode:
            raise RuntimeError("Native capture rejected; see " + label + ".log")
        return self.directory / label / "capture.json"

    def java_command(self, entrypoint, *arguments):
        cp = (ROOT / "debezium-connector-ibmi/target/connect-probe-classpath.txt").read_text().strip()
        cp = ":".join([str(ROOT / "debezium-connector-ibmi/target/test-classes"), str(ROOT / "debezium-connector-ibmi/target/classes"),
                       str(ROOT / "journal-parsing/target/classes"), str(ROOT / "jt400-override-ccsid/target/classes"), cp])
        java = shutil.which("java")
        if not java:
            raise ValueError("Run through mise exec java@temurin-21")
        return [java, "-cp", cp, "io.debezium.connector.db2as400." + entrypoint, *map(str, arguments)]

    def independent_writer(self):
        session = None
        try:
            session = self.connection()
            counter = 1000000
            while not self.stop.is_set() and counter < 1002000:
                counter += 1
                self.execute(session, "unrelated-insert", f"INSERT INTO {self.library}.A VALUES ({counter})")
                self.progress.append({"id": counter, **stamp()})
                self.stop.wait(.5)
            if counter >= 1002000:
                raise RuntimeError("Independent writer budget exhausted")
        except Exception as error:
            self.background_error.append(self.fixture.redact(str(error)))

    def enumeration_writer(self):
        try:
            if self.stop.is_set() or self.churn_stop.is_set():
                return
            if time.monotonic() >= self.deadline:
                raise TimeoutError("Ten-minute fixture workload budget exhausted")
            session = self.connection(True)
            self.churn_job = session[1]
            self.churn_ready.set()
            if not self.churn_go.wait(20):
                raise TimeoutError("Concurrent writer admission timed out")
            for cycle in range(64):
                if self.stop.is_set() or self.churn_stop.is_set():
                    break
                if time.monotonic() >= self.deadline:
                    raise TimeoutError("Ten-minute fixture workload budget exhausted")
                self.churn_sequence += 1
                self.execute(session, "insert", f"INSERT INTO {self.library}.M VALUES ({self.churn_sequence})")
                # Complete admitted work even when stop arrives during the INSERT.
                self.execute(session, "commit" if cycle % 2 == 0 else "rollback")
        except Exception as error:
            self.background_error.append(self.fixture.redact(str(error)))
        finally:
            self.churn_ready.set()  # Startup failures cannot strand the controller.

    def concurrent_census(self, observer):
        self.churn_stop.clear()
        self.churn_go.clear()
        self.churn_ready.clear()
        self.churn_job = None
        activity = {"started": stamp(), "maximum_cycles": 64, "drained": False}
        self.report.setdefault("concurrent_writers", []).append(activity)
        self.churn_thread = threading.Thread(target=self.enumeration_writer, name="owned-enumeration-writer", daemon=True)
        self.churn_thread.start()
        try:
            if not self.churn_ready.wait(20) or self.churn_job is None or self.background_error:
                raise RuntimeError("Concurrent writer failed to become ready")
            activity["job"] = self.churn_job
            sample = census(observer, self.library, before_definitions=self.churn_go.set)
            activity["census"] = sample
        finally:
            self.churn_stop.set()
            self.churn_go.set()
            self.churn_thread.join(timeout=20)
            activity.update(drained=not self.churn_thread.is_alive(), finished=stamp())
            self.persist()
            if self.churn_thread.is_alive():
                raise RuntimeError("Concurrent writer did not drain")
        if self.background_error:
            raise RuntimeError(self.background_error[0])
        return sample, audit_enumeration_overlap(sample, self.ledger, self.churn_job)

    def rotate(self):
        before = self.fixture.receivers("rotation-before.json")
        command = json.loads((self.fixture.directory / "recording-plan.json").read_text())["command"]
        self.fixture.sql("CALL QSYS2.QCMDEXC('" + command + "')", "rotation.log", True)
        prior = self.fixture.owned_objects
        current = self.fixture.inventory("objects-rotated.json")
        identity = lambda row: (row["OBJNAME"], row["OBJTYPE"], row["OBJATTRIBUTE"], row["OBJCREATED"])
        if not {identity(row) for row in prior} <= {identity(row) for row in current}:
            raise ValueError("Owned objects changed unexpectedly")
        added = [row for row in current if identity(row) not in {identity(x) for x in prior}]
        if len(added) != 1 or added[0]["OBJTYPE"] != "*JRNRCV":
            raise ValueError("Unexpected rotation objects")
        self.fixture.owned_objects = current
        after = self.fixture.receivers("rotation-after.json")
        old = next(row for row in before if row["STATUS"] == "ATTACHED")
        new = next(row for row in after if row["STATUS"] == "ATTACHED")
        if len(after) != len(before) + 1 or old["JOURNAL_RECEIVER_NAME"] == new["JOURNAL_RECEIVER_NAME"] or any(
                old["FIXED_LENGTH_DATA_INCLUDES_" + key] != new["FIXED_LENGTH_DATA_INCLUDES_" + key] for key in self.fixture.FLAGS):
            raise ValueError("Rotation did not preserve recording fields")

    def attempt(self, number, scenario, silent, changing, observer):
        name = f"attempt-{number:02d}-{scenario}"
        if scenario == "zero-pending":
            self.execute(changing, "savepoint", "SAVEPOINT S0 ON ROLLBACK RETAIN CURSORS")
        # Retained captures show a changed target can abandon the optimized clear
        # and use row deletes. Start the cycle on M, leaving B eligible for its
        # first optimized clear; the native run must still establish the subtype.
        clear_scenarios = {"clear-commit", "clear-rollback", "optimized-delete", "savepoint"}
        self.insert(changing, "M" if scenario in clear_scenarios else "B")
        if scenario == "zero-pending":
            self.execute(changing, "rollback-to-savepoint", "ROLLBACK TO SAVEPOINT S0")
        if scenario in ["clear-commit", "clear-rollback", "optimized-delete"]:
            sql = f"DELETE FROM {self.library}.B" if scenario == "optimized-delete" else f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS"
            self.execute(changing, "clear", sql)
            self.insert(changing, "B")
        if scenario == "savepoint":
            self.execute(changing, "savepoint", "SAVEPOINT S1 ON ROLLBACK RETAIN CURSORS")
            self.execute(changing, "truncate", f"TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS")
            self.execute(changing, "savepoint", "SAVEPOINT S2 ON ROLLBACK RETAIN CURSORS")
            self.insert(changing, "B")
            self.execute(changing, "rollback-to-savepoint", "ROLLBACK TO SAVEPOINT S1")
        lower = self.capture(name + "-L", "anchor")
        progress_before = len(self.progress)

        def race(definitions):
            if scenario in ["commit", "clear-commit", "optimized-delete"]:
                self.execute(changing, "commit")
            elif scenario in ["rollback", "clear-rollback"]:
                self.execute(changing, "rollback")
            elif scenario == "reuse":
                self.execute(changing, "commit")
                self.insert(changing, "B")
            elif scenario == "new-definition":
                extra = self.connection(True)
                self.insert(extra, "M")
                self.execute(extra, "commit")
            elif scenario == "late-table":
                self.insert(silent, "M")
            elif scenario == "rotation":
                self.rotate()

        overlap = None
        if scenario == "enumeration-concurrent":
            sample, overlap = self.concurrent_census(observer[0])
        else:
            sample = census(observer[0], self.library, race)
        sample_file = self.directory / (name + "-census.json")
        save(sample_file, sample)
        upper = self.capture(name + "-Q", "anchor")
        bracket = self.capture(name + "-bracket", "capture", lower, upper, 4096)
        item = {"scenario": scenario, "lower": str(lower.relative_to(self.directory)), "upper": str(upper.relative_to(self.directory)),
                "census": sample_file.name, "bracket": str(bracket.relative_to(self.directory)),
                "unrelated_writes_during_observation": len(self.progress) - progress_before}
        self.attempts.append(item)
        if overlap is not None:
            item["enumeration_overlap"] = overlap
        self.execute(changing, "rollback")
        self.persist()

    def run(self):
        preflight = self.capture("preflight", "preflight")
        self.report["source_connected"] = True
        require_authority([json.loads(preflight.read_text())["authority"]])
        self.fixture.setup()
        self.deadline = time.monotonic() + 600
        self.capture("native-bindings", "bindings")
        baseline = self.capture("baseline", "anchor")
        silent, changing, observer = self.connection(True), self.connection(True), self.connection(read_only=True)
        self.insert(silent, "U")
        thread = threading.Thread(target=self.independent_writer, name="owned-unrelated-writer", daemon=True)
        self.writer_thread = thread
        thread.start()
        try:
            for repeat in range(self.repetitions):
                for scenario in ["silent", "commit", "rollback", "reuse", "new-definition", "late-table", "savepoint", "zero-pending", "enumeration-concurrent",
                                 "clear-commit", "clear-rollback", "optimized-delete"]:
                    if self.background_error:
                        raise RuntimeError(self.background_error[0])
                    self.attempt(len(self.attempts), scenario, silent, changing, observer)
            self.attempt(len(self.attempts), "rotation", silent, changing, observer)
        finally:
            self.stop.set()
            thread.join(timeout=20)
            if thread.is_alive():
                raise RuntimeError("Independent writer did not drain")
        self.execute(silent, "rollback")
        self.execute(changing, "rollback")
        final = self.capture("final", "anchor")
        self.capture("oracle", "capture", baseline, final, 1024 * 1024)
        self.persist()  # Keep the independent writer ledger even if offline replay fails.
        process = subprocess.run(self.java_command("As400OnlineCensusReplay", self.directory), stdin=subprocess.DEVNULL,
                                 text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=80)
        (self.directory / "replay.log").write_text(self.fixture.redact(process.stdout))
        if process.returncode:
            raise RuntimeError("Offline strict replay rejected; see replay.log")
        replay = json.loads((self.directory / "replay.json").read_text())
        ledger_result = audit_ledger(self.ledger, replay, self.library)
        self.report["writer_ledger_replay"] = ledger_result
        oracle = replay["comparison_captures"]["oracle"]
        for attempt in self.attempts:
            lower = json.loads((self.directory / attempt["lower"]).read_text())
            upper = json.loads((self.directory / attempt["upper"]).read_text())
            sample = json.loads((self.directory / attempt["census"]).read_text())
            bracket = replay["comparison_captures"][Path(attempt["bracket"]).parent.name]
            attempt["audit"] = audit_inventory(sample, bracket, oracle, int(lower["from"]["sequence"]), int(upper["through"]["sequence"]))
            if ledger_result["status"] == "matched":
                attempt["audit"]["oracle_completeness"] = "controlled-workload-matched-native-history-unqualified"
        save(self.directory / "unrelated-progress.json", self.progress)
        consistent = ledger_result["status"] == "matched" and all(
            attempt["audit"]["status"] == "capture-consistent" and attempt["unrelated_writes_during_observation"] > 0 for attempt in self.attempts)
        covered = enumeration_covered(self.attempts)
        self.report["enumeration_overlap_covered"] = covered
        consistent = consistent and covered
        self.report.update(status="captured-unqualified" if consistent and not self.background_error else "rejected", background_errors=self.background_error,
                           qualification="Controlled writer ledger checked; native visibility contract, historical bindings and combined handoff remain unqualified")

    def cleanup(self):
        self.stop.set()
        self.churn_stop.set()
        self.churn_go.set()
        self.report["fixture_created"] = self.fixture.created
        errors = []
        for thread in [self.writer_thread, self.churn_thread]:
            if thread is None:
                continue
            thread.join(timeout=20)
            if thread.is_alive():
                self.report["fixture_cleanup"] = {"status": "blocked", "error": "Writer did not drain; no shared connection close or fixture deletion attempted"}
                self.persist()
                return
        for connection in reversed(self.connections):
            try:
                if not connection.autocommit:
                    connection.rollback()
                connection.close()
            except Exception as error:
                errors.append(self.fixture.redact(str(error)))
        self.report["connection_cleanup_errors"] = errors
        try:
            self.report["fixture_cleanup"] = self.fixture.cleanup()
        except Exception as error:
            self.report["fixture_cleanup"] = {"status": "blocked", "error": self.fixture.redact(str(error))}
        self.persist()


def main():
    if len(sys.argv) not in [3, 4]:
        raise SystemExit("Usage: dcg-online-census-probe.py NEW_DIRECTORY DCNCdddd [repetitions=1]; explicit IBMI_TEST_CONFIG required")
    import pyodbc
    pyodbc.pooling = False  # Avoid client reuse; connection close does not prove native *JOB lock release.
    experiment = Experiment(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) == 4 else 1)
    try:
        experiment.run()
    except BaseException as error:
        experiment.report.update(status="failed", error=experiment.fixture.redact(str(error)))
        raise
    finally:
        experiment.cleanup()
    if experiment.report["fixture_cleanup"]["status"] != "passed" or experiment.report["connection_cleanup_errors"]:
        raise SystemExit("Cleanup incomplete; inspect report.json")
    if experiment.report["status"] != "captured-unqualified":
        raise SystemExit("Census capture consistency checks failed; inspect report.json")
    print(str(experiment.directory / "report.json"))


if __name__ == "__main__":
    main()
