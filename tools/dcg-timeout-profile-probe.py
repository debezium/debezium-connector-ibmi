#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""One bounded local timing case using the fixed candidate Connect profile."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("catchup_probe", ROOT / "tools/dcg-config-catchup-probe.py")
catchup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catchup)


class TimingExperiment(catchup.CatchupExperiment):
    def __init__(self, directory, isolation_hold, second_fault_delay):
        self.clock_start = time.monotonic()
        self.isolation_hold = isolation_hold
        self.second_fault_delay = second_fault_delay
        super().__init__(directory, "aligned")
        self.report["timing_case"] = {
            "isolation_hold_seconds": isolation_hold,
            "second_fault_delay_seconds": second_fault_delay,
            "events": [],
        }
        self.save()

    def mark(self, event, **fields):
        self.report["timing_case"]["events"].append({
            "event": event, "utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(time.monotonic() - self.clock_start, 6), **fields,
        })
        self.save()

    def stage(self):
        super().stage()
        self.report["harness_sha256"]["tools/dcg-timeout-profile-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def command(self, *args, **kwargs):
        result = super().command(*args, **kwargs)
        if args[:3] == ("docker", "network", "disconnect"):
            self.disconnected_at = time.monotonic()
            self.mark("network-disconnected")
        elif args[:3] == ("docker", "network", "connect"):
            self.mark("network-reconnected", isolation_seconds=round(time.monotonic() - self.disconnected_at, 6))
        return result

    def signal(self, generation, suffix, value=""):
        if suffix == "send-release":
            # Preserve the original blocked-send sequence. The extra wait is
            # while disconnected, BEFORE releasing the old send and reconnecting.
            self.mark("isolation-hold-start", requested_seconds=self.isolation_hold)
            started = time.monotonic()
            time.sleep(self.isolation_hold)
            self.mark("isolation-hold-end", actual_seconds=round(time.monotonic() - started, 6))
        return super().signal(generation, suffix, value)

    def stop(self, worker):
        if worker.endswith("-partition-w2") and getattr(self, "failure_context", None):
            # Begin only after the unchanged old-send/output/checkpoint checks.
            # No readiness gate or new authorization is introduced.
            self.mark("second-fault-delay-start", requested_seconds=self.second_fault_delay)
            started = time.monotonic()
            time.sleep(self.second_fault_delay)
            self.mark("second-fault-delay-end", actual_seconds=round(time.monotonic() - started, 6))
            result = super().stop(worker)
            self.mark("replacement-killed")
            return result
        return super().stop(worker)

    def latest_generation(self, scenario, role, excluded):
        started = time.monotonic()
        try:
            generation = super().latest_generation(scenario, role, excluded)
            self.mark("new-task-prepared", generation=generation)
            return generation
        finally:
            self.report["timing_case"]["reassignment_wait_seconds"] = round(time.monotonic() - started, 6)
            self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new evidence directory; build ownership probe first")
    parser.add_argument("--isolation-hold-seconds", type=int, choices=[0, 12], required=True)
    parser.add_argument("--second-fault-delay-seconds", type=int, choices=[0, 5, 25], required=True)
    args = parser.parse_args()
    experiment = TimingExperiment(args.directory, args.isolation_hold_seconds, args.second_fault_delay_seconds)
    try:
        experiment.stage()
        experiment.start_broker()
        # All publication, stale-send, F/W, replay, source metadata, projection,
        # reassignment and normal-stop assertions execute in the original method.
        experiment.fencing(False)
        experiment.check("comparison uses no custom publication authority",
                         not list(experiment.control.glob("*.registry*")) and not list(experiment.control.glob("*.authorized-worker*")), "control/")
        experiment.check("all fault gates completed before deadlines", not list(experiment.control.glob("*.hook-timeout.json")), "control/")
        events = {event["event"]: event for event in experiment.report["timing_case"]["events"]}
        experiment.check("requested timing interventions ran in order", list(events) == [
            "network-disconnected", "isolation-hold-start", "isolation-hold-end", "network-reconnected",
            "second-fault-delay-start", "second-fault-delay-end", "replacement-killed", "new-task-prepared"] and
            events["isolation-hold-end"]["actual_seconds"] >= args.isolation_hold_seconds and
            events["second-fault-delay-end"]["actual_seconds"] >= args.second_fault_delay_seconds,
            "report.json:timing_case")
        experiment.report["status"] = "passed"
    except BaseException as error:
        experiment.report["status"] = "failed"
        experiment.report["error"] = str(error)
        experiment.save()
        try:
            experiment.capture_failure()
        except Exception as diagnostic_error:
            experiment.report["failure_evidence_error"] = str(diagnostic_error)
        raise
    finally:
        experiment.cleanup()
        experiment.save()
    if experiment.report["cleanup"]["status"] != "passed":
        raise SystemExit("Cleanup failed; inspect report.json")
    print(str(experiment.directory / "report.json"), flush=True)


if __name__ == "__main__":
    main()
