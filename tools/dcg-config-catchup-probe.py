#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Controlled local comparisons; the original partition scenario is unchanged."""
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("ownership_probe", ROOT / "tools/dcg-ownership-probe.py")
ownership = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ownership)


class CatchupExperiment(ownership.OwnershipExperiment):
    def __init__(self, directory, variant):
        super().__init__(directory)
        self.variant = variant
        self.report["diagnostic_variant"] = variant
        self.report["selected_scenarios"] = ["partition"]
        self.report["worker_overrides"] = ({"request.timeout.ms": "5000", "default.api.timeout.ms": "10000"}
                                           if variant in ["fast-client", "aligned"] else {})
        if variant == "aligned":
            self.report["worker_overrides"].update({"worker.sync.timeout.ms": "20000", "rebalance.timeout.ms": "30000"})
            self.report["administrative_stop_http_timeout_seconds"] = 60
        self.save()

    def stage(self):
        super().stage()
        self.report["harness_sha256"]["tools/dcg-config-catchup-probe.py"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        properties = self.directory / (name + ".properties")
        if properties.exists() and self.report["worker_overrides"]:
            # Kafka 4.3.1 ConnectDistributed constructs SharedTopicAdmin directly
            # from config.originals(), not the admin.* connector-client overrides.
            # These are explicit LOCAL worker variants, not a production recipe.
            with properties.open("a") as stream:
                for key, value in self.report["worker_overrides"].items():
                    stream.write(key + "=" + value + "\n")
        return super().container(name, args)

    def command(self, *args, **kwargs):
        if args[:3] == ("docker", "network", "connect"):
            self.reconnected_at = datetime.now(timezone.utc).isoformat()
        if self.variant == "aligned" and args[-4:-1] == ("rest-method", "PUT", "/connectors/partition/stop"):
            # Match only the idempotent administrative stop transport. Preserve
            # the parent's bounded explicit-response retry policy and all task,
            # publication, fencing and recovery deadlines/assertions.
            args = (*args, "60")
            kwargs["timeout"] = 70
        return super().command(*args, **kwargs)

    def stop(self, worker):
        context = getattr(self, "failure_context", None)
        if self.variant in ["settled", "rejoined"] and worker.endswith("-partition-w2") and context:
            started = time.monotonic()
            # The comparison deliberately delays the SECOND fault. The original
            # network partition, stale-send assertions and subsequent 100s task
            # reassignment bound are unchanged. This is not the default scenario.
            def caught_up():
                log = self.command("docker", "logs", "--since", self.reconnected_at, "--tail", "1000", context["worker"])
                marker = ("Finished read to end log for topic configs-partition" if self.variant == "settled"
                          else "Successfully joined group with generation")
                if marker in log:
                    (self.directory / "before-second-fault-catchup.log").write_text(log)
                    return True
                time.sleep(1)
                return False
            ownership.base.wait_for("old worker meets " + self.variant + " condition before second fault", caught_up, 240)
            self.report["second_fault_wait_seconds"] = round(time.monotonic() - started, 3)
            self.save()
        return super().stop(worker)


def main():
    if len(sys.argv) != 3 or sys.argv[2] not in ["fast-client", "settled", "aligned", "rejoined"]:
        raise SystemExit("Usage: dcg-config-catchup-probe.py NEW_EVIDENCE_DIRECTORY fast-client|settled|aligned|rejoined (build ownership probe first)")
    experiment = CatchupExperiment(Path(sys.argv[1]), sys.argv[2])
    try:
        experiment.stage()
        experiment.start_broker()
        experiment.fencing(False)
        experiment.check("comparison uses no custom publication authority",
                         not list(experiment.control.glob("*.registry*")) and not list(experiment.control.glob("*.authorized-worker*")), "control/")
        experiment.check("all fault gates completed before deadlines", not list(experiment.control.glob("*.hook-timeout.json")), "control/")
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
