#!/usr/bin/env python3
"""Bounded local KafkaSchemaHistory experiment; never connects to IBM i."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "apache/kafka:4.3.1"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--java", required=True, type=Path)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    name = "dcg-binding-probe-" + uuid.uuid4().hex[:12]
    report = {"name": name, "image": IMAGE, "scope": "local Kafka history and native fixture replay only", "phases": []}

    def save():
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    def command(*command_args, timeout=30):
        return subprocess.check_output(command_args, cwd=ROOT, text=True, stderr=subprocess.STDOUT, timeout=timeout)

    endpoint = os.environ.get("DOCKER_HOST") if not os.environ.get("DOCKER_CONTEXT") else None
    if not endpoint:
        endpoint = json.loads(command("docker", "context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"))
    if not endpoint.startswith("unix://"):
        raise RuntimeError("Local Unix-socket Docker daemon required")
    report["docker_endpoint"] = endpoint
    report["image_id"] = command("docker", "image", "inspect", IMAGE, "--format", "{{.Id}}").strip()
    dependencies = (ROOT / "debezium-connector-ibmi/target/binding-history-classpath.txt").read_text().strip()
    local = [ROOT / "debezium-connector-ibmi/target/test-classes", ROOT / "debezium-connector-ibmi/target/classes",
             ROOT / "journal-parsing/target/classes", ROOT / "jt400-override-ccsid/target/classes"]
    classpath = ":".join(map(str, local)) + ":" + dependencies
    hashes = {}
    for part in classpath.split(":"):
        path = Path(part)
        for entry in path.rglob("*.class") if path.is_dir() else [path]:
            if entry.is_file():
                hashes[str(entry)] = hashlib.sha256(entry.read_bytes()).hexdigest()
    (evidence / "runtime-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
    (evidence / "classpath.txt").write_text(classpath + "\n")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    bootstrap = "127.0.0.1:" + str(port)
    env = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
           "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093", "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://" + bootstrap,
           "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
           "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER", "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
           "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
           "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
           "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
           "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m"}
    container = None
    try:
        docker_args = ["docker", "run", "-d", "--name", name, "--label", "dcg.binding.probe=" + name,
                       "--memory", "1g", "-p", f"127.0.0.1:{port}:9092"]
        for key, value in env.items():
            docker_args += ["-e", key + "=" + value]
        container = command(*docker_args, IMAGE).strip()
        report.update(container=container, bootstrap=bootstrap, configuration=env)
        save()
        deadline = time.monotonic() + 45
        while "Kafka Server started" not in command("docker", "logs", container):
            if time.monotonic() >= deadline:
                raise RuntimeError("Local broker did not start")
            time.sleep(.5)
        for phase in ["initialize", "open-truncate", "commit", "open-delete", "rollback", "again", "missing-offset", "missing-history",
                      "invalid-null", "invalid-empty", "invalid-source", "invalid-position", "invalid-json", "conflicting-history", "candidate-race"]:
            started = time.monotonic()
            with (evidence / (phase + ".log")).open("w") as log:
                result = subprocess.run([str(args.java), "-cp", classpath, "io.debezium.connector.db2as400.As400BindingHistoryProbe",
                                         bootstrap, name, str(evidence), phase], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=60)
            report["phases"].append({"name": phase, "exit_code": result.returncode, "seconds": round(time.monotonic() - started, 3)})
            save()
            if result.returncode:
                raise RuntimeError("Probe failed in " + phase + "; see saved log")
        report["status"] = "passed"
    except Exception as error:
        report.update(status="failed", error=str(error))
        raise
    finally:
        if container:
            (evidence / "broker.log").write_text(command("docker", "logs", container))
            owned = json.loads(command("docker", "inspect", container))[0]
            if owned["Config"]["Labels"].get("dcg.binding.probe") != name:
                raise RuntimeError("Cleanup ownership mismatch")
            command("docker", "rm", "-f", container)
            remaining = command("docker", "ps", "-aq", "--filter", "label=dcg.binding.probe=" + name).strip()
            report["cleanup"] = {"status": "passed" if not remaining else "failed", "owned_container_absent": not remaining}
        save()
    print("Local binding-history experiment and cleanup passed; evidence:", evidence)


if __name__ == "__main__":
    main()
