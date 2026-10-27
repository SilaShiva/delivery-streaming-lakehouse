"""Create Kafka topics and register Avro schemas.

Run once after scripts\\start_stack.ps1:
    python producer/setup_topics.py

Topic creation shells out to kafka-topics.bat from the local Kafka install
(native Windows stack — no containers), so runtimes.env.ps1 must exist
(scripts\\download_runtimes.ps1 creates it).
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import time

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

TOPICS = {
    "orders": {"num_partitions": 3},
    "orders-json": {"num_partitions": 3},
    "rider-locations": {"num_partitions": 3},
    "restaurant-updates": {"num_partitions": 1},
    "live-metrics": {"num_partitions": 3},
    "late-orders": {"num_partitions": 1},
}

SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"
SUBJECTS = {
    "orders-value": "order_events.avsc",
    "rider-locations-value": "rider_location.avsc",
}

RETENTION_MS = "604800000"  # 7 days — replay window for backfills (D9)


def kafka_topics_bat() -> pathlib.Path:
    env_file = REPO_ROOT / "scripts" / "runtimes.env.ps1"
    if not env_file.exists():
        sys.exit("scripts/runtimes.env.ps1 not found — run scripts/download_runtimes.ps1 first")
    env = {}
    for line in env_file.read_text().splitlines():
        line = line.strip().lstrip("$")
        if "=" in line:
            k, v = line.split("=", 1)
            env[k] = v.strip('"')
    bat = pathlib.Path(env["KAFKA_HOME"]) / "bin" / "windows" / "kafka-topics.bat"
    if not bat.exists():
        sys.exit(f"kafka-topics.bat not found at {bat}")
    return bat


def create_topics() -> None:
    bat = kafka_topics_bat()
    for topic, cfg in TOPICS.items():
        subprocess.run(
            [
                str(bat),
                "--bootstrap-server", KAFKA_BOOTSTRAP,
                "--create", "--if-not-exists",
                "--topic", topic,
                "--partitions", str(cfg["num_partitions"]),
                "--replication-factor", "1",
                "--config", f"retention.ms={RETENTION_MS}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"topic ready: {topic}")


def wait_for_registry(timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if requests.get(f"{SCHEMA_REGISTRY}/subjects", timeout=2).status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    sys.exit(f"schema registry not reachable at {SCHEMA_REGISTRY} after {timeout_s}s")


def register_schemas() -> None:
    for subject, filename in SUBJECTS.items():
        schema = json.loads((SCHEMA_DIR / filename).read_text())
        resp = requests.post(
            f"{SCHEMA_REGISTRY}/subjects/{subject}/versions",
            json={"schemaType": "AVRO", "schema": json.dumps(schema)},
            timeout=10,
        )
        if resp.status_code in (200, 201):
            print(f"schema registered: {subject} v{resp.json()['id']}")
        elif resp.status_code == 409:
            print(f"schema unchanged: {subject} (already registered, backward-compatible)")
        else:
            sys.exit(f"schema registration failed for {subject}: {resp.status_code} {resp.text}")


if __name__ == "__main__":
    create_topics()
    wait_for_registry()
    register_schemas()
    print("setup complete")
