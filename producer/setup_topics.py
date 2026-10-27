"""Create Kafka topics and register Avro schemas.

Run once after `docker compose up -d`:
    python producer/setup_topics.py
"""

from __future__ import annotations

import json
import pathlib
import sys
import time

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"

TOPICS = {
    "orders": {"num_partitions": 3, "replication_factor": 1},
    "rider-locations": {"num_partitions": 3, "replication_factor": 1},
    "restaurant-updates": {"num_partitions": 1, "replication_factor": 1},
    "live-metrics": {"num_partitions": 3, "replication_factor": 1},
}

SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"
SUBJECTS = {
    "orders-value": "order_events.avsc",
    "rider-locations-value": "rider_location.avsc",
}


def create_topics() -> None:
    """Create topics via the Kafka admin HTTP-less route: kafka-topics.sh in the broker container.

    Kept as a subprocess call so the host machine needs no kafka-python admin API
    (which historically lags broker versions)."""
    import subprocess

    for topic, cfg in TOPICS.items():
        subprocess.run(
            [
                "docker", "exec", "dsl-kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--create", "--if-not-exists",
                "--topic", topic,
                "--partitions", str(cfg["num_partitions"]),
                "--replication-factor", str(cfg["replication_factor"]),
                "--config", "retention.ms=604800000",  # 7 days — replay window for backfills (D9)
            ],
            check=True,
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
