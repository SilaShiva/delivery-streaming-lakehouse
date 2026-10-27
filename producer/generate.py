"""Synthetic delivery-event generator.

Streams Avro-encoded events to Kafka with realistic messiness, because clean
synthetic data teaches nothing:

  * diurnal order curve (lunch + dinner peaks)
  * event bursts (rain = surge = order spikes)
  * out-of-order events (rider offline then reconnects — late data for watermarks)
  * duplicate sends (producer retries — why dedup keys matter)
  * occasional schema-valid-but-weird values (Rs 0 cancelled orders)

Usage:
    python producer/generate.py --rate 50            # ~50 orders/sec
    python producer/generate.py --rate 50 --chaos    # heavy late data + dupes
"""

from __future__ import annotations

import argparse
import io
import json
import random
import struct
import time
import uuid
from datetime import datetime, timedelta, timezone

import fastavro
import requests
from confluent_kafka import Producer

SCHEMA_REGISTRY = "http://localhost:8081"
BOOTSTRAP = "localhost:9092"


def fetch_schema(subject: str) -> tuple[int, str]:
    """Latest registered schema for a subject -> (id, schema_json_str)."""
    r = requests.get(f"{SCHEMA_REGISTRY}/subjects/{subject}/versions/latest", timeout=5)
    r.raise_for_status()
    body = r.json()
    return body["id"], body["schema"]


def avro_encode(schema_id: int, schema: dict, record: dict) -> bytes:
    """Confluent wire format: magic byte 0 + 4-byte schema id + avro binary."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def diurnal_multiplier(now: datetime) -> float:
    """Lunch (13:00) and dinner (20:00) peaks, IST-shaped."""
    hour = now.hour
    return 0.4 + 0.6 * (
        (2.718 ** -((hour - 13) ** 2) / 6) + 1.3 * (2.718 ** -((hour - 20) ** 2) / 6)
    )


def make_order_event(now: datetime, chaos: bool) -> dict:
    event_ts = now
    if chaos and random.random() < 0.08:
        # rider was offline; event shows up minutes late
        event_ts = now - timedelta(minutes=random.uniform(2, 12))
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"o-{random.randint(1, 100000)}",
        "restaurant_id": f"r-{random.randint(1, 3000)}",
        "customer_id": f"c-{random.randint(1, 60000)}",
        "event_type": random.choices(
            ["created", "confirmed", "picked_up", "delivered", "cancelled"],
            weights=[30, 10, 10, 45, 5],
        )[0],
        "order_value": round(max(0.0, random.gauss(420, 180)), 2),
        "event_ts": int(event_ts.timestamp() * 1000),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=50, help="base orders/sec")
    ap.add_argument("--chaos", action="store_true", help="late events + duplicates")
    args = ap.parse_args()

    schema_id, schema_json = fetch_schema("orders-value")
    schema = json.loads(schema_json)
    print(f"using orders-value schema id={schema_id}")

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "1"})
    print("producing… Ctrl+C to stop")

    total = 0
    while True:
        now = datetime.now(timezone.utc)
        rate = args.rate * diurnal_multiplier(now)
        n_events = max(1, round(rate))
        for _ in range(n_events):
            ev = make_order_event(now, args.chaos)
            payload = avro_encode(schema_id, schema, ev)
            key = ev["order_id"].encode()
            producer.produce("orders", key=key, value=payload)
            if args.chaos and random.random() < 0.02:
                producer.produce("orders", key=key, value=payload)  # dupe: retry lookalike
        producer.poll(0)
        total += n_events
        if total % 500 < n_events:
            print(f"  sent ~{total} events")
        time.sleep(1.0)


if __name__ == "__main__":
    main()
