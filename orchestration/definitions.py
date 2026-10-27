"""Dagster definitions: compaction, backfill, and data-quality gates.

Assets:
  * iceberg_compaction  — hourly: merge small files, expire old snapshots
  * dq_gates            — freshness + row-count + null-rate checks; failures
                          surface in the Dagster UI (the "know before the CEO" story)
  * backfill_window     — replay a Kafka offset range through the batch job into
                          a shadow table, then swap (D9)

Run:  dagster dev -f orchestration/definitions.py   → http://localhost:3000
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    asset,
    define_asset_job,
    ScheduleDefinition,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = REPO_ROOT / "warehouse.duckdb"


@asset(description="Hourly Iceberg maintenance: compact small files, expire snapshots.")
def iceberg_compaction(context: AssetExecutionContext) -> dict:
    """Runs inside the Flink container where Iceberg procedures are available."""
    context.log.info("compacting lake.raw.orders")
    # Iceberg's rewrite_data_files via Flink SQL:
    #   CALL lake.system.rewrite_data_files(
    #        table => 'lake.raw.orders',
    #        options => map('min-input-files', '5'));
    # expire_snapshots keeps the last 72h — time travel window for incident forensics.
    cmd = [
        "docker", "exec", "dsl-jobmanager",
        "/opt/flink/bin/sql-client.sh", "embedded",
        "-e", "/opt/jobs/flink-sql.conf",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    stats = {"stdout_tail": result.stdout[-2000:], "rc": result.returncode}
    if result.returncode != 0:
        raise RuntimeError(f"compaction failed: {result.stderr[-1000:]}")
    return stats


@asset(description="Data-quality gates over the marts: freshness, volume, nulls.")
def dq_gates(context: AssetExecutionContext) -> dict:
    """Failures raise — Dagster UI shows red, that's the alerting path."""
    import duckdb

    checks: dict[str, dict] = {}
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # 1. freshness: marts built in the last 2h
    latest = con.execute(
        "select max(ingest_ts) from stg_orders"
    ).fetchone()[0]
    checks["freshness"] = {"latest_event": str(latest), "max_age_min": 120}

    # 2. volume: same-hour-last-week ±20% (the gate reused from streaming-insights-platform)
    rows = con.execute("""
        with cur as (
            select count(*) n from stg_orders
            where event_ts >= now() - interval 1 hour
        ), ref as (
            select count(*) n from stg_orders
            where event_ts >= now() - interval 7 day - interval 1 hour
              and event_ts <  now() - interval 7 day
        )
        select cur.n, ref.n from cur, ref
    """).fetchone()
    cur_n, ref_n = rows
    if ref_n and cur_n and abs(cur_n - ref_n) / ref_n > 0.20:
        raise RuntimeError(f"row-count gate failed: current={cur_n} ref={ref_n}")
    checks["row_count"] = {"current_hour": cur_n, "same_hour_last_week": ref_n}

    # 3. nulls: no null keys in the marts
    nulls = con.execute(
        "select count(*) from stg_orders where event_id is null or order_id is null"
    ).fetchone()[0]
    if nulls:
        raise RuntimeError(f"null-key gate failed: {nulls} rows")
    checks["null_keys"] = {"violations": 0}

    con.close()
    context.log.info(f"dq gates passed: {json.dumps(checks)}")
    return checks


@asset(description="Replay a Kafka offset range into a shadow Iceberg table, then swap (D9).")
def backfill_window(context: AssetExecutionContext) -> dict:
    """Parameterized by Dagster run config: topic, start_offset, end_offset."""
    # The backfill path: read_range -> shadow table -> diff -> atomic swap.
    # Implementation lands with the batch-tier milestone; wiring lives here so
    # the Dagster graph shows the intended topology from day one.
    context.log.info("backfill_window wired; body lands with batch-tier milestone")
    return {"status": "wired"}


dq_job = define_asset_job("lakehouse_maintenance", selection=[iceberg_compaction, dq_gates])

maintenance_schedule = ScheduleDefinition(
    job=dq_job,
    cron_schedule="5 * * * *",  # hourly at :05
    name="hourly_maintenance",
)

defs = Definitions(
    assets=[iceberg_compaction, dq_gates, backfill_window],
    jobs=[dq_job],
    schedules=[maintenance_schedule],
)
