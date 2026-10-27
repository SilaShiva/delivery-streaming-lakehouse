# delivery-streaming-lakehouse

Real-time delivery platform demo — the data stack a Swiggy/Uber-scale company actually runs:
Kafka → Flink → Iceberg lakehouse, with schema governance, dbt marts, and Dagster orchestration.
Runs **natively on Windows** — downloads, scripts, and a laptop. No containers, no cloud, no bill.

> Part of a data-engineering portfolio. Companion projects: streaming-insights-platform
> (Redis Streams + dbt + Dagster), delivery-eta-quantile, review-intelligence-nlp,
> advertiser-growth-prospecting.

## Why this project exists

Streaming systems look simple in tutorials and fall apart in interviews. This repo is built
around the questions senior DE interviews actually ask:

| Interview question | Where this repo answers it |
|---|---|
| "How do you handle late-arriving events?" | Flink event-time windows + watermarks; late events land in the correct window. `docs/decisions.md` shows the cost of a bad watermark choice |
| "Exactly-once vs at-least-once?" | Kafka→Iceberg exactly-once sink via checkpointing; rider-location pings deliberately at-least-once (cheaper, idempotent downstream) |
| "How do you evolve a schema without breaking consumers?" | Avro + Schema Registry, `backward` compatibility; a schema-evolution commit shows old consumers surviving a field addition |
| "50k tiny files, queries got slow — fix it" | Iceberg compaction job with before/after file counts |
| "A bug corrupted 3 days of data — walk me through it" | Kafka replay backfill; reprocess a window from the topic into Iceberg |
| "How do you know the pipeline is broken before the CEO is?" | Freshness + row-count + null-rate checks, alerting in the Dagster UI |

See `docs/decisions.md` for the trade-off log — judgment over tools.

## Architecture

```
producer (Python, replays synthetic delivery events with realistic skew + bursts)
   │  Avro, schema-validated
   ▼
Schema Registry ──► Kafka (KRaft): orders │ rider-locations │ restaurant-updates
   │
   ▼
Flink (local standalone cluster, Windows tarball)
   ├─ real-time tier : 10-min event-time windows, watermarks, late-data handling
   │                   → Flink SQL job → Kafka topic "live-metrics"
   └─ batch tier     : exactly-once sink → Iceberg tables on the local filesystem
   │
   ▼
DuckDB over Iceberg → dbt (staging → marts) → analytics tables
   │
Dagster : hourly compaction · nightly backfill · data-quality gates
```

## Stack

| Layer | Tech | Why |
|---|---|---|
| Transport | Kafka 3.8 (KRaft) | industry default; KRaft = no ZooKeeper |
| Contracts | Avro + Confluent Schema Registry | schema evolution with compatibility checks |
| Processing | Flink 1.19 (Windows standalone) | event-time semantics, exactly-once checkpoints |
| Storage | Iceberg 0.7 on the local FS | ACID lakehouse tables, hidden partitioning, time travel — S3 is a detail, not a requirement |
| Transform | dbt + DuckDB | same modeling pattern as production stacks |
| Orchestration | Dagster | asset-based, typed, great local UI |

## Repo layout

```
producer/           synthetic event generator (orders, riders, restaurants)
  schemas/          Avro schemas + registration script
flink/sql/          Flink SQL job: real-time windows over order events
flink/pyflink_walkthrough/  DataStream-API version of the window job (interview walkthrough)
flink/iceberg_sink/ batch tier: Kafka → Iceberg, exactly-once
dbt/                staging + marts models over Iceberg via DuckDB
orchestration/      Dagster assets: compaction, backfill, data-quality gates
scripts/            download + start/stop everything on Windows
docs/               decisions.md (trade-off log), runbook.md, interview_notes.md
```

## Quickstart (Windows, no containers)

```powershell
# 0. one-time: download Kafka, Flink, Schema Registry tarballs (needs Java 17)
.\scripts\download_runtimes.ps1

# 1. start everything (kafka, schema-registry, flink)
.\scripts\start_stack.ps1

# 2. create topics + register Avro schemas
python producer\setup_topics.py

# 3. start streaming events
python producer\generate.py --rate 50          # 50 events/sec

# 4. submit the Flink SQL window job
.\scripts\submit_flink_jobs.ps1

# 5. build the lakehouse models
cd dbt && dbt build

# 6. orchestrate + observe
dagster dev -f orchestration\definitions.py     # http://localhost:3000
```

## Status

Work in progress — see commit history for build order.
