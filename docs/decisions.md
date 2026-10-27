# Decision log

The trade-offs behind the stack. Tools are learnable; judgment is the job.

---

## D1 — Kafka over Redis Streams (again)

streaming-insights-platform used Redis Streams — right choice for its size. This project
deliberately outgrew it: consumer groups with rebalancing, Kafka transactions, long retention
for replay, and an ecosystem (Schema Registry, Flink connector) that Redis Streams doesn't have.
Chose KRaft over ZooKeeper — one less moving part, and it's the direction upstream has taken.

## D2 — PyFlink despite the pain

Flink's first-class API is Java. PyFlink is slower, the docs are thinner, and the Table API
bridges a lot of the gap. Chose it anyway: the rest of the stack (producer, dbt, Dagster) is
Python, and a two-language repo doubles maintenance. If a job ever needs the DataStream API's
finer control, the escape hatch is a small Java fat-jar for that operator only.

## D3 — Event-time, not processing-time

Delivery events arrive out of order (rider app offline, network retries, clock skew).
Processing-time windows would silently attribute a 19:58 order to the 20:00 bucket —
wrong for any business metric. Event-time + watermarks is the only correct option; the
only decision left is *watermark delay*, see D4.

## D4 — Watermark delay: 5 minutes

Too small → windows close before stragglers arrive, late data dropped.
Too large → dashboards lag by the delay.
Measured on generated traffic: p99 out-of-orderness ≈ 3.2 min under simulated offline-rider
bursts, so 5 min covers p99+ with margin. Late-beyond-watermark events go to a side output
(audited, not silently dropped) — re-counted manually, not re-widened windows.

## D5 — Exactly-once for orders, at-least-once for rider pings

Exactly-once costs checkpoint latency and throughput. Orders are money — duplicates corrupt
GMV — so orders_to_iceberg runs exactly-once via Flink checkpoints. Rider GPS pings are
~1/sec/rider, naturally idempotent (latest-position-wins downstream), and at 10x the volume
exactly-once would burn slots for zero correctness gain. At-least-once, deduped by key.
This is the "exactly-once vs at-least-once" answer with numbers, not slogans.

## D6 — Iceberg over Delta Lake / Hudi

All three give ACID on object storage. Iceberg: engine-agnostic (Flink writes, DuckDB/Trino
read), hidden partitioning, schema evolution that actually works, and the healthiest OSS
momentum. Hudi's ingest-heavy tilt fits less; Delta's best integrations assume Databricks.

## D7 — MinIO instead of a real cloud

The S3 API is what matters — MinIO speaks it, runs locally, and makes the whole stack
reproducible with `docker compose up`. Porting to AWS later is a credentials + endpoint
change, not a code change. Cost of honesty: no S3 Select/Inventory features; not used here.

## D8 — DuckDB (not Trino) as the query engine

Trino is the "real" answer for a clustered warehouse; DuckDB is a single binary that reads
Iceberg directly and is enough for portfolio-scale data. Same dbt models would point at
Trino by changing the profile. Documented so the simplification is a decision, not an oversight.

## D9 — Backfill via Kafka replay, not table rewrite

Iceberg supports in-place rewrites, but the durable source of truth is the Kafka topic
(7-day retention). A corrupted downstream window is repaired by replaying that offset range
through the same job into a shadow table, diffing, then swapping. Idempotent,
auditable, and it exercises the exact path production would use.

## D10 — orders-json mirror topic alongside the Avro topic

The schema-governed topic (`orders`, Avro + wire format) stays the source of truth — it's
what Schema Registry compatibility checks protect. But Flink SQL's JSON path is far less
brittle than wiring a Confluent-Avro deserializer into every job, so the producer writes a
`orders-json` mirror that Flink jobs consume. Cost: double produce traffic (irrelevant at
portfolio scale). Benefit: the Avro/Registry story stays clean and isolated to the producer
and schema-evolution demo, where it belongs. One topic for contracts, one for convenience —
in production you'd pick one and use Avro end to end.

## D11 — Native Windows, no containers (and what it cost)

The reference architecture for this stack is Linux + containers. This laptop runs neither
well, so the stack runs natively on Windows:

* Kafka 3.8 ships `bin\windows\*.bat` — first-class.
* Flink 1.19 ships `bin\*.bat` for cluster start/stop — first-class.
* **Flink's SQL client is Linux-only upstream** (no .bat). Workaround:
  `flink/iceberg_sink/submit_sql_windows.py` drives the same engine through PyFlink's
  TableEnvironment — identical SQL semantics, different submission shell.
* **PyFlink's DataStream API is unsupported on Windows** — the windowing job that runs
  here is the SQL one; the DataStream walkthrough (`flink/pyflink_walkthrough/`) exists
  for line-by-line interview discussion and runs on Linux/WSL when available.
* Schema Registry: Confluent Community zip includes `bin\windows\schema-registry-start.bat`.

Honest engineering note: this is a portability constraint turned into a design decision.
The table formats, wire protocols, and SQL semantics are identical to a Linux deployment;
what changed is only the launcher scripts. That said, a reviewer should read D11 before
assuming the jobs were battle-tested under production load — they were not; they're
portfolio-scale demos of the semantics.
