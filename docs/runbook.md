# Runbook

Day-to-day operation, native Windows. First-time setup: see README quickstart.

## Bring the stack up / down

```powershell
.\scripts\download_runtimes.ps1     # one-time: fetch Kafka/Flink/Schema Registry tarballs
.\scripts\start_stack.ps1           # start kafka (9092), schema-registry (8081), flink (8082)
.\scripts\stop_stack.ps1            # stop everything
.\scripts\stop_stack.ps1 -wipe      # stop AND delete all state (kafka logs, iceberg, duckdb)
```

Ports:

| Service | URL |
|---|---|
| Flink UI | http://localhost:8082 |
| Schema Registry | http://localhost:8081 |
| Dagster | http://localhost:3000 |
| Kafka | localhost:9092 |

Log files land in `.state\logs\` when start_stack.ps1 launches processes.

## Produce / inspect events

```powershell
python producer\generate.py --rate 50            # steady traffic
python producer\generate.py --rate 50 --chaos    # + late events, duplicates

# watch the orders topic
& "$env:KAFKA_HOME\bin\windows\kafka-console-consumer.bat" `
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5

# consumer lag (is Flink keeping up?)
& "$env:KAFKA_HOME\bin\windows\kafka-consumer-groups.bat" `
  --bootstrap-server localhost:9092 --describe --group flink-to-iceberg
```

## Flink jobs

```powershell
# SQL jobs (windows job + iceberg sink) via the TableEnvironment gateway:
python flink\iceberg_sink\submit_sql_windows.py flink\sql\realtime_metrics.sql
python flink\iceberg_sink\submit_sql_windows.py flink\iceberg_sink\orders_to_iceberg.sql

# cancel: Flink UI (localhost:8082) → job → Cancel
```

Checkpoints are the exactly-once spine (D5): if a job restarts, verify the
checkpoint history in the UI shows completed checkpoints before trusting counts.

## Iceberg tables

Warehouse root: `lakehouse\` in the repo (Hadoop catalog, `file:///` URIs).
Inspect snapshots with DuckDB's iceberg extension or via Flink SQL
(`SELECT * FROM lake.raw.orders VERSION AS OF <snapshot-id>`).

## Incident: marts look wrong

1. `dq_gates` in Dagster — which gate failed? freshness/volume/nulls localizes it.
2. Consumer lag check above — is the pipeline just behind?
3. Kafka replay is the repair path (D9): the topic holds 7 days. See
   `orchestration/definitions.py::backfill_window`.
4. Iceberg time travel for forensics: snapshot queries above.

## Incident: a service won't start

* Kafka on first run formats its KRaft storage into `.state\kafka-logs` —
  if that dir is half-written (crashed first run), `stop_stack.ps1 -wipe` and retry.
* Flink UI down: check `runtimes\flink*\log\` next to the tarball extraction.
* Schema Registry down: it needs Kafka up first — start order matters
  (start_stack.ps1 handles it; manual starts must too).
