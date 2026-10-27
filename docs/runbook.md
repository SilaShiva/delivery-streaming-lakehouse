# Runbook

Day-to-day operation. First-time setup: see README quickstart.

## Bring the stack up / down

```bash
docker compose up -d          # start everything
docker compose ps             # verify: kafka, schema-registry, jobmanager, taskmanager, minio, postgres
docker compose down           # stop (keeps volumes)
docker compose down -v        # stop and wipe all state
```

Ports:

| Service | URL |
|---|---|
| Flink UI | http://localhost:8082 |
| Schema Registry | http://localhost:8081 |
| MinIO console | http://localhost:9001 (minioadmin/minioadmin) |
| Dagster | http://localhost:3000 |
| Kafka | localhost:9092 |

## Produce / inspect events

```bash
python producer/generate.py --rate 50            # steady traffic
python producer/generate.py --rate 50 --chaos    # + late events, duplicates

# watch the orders topic
docker exec dsl-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5

# consumer lag (is Flink keeping up?)
docker exec dsl-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-to-iceberg
```

## Flink jobs

```bash
docker exec dsl-jobmanager flink run -py /opt/jobs/realtime_metrics.py
docker exec dsl-jobmanager flink run -py /opt/jobs/orders_to_iceberg.py
# cancel: grab Job ID from the UI (localhost:8082), then
docker exec dsl-jobmanager flink cancel <job-id>
```

Checkpoints are the exactly-once spine (D5): if a job restarts, verify the
checkpoint history in the UI shows completed checkpoints before trusting counts.

## Incident: marts look wrong

1. `dq_gates` in Dagster — which gate failed? freshness/volume/nulls localizes it.
2. Consumer lag check above — is the pipeline just behind?
3. Kafka replay is the repair path (D9): the topic holds 7 days. See
   `orchestration/definitions.py::backfill_window`.
4. Iceberg time travel for forensics: `SELECT * FROM lake.raw.orders VERSION AS OF <snapshot-id>`.

## Incident: Docker won't start

Stack needs ~4 GB RAM for Docker Desktop (WSL2 backend). If the taskmanager
crash-loops, lower `taskmanager.numberOfTaskSlots` to 2 in docker-compose.yml.
