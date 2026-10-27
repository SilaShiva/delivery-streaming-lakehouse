-- Batch tier: Kafka orders -> Iceberg table on the local filesystem, exactly-once.
--
-- The exactly-once story (D5): Flink checkpoints + Iceberg commits form a
-- two-phase protocol. Data files from uncommitted checkpoints are orphaned and
-- cleaned; committed files are never replayed into the table. That is what
-- makes the sink exactly-once, not merely idempotent.
--
-- Storage: local directory via Iceberg's Hadoop catalog. Object storage is a
-- deployment detail; the table format semantics (snapshots, time travel,
-- partition evolution) are identical to an S3-backed warehouse (D7).

CREATE CATALOG IF NOT EXISTS lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///C:/Users/sila.shivanandan/Desktop/Swiggy/DS Projects/delivery-streaming-lakehouse/lakehouse'
);

CREATE DATABASE IF NOT EXISTS lake.raw;

CREATE TABLE IF NOT EXISTS lake.raw.orders (
    event_id       STRING,
    order_id       STRING,
    restaurant_id  STRING,
    customer_id    STRING,
    event_type     STRING,
    order_value    DOUBLE,
    event_ts       TIMESTAMP(3),
    ingest_ts      TIMESTAMP(3)
) PARTITIONED BY (days(event_ts)) WITH (
    'write.format.default' = 'parquet',
    'write.target-file-size-bytes' = '134217728',
    'format-version' = '2'
);

CREATE TABLE source_orders (
    event_id       STRING,
    order_id       STRING,
    restaurant_id  STRING,
    customer_id    STRING,
    event_type     STRING,
    order_value    DOUBLE,
    event_ts       TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-json',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-to-iceberg',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

INSERT INTO lake.raw.orders
SELECT
    event_id,
    order_id,
    restaurant_id,
    customer_id,
    event_type,
    order_value,
    event_ts,
    CURRENT_TIMESTAMP AS ingest_ts
FROM source_orders;
