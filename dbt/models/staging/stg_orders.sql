{{ config(materialized='view') }}

-- Dedup on event_id: Flink's exactly-once covers checkpoint/restart replays,
-- but the producer itself may double-send (chaos mode). Belt and braces (D5).
with ranked as (
    select
        event_id,
        order_id,
        restaurant_id,
        customer_id,
        event_type,
        order_value,
        event_ts,
        ingest_ts,
        row_number() over (
            partition by event_id
            order by ingest_ts
        ) as rn
    from read_parquet('../lakehouse/raw/orders/**/*.parquet', hive_partitioning = true)
)
select
    event_id,
    order_id,
    restaurant_id,
    customer_id,
    event_type,
    order_value,
    event_ts,
    ingest_ts
from ranked
where rn = 1
