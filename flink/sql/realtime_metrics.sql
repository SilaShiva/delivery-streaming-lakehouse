-- Real-time tier: 10-minute event-time windows over order events, Flink SQL.
--
-- This is the job that actually runs on the native-Windows stack. The
-- DataStream-API twin lives in flink/pyflink_walkthrough/ for line-by-line
-- interview walkthroughs; SQL is the form that runs everywhere (D2/D11).
--
-- Watermark: 5 min bounded out-of-orderness (D4 — measured p99 lateness 3.2 min).
-- Late data beyond the watermark: this SQL job lets Flink drop it silently,
-- which is exactly why the audit path exists — the side-output job above runs
-- in parallel during demo runs, and the late-orders topic is reconciled nightly.

CREATE TABLE source_orders (
    event_id       STRING,
    order_id       STRING,
    restaurant_id  STRING,
    customer_id    STRING,
    event_type     STRING,
    order_value    DOUBLE,
    event_ts       TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders-json',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-realtime-sql',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'json'
);

CREATE TABLE live_metrics (
    restaurant_id  STRING,
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    gmv            DOUBLE,
    created        BIGINT,
    delivered      BIGINT,
    cancelled      BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'live-metrics',
    'properties.bootstrap.servers' = 'localhost:9092',
    'value.format' = 'json'
);

INSERT INTO live_metrics
SELECT
    restaurant_id,
    window_start,
    window_end,
    round(sum(gmv_delta), 2)                          AS gmv,
    count(*) filter (where event_type = 'created')    AS created,
    count(*) filter (where event_type = 'delivered')  AS delivered,
    count(*) filter (where event_type = 'cancelled')  AS cancelled
FROM TABLE(
    TUMBLE(TABLE source_orders, DESCRIPTOR(event_ts), INTERVAL '10' MINUTES)
)
GROUP BY restaurant_id, window_start, window_end;
