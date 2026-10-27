{{ config(materialized='table') }}

-- Order funnel per restaurant-hour: the table ops and finance both read.
-- Partitioned thinking lives here because the raw table is partitioned by day;
-- hourly rollups are what the business actually reasons about.

with orders_pivot as (
    select
        restaurant_id,
        date_trunc('hour', event_ts) as hour_ts,
        count(*) filter (where event_type = 'created')   as orders_created,
        count(*) filter (where event_type = 'confirmed') as orders_confirmed,
        count(*) filter (where event_type = 'picked_up') as orders_picked_up,
        count(*) filter (where event_type = 'delivered') as orders_delivered,
        count(*) filter (where event_type = 'cancelled') as orders_cancelled,
        sum(order_value) filter (where event_type = 'delivered') as gmv_delivered,
        sum(order_value) filter (where event_type = 'cancelled') as gmv_lost
    from {{ ref('stg_orders') }}
    group by 1, 2
)

select
    *,
    -- funnel conversion: delivered / created. < 1 means events still in flight
    -- or lost; sustained > 1 means duplicates survived dedup -> pipeline bug.
    round(orders_delivered * 1.0 / nullif(orders_created, 0), 4) as delivery_rate,
    round(gmv_delivered, 2) as gmv_delivered
from orders_pivot
