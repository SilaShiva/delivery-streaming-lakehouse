{{ config(materialized='table') }}

-- Restaurant health scorecard: the table an ops team would actually be paged on.
-- Cancellation-rate spikes are the earliest signal of restaurant-side trouble
-- (closed but still accepting orders, menu mismatch, rider starvation).

with recent as (
    select * from {{ ref('mart_order_funnel') }}
    where hour_ts >= (
        select coalesce(max(hour_ts) - interval 24 hour, min(hour_ts))
        from {{ ref('mart_order_funnel') }}
    )
)

select
    restaurant_id,
    sum(orders_created)   as orders_24h,
    sum(orders_cancelled) as cancellations_24h,
    round(sum(orders_cancelled) * 1.0 / nullif(sum(orders_created), 0), 4) as cancel_rate,
    round(sum(gmv_delivered), 2)                                          as gmv_24h,
    -- crude health tier: ops focuses on the worst decile first
    case
        when sum(orders_cancelled) * 1.0 / nullif(sum(orders_created), 0) > 0.15 then 'critical'
        when sum(orders_cancelled) * 1.0 / nullif(sum(orders_created), 0) > 0.08 then 'watch'
        else 'healthy'
    end as health_tier
from recent
group by restaurant_id
