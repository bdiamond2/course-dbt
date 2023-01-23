{{
    config(materialized='table')
}}

with daily_prd_events as (
    select
        product_guid
        , TRUNC(created_at_utc, 'day') as date
        , event_type
        , count(*) as count
    from {{ ref('stg_postgres__events') }}
    where product_guid is not null
    group by 1, 2, 3
),
avg_daily_events as (
    select
        product_guid
        , event_type
        , avg(count) as count
    from daily_prd_events
    group by 1, 2
),
avg_daily_pv as (
    select product_guid, count
    from avg_daily_events
    where event_type = 'page_view'
),
avg_daily_atc as (
    select product_guid, count
    from avg_daily_events
    where event_type = 'add_to_cart'
),

event_total_counts as (
    select
        p.product_guid
        , sum(case when event_type='page_view' then 1 else 0 end) as page_view_total_count
        , sum(case when event_type='add_to_cart' then 1 else 0 end) as add_to_cart_total_count
    from {{ ref('stg_postgres__events') }} e
    left join {{ ref('stg_postgres__products') }} p on e.product_guid = p.product_guid
    where e.product_guid is not null
    group by 1
),

order_counts as (
    select
        product_guid
        , count(*) as order_count
        , sum(quantity) as total_quantity_sold
    from {{ ref('stg_postgres__order_items') }}
    group by 1
)

select
    etc.product_guid
    , etc.page_view_total_count
    , adpv.count as page_view_avg_daily_count
    , etc.add_to_cart_total_count
    , adatc.count as add_to_cart_avg_daily_count
    , oc.order_count
    , oc.total_quantity_sold
from event_total_counts etc
left join avg_daily_pv adpv using (product_guid)
left join avg_daily_atc adatc using (product_guid)
left join order_counts oc using (product_guid)