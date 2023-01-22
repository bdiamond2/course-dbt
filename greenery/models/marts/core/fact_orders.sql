{{
    config(materialized='table')
}}

with order_event_count as (
    select
        order_guid
        , count(*) as event_count
    from {{ ref('stg_postgres__events') }}
    where order_guid is not null
    group by 1
)
, order_item_count as (
    select
        order_guid
        , count(*) as item_count
    from {{ ref('stg_postgres__order_items') }}
    group by 1
)
, final as (
    select
        o.order_guid
        , o.user_guid
        , o.promo_desc
        , o.address_guid
        , o.created_at_utc
        , o.order_cost
        , o.shipping_cost
        , o.order_total
        , o.tracking_guid
        , o.shipping_service
        , o.estimated_delivery_at_utc
        , o.delivered_at_utc
        , o.status
        , oec.event_count
        , oic.item_count
    from {{ ref('stg_postgres__orders')}} o
    left join order_event_count oec on o.order_guid = oec.order_guid
    left join order_item_count oic on o.order_guid = oic.order_guid
)

select * from final