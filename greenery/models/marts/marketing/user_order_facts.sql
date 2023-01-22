{{
    config(materialized='table')
}}

with final as (
    select
        u.user_guid
        , count(o.order_guid) as order_count
        , (case when order_count > 0 then 1 else 0 end) as has_ordered
        , (case when order_count > 1 then 1 else 0 end) as has_ordered_mult
        , min(o.created_at_utc) as first_order_utc
        , max(o.created_at_utc) as last_order_utc
        , coalesce(avg(o.order_cost), 0) as avg_order_costs
        , coalesce(sum(o.order_cost), 0) as total_order_costs
        , coalesce(avg(o.shipping_cost), 0) as avg_shipping_costs
        , coalesce(sum(o.shipping_cost), 0) as total_shipping_costs
        , avg(DATEDIFF(day, o.created_at_utc, o.delivered_at_utc)) as avg_ship_time_days --null for users w/o completed order
        , count(distinct o.shipping_service) as distinct_ship_svcs_count
    from {{ref('stg_postgres__orders')}} o
    right join {{ref('stg_postgres__users')}} u on u.user_guid=o.user_guid --right join to include users w/o orders
    group by 1
)
select * from final