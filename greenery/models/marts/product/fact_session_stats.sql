{{
    config(materialized='table')
}}

with final as (
    select
        session_guid
        , min(e.created_at_utc) as session_start
        , max(e.created_at_utc) as session_end
        , datediff('minute', session_start, session_end) as session_len_minutes
        , count(e.event_guid) as event_count
        , sum(case when e.event_type = 'checkout' then 1 else 0 end) as checkout_count
        , sum(case when e.event_type = 'package_shipped' then 1 else 0 end) as package_shipped_count
        , sum(case when e.event_type = 'add_to_cart' then 1 else 0 end) as add_to_cart_count
        , sum(case when e.event_type = 'page_view' then 1 else 0 end) as page_view_count
        , sum(case when e.event_type = 'checkout' then fo.order_total else 0 end) as order_total
    from {{ ref('stg_postgres__events') }} e
    left join {{ ref('fact_orders') }} fo on e.order_guid = fo.order_guid
    group by 1
)

select * from final