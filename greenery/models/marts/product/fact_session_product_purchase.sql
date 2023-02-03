{{
    config(materialized='table')
}}

with final as (
    select
        e.session_guid
        , coalesce(e.product_guid, oi.product_guid) product
        -- , max(case when e.product_guid is not null then 1 else 0 end) as has_page_view
        , max(case when oi.product_guid is not null then 1 else 0 end) as has_purchase
    from {{ ref('stg_postgres__events') }} e
    left join {{ ref('stg_postgres__order_items') }} oi on e.order_guid = oi.order_guid
    group by 1, 2
)

select * from final

