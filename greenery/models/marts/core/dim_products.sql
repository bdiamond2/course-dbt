{{
    config(materialized='table')
}}

with final as (
    select
        product_guid
        , name
        , price
        , inventory
    from {{ ref('stg_postgres__products') }}
)

select * from final