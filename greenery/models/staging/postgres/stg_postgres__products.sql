with src_products as (
    select *
    from {{ source('postgres', 'products') }}
)

, recast as (
    select
    product_id as product_guid
    , name
    , price
    , inventory
    from src_products
)

select * from recast