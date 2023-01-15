with src_promos as (
    select *
    from {{ source('postgres', 'promos') }}
)

, recast as (
    select
        promo_id as promo_code
        , discount
        , status
    from src_promos
)

select * from recast