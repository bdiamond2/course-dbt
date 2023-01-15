with addr_src as (
    select *
    from {{ source('postgres', 'addresses') }}
)
, recast as (
    select
        ADDRESS_ID as address_guid
        , ADDRESS
        , ZIPCODE
        , STATE
        , COUNTRY
    from addr_src
)

select * from recast