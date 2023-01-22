{{
    config(materialized='table')
}}

with final as (
    select
        u.user_guid
        , u.first_name
        , u.last_name
        , u.email
        , u.phone_number
        , u.created_at_utc
        , u.updated_at_utc
        , u.address_guid
        , a.address
        , a.zipcode
        , a.state
        , a.country
    from {{ ref('stg_postgres__users') }} u
    left join {{ ref('stg_postgres__addresses') }} a using (address_guid)
)

select * from final