{{ config(materialized='table') }}

{%- set event_types = get_event_types() -%}

with final as (
    select
        session_guid
        , min(e.created_at_utc) as session_start
        , max(e.created_at_utc) as session_end
        , datediff('minute', session_start, session_end) as session_len_minutes
        , count(e.event_guid) as event_count
        {% for etype in event_types %}
        , sum(case when e.event_type = '{{etype}}' then 1 else 0 end) as {{etype}}_count
        {% endfor %}
        , sum(case when e.event_type = 'checkout' then fo.order_total else 0 end) as order_total
    from {{ ref('stg_postgres__events') }} e
    left join {{ ref('fact_orders') }} fo on e.order_guid = fo.order_guid
    group by 1
)

select * from final