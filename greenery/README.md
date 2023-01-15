How many users do we have?
```
select count(distinct user_guid)
from stg_postgres__users;
```
- 130

On average, how many orders do we receive per hour?
```
with orders_per_hr as (
    select
        TRUNC(created_at_utc, 'hour'),
        count(order_guid) as orders
    from stg_postgres__orders
    group by 1
)
select AVG(orders) from orders_per_hr;
```
- 7.520833

On average, how long does an order take from being placed to being delivered?
```
with deliv_time as (
    select
        order_guid,
        TIMEDIFF('hour', created_at_utc, delivered_at_utc) as hours_to_deliver
    from stg_postgres__orders
    where delivered_at_utc is not null
)
select AVG(hours_to_deliver) from deliv_time;
```
- 93.4 hours

How many users have only made one purchase? Two purchases? Three+ purchases?
Note: you should consider a purchase to be a single order. In other words, if a user places one order for 3 products, they are considered to have made 1 purchase.
```
with orders_per_user as (
    select
        user_guid,
        count(order_guid) as num_orders
    from stg_postgres__orders
    group by 1
    )
,
orders_per_user_grpd as (
    select
        user_guid,
        num_orders,
        case
            when num_orders = 1
                then '1'
            when num_orders = 2
                then '2'
            else
                '3+'
        end as num_orders_grpd
    from orders_per_user
)
select
    num_orders_grpd,
    count(user_guid)
from orders_per_user_grpd
group by 1
order by 1
;
```
- 25 with just 1, 28 with 2, and 71 with 3 or more orders.

On average, how many unique sessions do we have per hour?
```
with events_by_hour as (
    select
        TRUNC(created_at_utc, 'hour') as hour_utc,
        count(event_guid) as event_count
    from stg_postgres__events
    group by 1
)
select AVG(event_count)
from events_by_hour
;
```
- 61.26

