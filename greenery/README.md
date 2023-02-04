# WEEK 3
*What is our overall conversion rate?*
```
select
    div0(
        sum(case when checkout_count > 0 then 1 else 0 end)
        , count(*)
    )
from fact_session_stats;
```
0.624567

*What is our conversion rate by product?*
```
select
    product
    , count(*) as sessions
    , sum(case when has_purchase = 1 then 1 else 0 end) as sessions_w_purchase
    , sessions_w_purchase / sessions as conversion_rate
from fact_session_product_purchase
group by 1
```
e.g. fb0e8be7-5ac4-4a76-a1fa-2cc4bf0b2d80 had **60.9%**

# WEEK 2
*What is our user repeat rate?*
```
select
    div0(
        sum(case when
            has_ordered_mult=1
            then 1 else 0 end)
        , sum(case when
            has_ordered=1
            then 1 else 0 end)
            ) as repeat_rate
from user_order_facts
```
- 0.798387

*What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?
NOTE: This is a hypothetical question vs. something we can analyze in our Greenery data set. Think about what exploratory analysis you would do to approach this question.*
We'd want to look at a combination of attributes from dim_users as well as any patterns that may exist in their order history. Things like city/state/country could be telling, as well as other demographic info we don't currently have access to in this dataset. In terms of purchase behavior, we could look at a number of things like the timing of their activity and orders, quantity purchased, or the specific products they tend to buy.

*Explain the marts models you added. Why did you organize the models in the way you did?*
I didn't identify any opportunities for intermediate models myself, but I'll take a look at some examples later.

Core:
- dim_products: Basic product fields, nothing added.
- dim_users: Basic user info plus joined address fields so analysts don't have to merge on address_guid.
- fact_orders: Basic order info plus the number of items in each order and the number of events associated with it. Item number is likely the more helpful addition, so you can check things like average items per order.

Marketing:
- user_order_facts: Number of orders they had, if they were repeat users, datetime of first and last orders, avg and totals for the costs of their orders, average shipping time, and the number of different shipping services used for them. This would allow us to identify who the biggest buyers are and look for patterns (e.g. does shipping cost or time correlate with order volume?).

Product:
- fact_product_engagement: Because what we're really looking for is more than just page views, I opted to call it "product engagement" to be more general. Here I look at product-level info for page views, add-to-cart actions, number of orders included in, and total number sold. The obvious application is finding out what the most popular products are, but also something like seeing what gets viewed/added but not purchased as much.


*Use the dbt docs to visualize your model DAGs to ensure the model layers make sense*
See '2023.01.23 dbt-dag.png' in this dbt directory.

# WEEK 1
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

