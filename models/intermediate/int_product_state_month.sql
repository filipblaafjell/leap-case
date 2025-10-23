-- Model: int_product_state_month
-- Grain: state + product_category + month
-- Purpose: Aggregates monthly product category performance per state.

{{ config(materialized='view') }}

with base as (
    select
        upper(trim(state)) as state,
        date_trunc('month', order_date) as month,
        upper(trim(product_category)) as product_category,
        sum(price * quantity) as total_revenue,
        sum(quantity) as total_quantity,
        count(distinct user_id) as unique_users,
        count(*) as total_orders
    from {{ ref('stg_amazon_purchases') }}
    where {{ filter_by_cutoff('order_date') }}
      and month is not null
      and state is not null
    group by 1,2,3
)

select
    state,
    month,
    product_category,
    total_revenue,
    total_quantity,
    unique_users,
    total_orders
from base
