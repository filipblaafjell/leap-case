{{ config(materialized='view') }}

with base as (
    select
        state,
        product_code,
        date_trunc('month', order_date) as month,
        trim(product_category) as product_category,
        sum(price * quantity) as total_revenue,
        sum(quantity) as total_quantity,
        count(distinct user_id) as unique_buyers,
        count(*) as total_orders
    from {{ ref('stg_amazon_purchases') }}
    where {{ filter_by_cutoff('order_date') }}
    and state is not null
    and {{ filter_valid_states('state') }}
    and price is not null
    and quantity is not null
    group by 1,2,3
)

select
    product_code,
    month,
    product_category,
    total_revenue,
    total_quantity,
    unique_buyers,
    total_orders
from base
