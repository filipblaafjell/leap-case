{{ config(materialized='table', tags=['mart','state','product','trends']) }}

select
    state,
    month,
    product_category,
    total_revenue,
    total_quantity,
    unique_buyers,
    total_orders,
    lag(total_revenue) over (partition by state, product_category order by month) as prev_revenue,
    (total_revenue - lag(total_revenue) over (partition by state, product_category order by month))
        / nullif(lag(total_revenue) over (partition by state, product_category order by month), 0) as revenue_growth_mom
from {{ ref('int_product_state_month') }}
