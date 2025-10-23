-- Model: mart_state_product_trends
-- Grain: state + month + product_category
-- Purpose: Measures revenue and order trends per product category within each state.

{{ config(materialized='table', tags=['mart','state','product','trends']) }}

with base as (
    select
        state,
        month,
        product_category,
        total_revenue,
        total_quantity,
        unique_users,
        total_orders,
        lag(total_revenue) over (partition by state, product_category order by month) as prev_revenue
    from {{ ref('int_product_state_month') }}
)

select
    state,
    month,
    product_category,
    total_revenue,
    total_quantity,
    unique_users,
    total_orders,
    (total_revenue - prev_revenue) / nullif(prev_revenue, 0) as revenue_growth_mom
from base
