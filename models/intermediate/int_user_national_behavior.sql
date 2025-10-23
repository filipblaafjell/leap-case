-- Model: int_user_behavior
-- Grain: user_id + month
-- Purpose: One record per user per month with spend and order metrics, joined to demographics.
-- Tests: TODO

{{ config(materialized='view') }}

with purchases as (
    select
        user_id,
        date_trunc('month', order_date) as month,
        sum(price * quantity) as total_spend,
        count(distinct product_code) as unique_products,
        count(*) as total_orders
    from {{ ref('stg_amazon_purchases') }}
    where {{ filter_by_cutoff('order_date') }}
    and user_id is not null
    and product_code is not null
    group by 1,2
),

joined_user as (
    select
        p.user_id,
        p.month,
        p.total_spend,
        p.unique_products,
        p.total_orders,
        u.gender,
        u.age_group,
        u.income_bracket,
        u.education_level,
        u.household_size
    from purchases p
    left join {{ ref('stg_amazon_survey') }} u using (user_id)
    where user_id is not null
    and month is not null
)

select
    user_id,
    month,
    total_spend,
    unique_products,
    total_orders,
    gender,
    age_group,
    income_bracket,
    education_level,
    household_size
from joined_user
