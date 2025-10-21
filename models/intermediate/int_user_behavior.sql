{{ config(materialized='view') }}

with purchases as (
    select
        user_id,
        date_trunc('month', order_date) as month,
        state,
        sum(price * quantity) as total_spend,
        count(distinct product_code) as unique_products,
        count(*) as total_orders
    from {{ ref('stg_amazon_purchases') }}
    group by 1,2,3
),

joined_user as (
    select
        p.user_id,
        p.month,
        p.state,
        p.total_spend,
        p.unique_products,
        p.total_orders,
        u.gender,
        u.age_group,
        u.income_bracket,
        u.education_level,
        u.household_size
    from purchases p
    left join {{ ref('int_user_profile') }} u using (user_id)
),

joined_unemp as (
    select
        ju.*,
        f.unemployment_rate
    from joined_user ju
    left join {{ ref('stg_fred_unemp') }} f
        on ju.state = f.state
       and ju.month = date_trunc('month', f.date)
)

select
    user_id,
    month,
    state,
    total_spend,
    unique_products,
    total_orders,
    gender,
    age_group,
    income_bracket,
    education_level,
    household_size,
    unemployment_rate
from joined_unemp
