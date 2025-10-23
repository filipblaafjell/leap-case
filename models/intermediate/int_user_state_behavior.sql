-- Model: int_user_state_behavior
-- Grain: user_id + month + state
-- Purpose: Aggregates user-level spend and order behavior per state per month, 
--          enriched with demographic data and state-level unemployment.

{{ config(materialized='view') }}

with purchases as (
    select
        user_id,
        upper(trim(state)) as state,
        date_trunc('month', order_date) as month,
        (sum(coalesce(price,0)*coalesce(quantity,0))) as total_spend,
        count(distinct product_code) as unique_products,
        count(*) as total_orders
    from {{ ref('stg_amazon_purchases') }}
    where {{ filter_by_cutoff('order_date') }}
      and user_id is not null
      and month is not null
      and state is not null
      and product_code is not null
    group by 1,2,3
),

joined_user as (
    select
        p.user_id,
        p.state,
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
),

state_unemp as (
    select
        date_trunc('month', month) as month,
        upper(trim(state)) as state,
        avg(unemployment_rate) as state_unemployment_rate
    from {{ ref('stg_fred_unemp') }}
    where {{ filter_by_cutoff('month') }}
    group by 1,2
)

select
    j.user_id,
    j.state,
    j.month,
    j.total_spend,
    j.unique_products,
    j.total_orders,
    j.gender,
    j.age_group,
    j.income_bracket,
    j.education_level,
    j.household_size,
    u.state_unemployment_rate
from joined_user j
left join state_unemp u
  on j.state = u.state
 and j.month = u.month
