-- Model: mart_state_demographics
-- Grain: state + month + gender + age_group + income_bracket
-- Purpose: Analyze unemployment and spend patterns by demographic segment within each state.

{{ config(materialized='table', tags=['mart','state','demographics']) }}

with demo_state_month as (
    select
        {{ rename_states('state') }} as state,
        month,
        gender,
        age_group,
        income_bracket,
        sum(total_spend) as total_spend,
        count(distinct user_id) as active_users,
        sum(total_spend) / nullif(count(distinct user_id), 0) as avg_user_spend,
        avg(state_unemployment_rate) as state_unemployment_rate
    from {{ ref('int_user_state_behavior') }}
    where gender is not null
      and age_group is not null
      and income_bracket is not null
    group by 1,2,3,4,5
)

select
    state,
    month,
    gender,
    age_group,
    income_bracket,
    total_spend,
    active_users,
    avg_user_spend,
    state_unemployment_rate,
    total_spend/sum(total_spend) over (partition by state, month) as revenue_share
from demo_state_month
