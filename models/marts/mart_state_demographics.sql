{{ config(materialized='table', tags=['mart','state','demographics']) }}

with demo_state_month as (
    select
        state,
        month,
        gender,
        age_group,
        income_bracket,
        sum(total_spend) as total_spend,
        count(distinct user_id) as active_users,
        avg(total_spend) as avg_user_spend,
        avg(unemployment_rate) as avg_unemployment_rate
    from {{ ref('int_user_behavior') }}
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
    avg_unemployment_rate
from demo_state_month

