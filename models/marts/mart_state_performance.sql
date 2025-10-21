{{ config(materialized='table', tags=['mart','state','performance']) }}

with state_month as (
    select
        state,
        month,
        sum(total_spend) as total_spend,
        count(distinct user_id) as active_users,
        avg(total_spend) as avg_user_spend,
        avg(unemployment_rate) as avg_unemployment_rate
    from {{ ref('int_user_behavior') }}
    group by 1,2
)

select
    state,
    month,
    total_spend,
    active_users,
    avg_user_spend,
    avg_unemployment_rate
from state_month