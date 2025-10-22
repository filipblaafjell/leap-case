{{ config(materialized='table', tags=['mart','state','performance']) }}

with state_month as (
    select
        {{ rename_states('state') }} as state,
        month,
        sum(total_spend) as total_spend,
        count(distinct user_id) as active_users,
        avg(total_spend) as avg_user_spend,
        avg(unemployment_rate) as avg_unemployment_rate
    from {{ ref('int_user_behavior') }}
    group by 1,2
),

moms as (
    select
        state,
        month,
        total_spend,
        active_users,
        avg_user_spend,
        avg_unemployment_rate,
        lag(total_spend) over (partition by state order by month) as prev_total_spend,
        lag(avg_user_spend) over (partition by state order by month) as prev_avg_spend
    from state_month
)

select
    state,
    month,
    total_spend,
    active_users,
    avg_user_spend,
    avg_unemployment_rate,
    -- MoM growth calculations
    (total_spend - prev_total_spend) / nullif(prev_total_spend, 0) as spend_growth_mom,
    (avg_user_spend - prev_avg_spend) / nullif(prev_avg_spend, 0) as avg_spend_growth_mom
from moms
