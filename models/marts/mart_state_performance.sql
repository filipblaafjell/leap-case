-- Model: mart_state_performance
-- Grain: state + month
-- Purpose: Aggregates state-level spend and activity metrics per month.
--          Based on user-state behavior intermediate.

{{ config(materialized='table', tags=['mart','state','performance']) }}

with state_month as (
    select
        {{ rename_states('state') }} as state,
        month,
        sum(total_spend) as total_spend,
        count(distinct user_id) as active_users,
        sum(total_spend) / nullif(count(distinct user_id), 0) as avg_user_spend
    from {{ ref('int_user_state_behavior') }}
    group by 1,2
),

moms as (
    select
        state,
        month,
        total_spend,
        active_users,
        avg_user_spend,
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
    (total_spend - prev_total_spend) / nullif(prev_total_spend, 0) as spend_growth_mom,
    (avg_user_spend - prev_avg_spend) / nullif(prev_avg_spend, 0) as avg_spend_growth_mom
from moms
