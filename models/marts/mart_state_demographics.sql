{{ config(materialized='table', tags=['mart','state','demographics']) }}

with demo_state_month as (
    select
        {{ rename_states('state') }} as state,
        month,
        gender,
        age_group,
        income_bracket,
        avg(unemployment_rate) as state_unemployment_rate,
    from {{ ref('int_user_behavior') }}
    group by 1,2,3,4,5
)

select
    state,
    month,
    gender,
    age_group,
    income_bracket,
    state_unemployment_rate
from demo_state_month

