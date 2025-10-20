{{ config(materialized='view') }}

with base as (

    select
        upper(trim(state)) as state,
        date_trunc('month', date::date) as month,
        unemployment_rate::float as unemployment_rate
    from {{ ref('stg_fred_unemp') }}
    where unemployment_rate is not null
)

, cleaned as (

    select
        state,
        month,
        round(unemployment_rate, 2) as unemployment_rate
    from base
    where state not in ('', 'NA', 'UNKNOWN')
)

select *
from cleaned
order by state, month
