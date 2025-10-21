{{ config(materialized='view') }}

with cleaned as (
    select
        try_to_date(date) as date,
        upper(trim(state)) as state,
        try_cast(unemployment_rate as float) as unemployment_rate
    from {{ source('raw_fred', 'FRED_UNEMPLOYMENT_STATE_RAW') }}
    where date is not null
)

select
    date,
    state,
    unemployment_rate
from cleaned