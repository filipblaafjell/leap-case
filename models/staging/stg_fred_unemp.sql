{{ config(materialized='view') }}

with cleaned as (
    select
        try_to_date(date) as month,
        upper(trim(state)) as state,
        try_cast(unemployment_rate as float) as unemployment_rate
    from {{ source('raw_fred', 'FRED_UNEMPLOYMENT_STATE_RAW') }}
)

select
    month,
    state,
    unemployment_rate
from cleaned
where {{ filter_valid_states('state') }}
  and state is not null