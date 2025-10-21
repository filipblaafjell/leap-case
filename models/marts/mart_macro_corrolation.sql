{{ config(materialized='table', tags=['mart','macro','national']) }}

with base as (
    select
        month,
        national_total_spend,
        active_users,
        avg_user_spend,
        avg_hourly_earnings,
        cpi_all_items,
        fed_funds_rate,
        retail_sales,
        consumer_sentiment,
        personal_income,
        personal_consumption,
        personal_saving_rate
    from {{ ref('int_national_macro_amazon') }}
),

metrics as (
    select
        month,
        national_total_spend,
        active_users,
        avg_user_spend,
        avg_hourly_earnings,
        cpi_all_items,
        fed_funds_rate,
        retail_sales,
        consumer_sentiment,
        personal_income,
        personal_consumption,
        personal_saving_rate,

        -- Derived KPIs
        (national_total_spend - lag(national_total_spend) over (order by month))
            / lag(national_total_spend) over (order by month) as spend_growth_mom,

        (active_users - lag(active_users) over (order by month))
            / lag(active_users) over (order by month) as active_users_growth_mom
    from base
)

select
    month,
    national_total_spend,
    active_users,
    avg_user_spend,
    avg_hourly_earnings,
    cpi_all_items,
    fed_funds_rate,
    retail_sales,
    consumer_sentiment,
    personal_income,
    personal_consumption,
    personal_saving_rate,
    spend_growth_mom,
    active_users_growth_mom
from metrics
