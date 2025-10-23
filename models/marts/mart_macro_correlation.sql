-- Model: mart_macro_correlation
-- Grain: month
-- Purpose: Combines national-level Amazon metrics with macroeconomic indicators
--          and computes month-over-month growth metrics.

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
        personal_saving_rate,
        national_unemployment_rate
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
        lag(national_total_spend) over (order by month) as prev_total_spend,
        lag(active_users) over (order by month) as prev_active_users,
        national_unemployment_rate
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
    (national_total_spend - prev_total_spend) / nullif(prev_total_spend, 0) as spend_growth_mom,
    (active_users - prev_active_users) / nullif(prev_active_users, 0) as active_users_growth_mom,
    national_unemployment_rate
from metrics
