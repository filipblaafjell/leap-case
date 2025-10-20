{{ config(materialized='view') }}

with base as (
    select
        date_trunc('month', date::date) as month,
        avg_hourly_earnings,
        cpi_all_items,
        food_price_index,
        gasoline_price_index,
        ecom_sales,
        fed_funds_rate,
        job_openings,
        personal_consumption as personal_consumption_expenditure,
        personal_income,
        personal_saving_rate,
        credit_card_debt as revolving_credit,
        retail_sales,
        consumer_sentiment
    from {{ ref('stg_fred_macro') }}
    where date is not null
)

select *
from base
order by month
