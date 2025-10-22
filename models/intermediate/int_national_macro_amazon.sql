{{ config(materialized='view') }}

with amazon_national as (
    select
        month,
        sum(total_spend) as national_total_spend,
        count(distinct user_id) as active_users,
        avg(total_spend) as avg_user_spend
    from {{ ref('int_user_behavior') }}
    where {{filter_by_cutoff('month')}}
    group by 1
),

macro as (
    select
        date_trunc('month', date)     as month,
        avg(avg_hourly_earnings)      as avg_hourly_earnings,
        avg(cpi_all_items)            as cpi_all_items,
        avg(food_price_index)         as food_price_index,
        avg(gasoline_price_index)     as gasoline_price_index,
        avg(fed_funds_rate)           as fed_funds_rate,
        avg(retail_sales) * 1000000   as retail_sales,
        avg(consumer_sentiment)       as consumer_sentiment,
        avg(personal_income)          as personal_income,
        avg(personal_consumption)     as personal_consumption,
        avg(personal_saving_rate)     as personal_saving_rate
    from {{ ref('stg_fred_macro') }}
    where {{filter_by_cutoff('date')}}
    group by 1
)

select
    a.month,
    a.national_total_spend,
    a.active_users,
    a.avg_user_spend,
    m.avg_hourly_earnings,
    m.cpi_all_items,
    m.food_price_index,
    m.gasoline_price_index,
    m.fed_funds_rate,
    m.retail_sales,
    m.consumer_sentiment,
    m.personal_income,
    m.personal_consumption,
    m.personal_saving_rate
from amazon_national a
left join macro m using (month)
