-- Model: int_national_macro_amazon
-- Grain: month
-- Purpose: Aggregates national-level Amazon metrics per month and joins macroeconomic indicators including CPI, income, consumption, and national unemployment rate.

{{ config(materialized='view') }}

with amazon_national as (
    select
        month,
        sum(total_spend) as national_total_spend,
        count(distinct user_id) as active_users,
        sum(total_spend) / nullif(count(distinct user_id), 0) as avg_user_spend
    from {{ ref('int_user_national_behavior') }}
    where {{ filter_by_cutoff('month') }}
    group by 1
),

macro as (
    select
        month,
        avg_hourly_earnings,      
        cpi_all_items,            
        food_price_index,         
        gasoline_price_index,     
        fed_funds_rate,       
        retail_sales,             
        consumer_sentiment,       
        personal_income,         
        personal_consumption,     
        personal_saving_rate     
    from {{ ref('stg_fred_macro') }}
    where {{filter_by_cutoff('month')}}
    and month is not null
),

unemp as (
    select
        month,
        avg(unemployment_rate) as national_unemployment_rate
    from {{ ref('stg_fred_unemp') }}
    where {{ filter_by_cutoff('month') }}
    group by month
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
    m.personal_saving_rate,
    u.national_unemployment_rate
from amazon_national a
left join macro m using (month)
left join unemp u using (month)
