{{ config(materialized='view') }}

with cleaned as (
    select
        try_to_date(date) as month,
        try_cast(ces0500000003 as float) as avg_hourly_earnings,
        try_cast(cpiaucsl as float) as cpi_all_items,
        try_cast(cusr0000saf11 as float) as food_price_index,
        try_cast(cusr0000setb01 as float) as gasoline_price_index,
        try_cast(ecomnsa as float) as ecom_sales,
        try_cast(fedfunds as float) as fed_funds_rate,
        try_cast(jtsjol as float) as job_openings,
        try_cast(pce as float) as personal_consumption,
        try_cast(pi as float) as personal_income,
        try_cast(psavert as float) as personal_saving_rate,
        try_cast(revolsl as float) as credit_card_debt,
        try_cast(rsafs as float) as retail_sales,
        try_cast(umcsent as float) as consumer_sentiment
    from {{ source('raw_fred', 'FRED_MACRO_RAW') }}
)

select 
    month, 
    avg_hourly_earnings,
    cpi_all_items,
    food_price_index,
    gasoline_price_index,
    ecom_sales,
    fed_funds_rate,
    job_openings,
    personal_consumption,
    personal_income,
    personal_saving_rate,
    credit_card_debt,
    retail_sales,
    consumer_sentiment
from cleaned