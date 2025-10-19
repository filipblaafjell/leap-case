{{ config(materialized='view') }}

with base as (
    select
        p.user_id,
        p.order_date,
        p.state,
        p.product_category,
        p.price,
        p.quantity,
        s.age_group,
        s.gender,
        s.income_bracket,
        s.education_level,
        s.amazon_use_frequency,
        s.household_size,
        u.unemployment_rate,
        m.cpi_all_items,
        m.fed_funds_rate,
        m.personal_income,
        m.retail_sales,
        m.consumer_sentiment
    from {{ ref('stg_amazon_purchases') }} p
    left join {{ ref('stg_amazon_survey') }} s
        on p.user_id = s.user_id
    left join {{ ref('stg_fred_unemp') }} u
        on p.state = u.state
        and date_trunc('month', p.order_date) = date_trunc('month', u.date)
    left join {{ ref('stg_fred_macro') }} m
        on date_trunc('month', p.order_date) = date_trunc('month', m.date)
)

select *
from base