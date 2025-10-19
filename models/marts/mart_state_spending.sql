{{ config(materialized='table') }}

with monthly as (
    select
        date_trunc('month', e.order_date)           as month,
        e.state                                     as state,
        sum(e.price * e.quantity)                   as total_spend,
        avg(e.price)                                as avg_price,
        avg(u.unemployment_rate)                    as avg_unemployment_rate,
        avg(m.cpi_all_items)                        as avg_cpi,
        avg(m.consumer_sentiment)                   as avg_consumer_sentiment,
        count(distinct e.user_id)                   as unique_customers,
        sum(e.quantity)                             as total_items_sold
    from {{ ref('int_amazon_fred_enriched') }} as e
    left join {{ ref('stg_fred_unemp') }} as u
        on e.state = u.state
        and date_trunc('month', e.order_date) = date_trunc('month', u.date)
    left join {{ ref('stg_fred_macro') }} as m
        on date_trunc('month', e.order_date) = date_trunc('month', m.date)
    where e.state != 'UNKNOWN'
    group by 1, 2
)

select *
from monthly
order by month, state
