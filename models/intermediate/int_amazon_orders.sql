{{ config(materialized='view') }}

with cleaned as (

    select
        user_id,
        order_date::date as order_date,
        upper(trim(state)) as state,
        upper(trim(product_category)) as product_category,
        price::float as price,
        quantity::int as quantity,

        
        price * quantity as total_spend,
        date_trunc('month', order_date) as month

    from {{ ref('stg_amazon_purchases') }}
    where price is not null 
      and quantity > 0
)

, normalized as (

    select
        user_id,
        order_date,
        month,
        state,
        case
            when product_category in ('BOOK', 'ABIS_BOOK') then 'BOOKS'
            when product_category like '%SHIRT%' then 'APPAREL'
            when product_category like '%ELECTRONIC%' then 'ELECTRONICS'
            else product_category
        end as normalized_category,
        price,
        quantity,
        total_spend
    from cleaned
)

select *
from normalized