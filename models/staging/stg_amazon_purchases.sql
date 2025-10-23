{{ config(materialized='view') }}

with cleaned as (
    select
        survey_response_id                                                      as user_id,
        try_to_date(order_date)                                                 as order_date,
        nullif(trim(upper(mapped_category)), 'UNKNOWN')                         as product_category,  
        cast(nullif(trim(purchase_price_per_unit), '') as float)                as price,
        cast(nullif(trim(quantity), '') as float)                               as quantity,                                 
        nullif(upper(trim(shipping_address_state)), 'UNKNOWN')                  as state,
        nullif(trim(title), '')                                                 as product_title,
        nullif(trim(product_code), '')                                          as product_code
    from {{ source('raw_amazon', 'AMAZON_PURCHASES_RAW') }}   
)

select
    user_id,
    order_date,
    product_category,
    price,
    quantity,
    state,
    product_title,
    product_code
from cleaned
where {{ filter_valid_states('state') }}
  and state is not null