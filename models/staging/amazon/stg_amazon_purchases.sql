{{ config(materialized='view') }}

with cleaned as (
    select
        survey_response_id                                                      as user_id,
        try_to_date(order_date)                                                 as order_date,
        initcap(nullif(trim(upper(product_category)), 'UNKNOWN'))               as product_category,  
        try_cast(purchase_price_per_unit as float)                              as price,
        try_cast(quantity as int)                                               as quantity,
        upper(trim(shipping_address_state))                                     as state,
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