{{ config(materialized='view') }}

with cleaned as (
    select
        survey_response_id                         as user_id,
        try_to_date(order_date)                    as order_date,
        trim(category)                             as product_category,
        cast(purchase_price_per_unit as float)     as price,
        cast(quantity as int)                      as quantity,
        
        -- Handle missing or inconsistent states
        case
            when shipping_address_state is null then 'UNKNOWN'
            else upper(trim(shipping_address_state))
        end                                        as state,

        trim(title)                                as product_title,
        trim(product_code)                         as product_code
    from {{ source('raw_amazon', 'AMAZON_PURCHASES_RAW') }}
    where order_date is not null
)

select * from cleaned
