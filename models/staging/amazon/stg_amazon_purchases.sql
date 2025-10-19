{{ config(materialized='view') }}

SELECT
    survey_response_id                AS user_id,
    order_date                        AS order_date,
    category                          AS product_category,
    purchase_price_per_unit           AS price,
    quantity::int                     AS quantity,
    shipping_address_state            AS state,
    title                             AS product_title,
    product_code                      AS product_code
FROM {{ source('raw_amazon', 'AMAZON_PURCHASES_RAW') }} 
WHERE order_date IS NOT NULL