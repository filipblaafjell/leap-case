{{ config(materialized='table') }}

with monthly as (
    select
        date_trunc('month', e.order_date)           as month,
        case 
            when e.state = 'AL' then 'Alabama'
            when e.state = 'AK' then 'Alaska'
            when e.state = 'AZ' then 'Arizona'
            when e.state = 'AR' then 'Arkansas'
            when e.state = 'CA' then 'California'
            when e.state = 'CO' then 'Colorado'
            when e.state = 'CT' then 'Connecticut'
            when e.state = 'DE' then 'Delaware'
            when e.state = 'FL' then 'Florida'
            when e.state = 'GA' then 'Georgia'
            when e.state = 'HI' then 'Hawaii'
            when e.state = 'ID' then 'Idaho'
            when e.state = 'IL' then 'Illinois'
            when e.state = 'IN' then 'Indiana'
            when e.state = 'IA' then 'Iowa'
            when e.state = 'KS' then 'Kansas'
            when e.state = 'KY' then 'Kentucky'
            when e.state = 'LA' then 'Louisiana'
            when e.state = 'ME' then 'Maine'
            when e.state = 'MD' then 'Maryland'
            when e.state = 'MA' then 'Massachusetts'
            when e.state = 'MI' then 'Michigan'
            when e.state = 'MN' then 'Minnesota'
            when e.state = 'MS' then 'Mississippi'
            when e.state = 'MO' then 'Missouri'
            when e.state = 'MT' then 'Montana'
            when e.state = 'NE' then 'Nebraska'
            when e.state = 'NV' then 'Nevada'
            when e.state = 'NH' then 'New Hampshire'
            when e.state = 'NJ' then 'New Jersey'
            when e.state = 'NM' then 'New Mexico'
            when e.state = 'NY' then 'New York'
            when e.state = 'NC' then 'North Carolina'
            when e.state = 'ND' then 'North Dakota'
            when e.state = 'OH' then 'Ohio'
            when e.state = 'OK' then 'Oklahoma'
            when e.state = 'OR' then 'Oregon'
            when e.state = 'PA' then 'Pennsylvania'
            when e.state = 'RI' then 'Rhode Island'
            when e.state = 'SC' then 'South Carolina'
            when e.state = 'SD' then 'South Dakota'
            when e.state = 'TN' then 'Tennessee'
            when e.state = 'TX' then 'Texas'
            when e.state = 'UT' then 'Utah'
            when e.state = 'VT' then 'Vermont'
            when e.state = 'VA' then 'Virginia'
            when e.state = 'WA' then 'Washington'
            when e.state = 'WV' then 'West Virginia'
            when e.state = 'WI' then 'Wisconsin'
            when e.state = 'WY' then 'Wyoming'
            when e.state = 'PR' then 'Puerto Rico'
            else e.state
        end as state_name,
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
order by month, state_name
