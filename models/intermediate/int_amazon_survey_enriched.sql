{{ config(materialized='view') }}

with cleaned as (

    select
        user_id,
        trim(lower(gender)) as gender_raw,
        trim(age_group) as age_group_raw,
        trim(income_bracket) as income_bracket_raw,
        trim(education_level) as education_level_raw,
        trim(amazon_use_frequency) as amazon_use_frequency_raw,
        nullif(household_size, '')::int as household_size
    from {{ ref('stg_amazon_survey') }}
    where user_id is not null
)

, standardized as (

    select
        user_id,

        -- Normalize gender
        case 
            when gender_raw like '%female%' then 'Female'
            when gender_raw like '%male%' then 'Male'
            else 'Other/Unknown'
        end as gender,

        -- Standardize age groups
        case 
            when age_group_raw in ('18-24 years','18–24 years') then '18-24'
            when age_group_raw in ('25-34 years','25–34 years') then '25-34'
            when age_group_raw in ('35-44 years','35–44 years') then '35-44'
            when age_group_raw in ('45-54 years','45–54 years') then '45-54'
            when age_group_raw in ('55-64 years','55–64 years') then '55-64'
            when age_group_raw ilike '%65%' then '65+'
            else 'Unknown'
        end as age_group,

        -- Numeric ordering for age
        case 
            when age_group_raw in ('18-24 years','18–24 years') then 1
            when age_group_raw in ('25-34 years','25–34 years') then 2
            when age_group_raw in ('35-44 years','35–44 years') then 3
            when age_group_raw in ('45-54 years','45–54 years') then 4
            when age_group_raw in ('55-64 years','55–64 years') then 5
            when age_group_raw ilike '%65%' then 6
            else null
        end as age_group_numeric,

        -- Clean income brackets
        case
            when income_bracket_raw ilike '%less%' then '<25k'
            when income_bracket_raw ilike '%25%' then '25k–49k'
            when income_bracket_raw ilike '%50%' then '50k–74k'
            when income_bracket_raw ilike '%75%' then '75k–99k'
            when income_bracket_raw ilike '%100%' then '100k–149k'
            when income_bracket_raw ilike '%150%' then '150k+'
            else 'Unknown'
        end as income_bracket,

        -- Numeric income group
        case
            when income_bracket_raw ilike '%less%' then 1
            when income_bracket_raw ilike '%25%' then 2
            when income_bracket_raw ilike '%50%' then 3
            when income_bracket_raw ilike '%75%' then 4
            when income_bracket_raw ilike '%100%' then 5
            when income_bracket_raw ilike '%150%' then 6
            else null
        end as income_group_numeric,

        -- Education level normalization
        case
            when education_level_raw ilike '%high school%' then 'High School'
            when education_level_raw ilike '%bachelor%' then 'Bachelor'
            when education_level_raw ilike '%graduate%' or education_level_raw ilike '%master%' then 'Graduate'
            when education_level_raw ilike '%some college%' then 'Some College'
            else 'Unknown'
        end as education_level,

        -- Amazon usage frequency cleanup
        case
            when amazon_use_frequency_raw ilike '%less than 5%' then '<5 per month'
            when amazon_use_frequency_raw ilike '%5%' and amazon_use_frequency_raw ilike '%10%' then '5–10 per month'
            when amazon_use_frequency_raw ilike '%more%' then '>10 per month'
            else 'Unknown'
        end as amazon_use_frequency,

        household_size

    from cleaned
)

select *
from standardized
