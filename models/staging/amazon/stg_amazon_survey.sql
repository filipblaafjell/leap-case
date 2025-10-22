{{ config(materialized='view') }}

with cleaned as (
    select
        survey_response_id                                    as user_id,
        nullif(trim(q_demos_age), '')                         as age_group,
        initcap(nullif(trim(q_demos_hispanic), ''))           as hispanic_origin,

        case
            when lower(trim(q_demos_race)) like '%black or african american,american indian/native american or alaska native%'
                then 'Other'
            else initcap(nullif(trim(q_demos_race), ''))
        end as race,
        
        initcap(nullif(trim(regexp_replace(q_demos_education, '\s*\(.*\)', '')), '')) as education_level,

        nullif(trim(q_demos_income), '')                      as income_bracket,
        initcap(nullif(trim(q_demos_gender), ''))             as gender,

        initcap(nullif(trim(regexp_replace(q_sexual_orientation, '\s*\(.*\)', '')), '')) as sexual_orientation,
        
        upper(trim(q_demos_state))                            as state,

        nullif(trim(regexp_replace(q_household_size, '\s*\(.*\)', '')), '') as household_size,

        nullif(trim(regexp_replace(q_amazon_use_howmany, '\s*\(.*\)', '')), '') as amazon_use_devices,

        nullif(trim(q_amazon_use_how_oft), '')                as amazon_use_frequency,
        initcap(nullif(trim(q_substance_use_cigarettes), '')) as uses_cigarettes,
        initcap(nullif(trim(q_substance_use_marijuana), ''))  as uses_marijuana,
        initcap(nullif(trim(q_substance_use_alcohol), ''))    as uses_alcohol,
        initcap(nullif(trim(q_personal_diabetes), ''))        as has_diabetes,
        initcap(nullif(trim(q_personal_wheelchair), ''))      as uses_wheelchair,
        nullif(trim(q_life_changes), '')                      as life_changes,
        initcap(nullif(trim(q_sell_your_data), ''))           as willing_to_sell_own_data,
        initcap(nullif(trim(q_sell_consumer_data), ''))       as willing_to_sell_consumer_data,
        initcap(nullif(trim(q_small_biz_use), ''))            as small_biz_use_case,
        initcap(nullif(trim(q_census_use), ''))               as census_data_use,
        initcap(nullif(trim(q_research_society), ''))         as research_society_use
    from {{ source('raw_amazon', 'AMAZON_SURVEY_RAW') }}
)

select
    user_id,
    age_group,
    hispanic_origin,
    race,
    education_level,
    income_bracket,
    gender,
    sexual_orientation,
    state,
    household_size,
    amazon_use_devices,
    amazon_use_frequency,
    uses_cigarettes,
    uses_marijuana,
    uses_alcohol,
    has_diabetes,
    uses_wheelchair,
    life_changes,
    willing_to_sell_own_data,
    willing_to_sell_consumer_data,
    small_biz_use_case,
    census_data_use,
    research_society_use
from cleaned
