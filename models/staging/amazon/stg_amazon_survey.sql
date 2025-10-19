{{ config(materialized='view') }}

with cleaned as (
    select
        survey_response_id                  as user_id,
        trim(q_demos_age)                   as age_group,
        initcap(trim(q_demos_hispanic))     as hispanic_origin,
        initcap(trim(q_demos_race))         as race,
        initcap(trim(q_demos_education))    as education_level,
        trim(q_demos_income)                as income_bracket,
        initcap(trim(q_demos_gender))       as gender,
        initcap(trim(q_sexual_orientation)) as sexual_orientation,
        upper(trim(q_demos_state))          as state,

        -- Clean household_size (fixing your failing test)
        case
            when regexp_like(q_amazon_use_hh_size, '^[0-9]+$') then try_cast(q_amazon_use_hh_size as int)
            when q_amazon_use_hh_size ilike '4%' then 4
            else null
        end as household_size,

        trim(q_amazon_use_howmany)                as amazon_use_devices,
        trim(q_amazon_use_how_oft)                as amazon_use_frequency,
        initcap(trim(q_substance_use_cigarettes)) as uses_cigarettes,
        initcap(trim(q_substance_use_marijuana))  as uses_marijuana,
        initcap(trim(q_substance_use_alcohol))    as uses_alcohol,
        initcap(trim(q_personal_diabetes))        as has_diabetes,
        initcap(trim(q_personal_wheelchair))      as uses_wheelchair,
        trim(q_life_changes)                      as life_changes,
        initcap(trim(q_sell_your_data))           as willing_to_sell_own_data,
        initcap(trim(q_sell_consumer_data))       as willing_to_sell_consumer_data,
        initcap(trim(q_small_biz_use))            as small_biz_use_case,
        initcap(trim(q_census_use))               as census_data_use,
        initcap(trim(q_research_society))         as research_society_use
    from {{ source('raw_amazon', 'AMAZON_SURVEY_RAW') }}
    where survey_response_id is not null
)

select * from cleaned