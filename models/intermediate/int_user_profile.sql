{{ config(materialized='view') }}

with base as (
    select
        user_id,
        initcap(gender)            as gender,
        initcap(age_group)         as age_group,
        initcap(income_bracket)    as income_bracket,
        initcap(education_level)   as education_level,
        initcap(race)              as race,
        initcap(hispanic_origin)   as hispanic_origin,
        initcap(sexual_orientation) as sexual_orientation,
        household_size,
        amazon_use_devices,
        amazon_use_frequency,
        initcap(uses_cigarettes)   as uses_cigarettes,
        initcap(uses_marijuana)    as uses_marijuana,
        initcap(uses_alcohol)      as uses_alcohol,
        initcap(has_diabetes)      as has_diabetes,
        initcap(uses_wheelchair)   as uses_wheelchair
    from {{ ref('stg_amazon_survey') }}
)

select
    user_id,
    gender,
    age_group,
    income_bracket,
    education_level,
    race,
    hispanic_origin,
    sexual_orientation,
    household_size,
    amazon_use_devices,
    amazon_use_frequency,
    uses_cigarettes,
    uses_marijuana,
    uses_alcohol,
    has_diabetes,
    uses_wheelchair
from base
