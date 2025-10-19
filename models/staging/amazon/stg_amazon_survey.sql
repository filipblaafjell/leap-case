{{ config(materialized='view') }}

SELECT
    survey_response_id              AS user_id,
    q_demos_age                     AS age_group,
    q_demos_hispanic                AS hispanic_origin,
    q_demos_race                    AS race,
    q_demos_education               AS education_level,
    q_demos_income                  AS income_bracket,
    q_demos_gender                  AS gender,
    q_sexual_orientation            AS sexual_orientation,
    q_demos_state                   AS state,
    q_amazon_use_howmany            AS amazon_use_devices,
    q_amazon_use_hh_size            AS household_size,
    q_amazon_use_how_oft            AS amazon_use_frequency,
    q_substance_use_cigarettes      AS uses_cigarettes,
    q_substance_use_marijuana       AS uses_marijuana,
    q_substance_use_alcohol         AS uses_alcohol,
    q_personal_diabetes             AS has_diabetes,
    q_personal_wheelchair           AS uses_wheelchair,
    q_life_changes                  AS life_changes,
    q_sell_your_data                AS willing_to_sell_own_data,
    q_sell_consumer_data            AS willing_to_sell_consumer_data,
    q_small_biz_use                 AS small_biz_use_case,
    q_census_use                    AS census_data_use,
    q_research_society              AS research_society_use
FROM {{ source('raw_amazon', 'AMAZON_SURVEY_RAW') }}
WHERE survey_response_id IS NOT NULL
