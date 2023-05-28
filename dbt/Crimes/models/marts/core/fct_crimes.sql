{{ 
    config(
        materialized='incremental'
        -- TODO: add unique_key attribute in order to rebuild only a subset of the data that had already been loaded
    ) 
}}

with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
dim_modalities as (
    select * from {{ ref('dim_modalities') }}
),
dim_dates as (
    select * from {{ ref('dim_dates') }}
),
dim_targets as (
    select * from {{ ref('dim_targets') }}
),
dim_victim_demographics as (
    select * from {{ ref('dim_victim_demographics') }}
),
dim_geographics as (
    select * from {{ ref('dim_geographics') }}
),
crimes as (
    select
        modality_id,
        date_id,
        target_id,
        demographics_id,
        geographics_id
    from stg_crimes c
    inner join dim_modalities m
        on c.crime_category = m.category
        and c.crime_modality = m.modality
    inner join dim_dates d
        on c.crime_date = d.date
    inner join  dim_targets t
        on c.target_type = t.target_type
        and c.target_description = t.target_description
    inner join dim_victim_demographics v
        on c.victim_age_category = v.age_category
        and c.victim_nationality = v.nationality
    inner join dim_geographics g
        on c.crime_province = g.province
        and c.crime_canton = g.canton
        and c.crime_district = g.district

    {% if is_incremental() %}
        where date_id > (select max(date_id) from {{ this }})
    {% endif %}
)
select *
from crimes
