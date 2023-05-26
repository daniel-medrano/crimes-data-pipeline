with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
geographics as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['crime_district', 'crime_canton', 'crime_province']) }} as geographics_id,
        crime_district,
        crime_canton,
        crime_province
    from stg_crimes
),
select *
from geographics