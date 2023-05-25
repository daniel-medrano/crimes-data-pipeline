with source as (
    select * from {{ source('raw', 'crimes') }}
),
not_mismatched as (
    select
        col1 as crime_category,
        col2 as crime_modality,
        to_date(col3) as crime_date,
        col4 as target_type,
        left(col5, charindex('[', col5) - 1) as target_description, 
        trim(upper(col6)) as victim_age_category,
        upper(col7) as victim_nationality,
        col8 as crime_province,
        col9 as crime_canton,
        col10 as crime_district
    from source
    where col11 is null
),
mismatched as (
    select
        col1 as crime_category,
        col2 as crime_modality,
        to_date(col3) as crime_date,
        col4 as target_type,
        left(col5, charindex('[', col5) - 1) as target_description, 
        trim(upper(col6)) as victim_age_category,
        concat(col8, ' ', col7) as victim_nationality,
        col9 as crime_province,
        col10 as crime_canton,
        col11 as crime_district
    from source
    where col11 is not null
),
stage as (
    select * 
    from not_mismatched
    union all
    select * 
    from mismatched
)

select *
from stage