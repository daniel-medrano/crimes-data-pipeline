with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
victim_demographics as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['victim_age_category', 'victim_nationality']) }} as demographics_id,
        victim_age_category as age_category,
        victim_nationality as nationality
    from stg_crimes
)
select *
from victim_demographics