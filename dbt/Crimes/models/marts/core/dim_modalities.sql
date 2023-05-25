with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
modalities as (
    select distinct 
        {{ dbt_utils.generate_surrogate_key(['crime_modality', 'crime_category']) }} as modality_id,
        crime_modality as modality,
        crime_category as category
    from stg_crimes
)
select * 
from modalities

