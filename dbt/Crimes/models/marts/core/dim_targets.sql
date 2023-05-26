with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
targets as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['target_type', 'target_description']) }} as target_id
        target_type,
        target_description
    from stg_crimes
)
select *
from targets