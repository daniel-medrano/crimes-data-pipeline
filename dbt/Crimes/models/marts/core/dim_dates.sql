with stg_crimes as (
    select * from {{ ref('stg_crimes') }}
),
dates as (
    select distinct
        concat(year(crime_date),
            case
                when length(month(crime_date)) = 1 then concat('0', month(crime_date))
                else to_varchar(month(crime_date))
            end,
            case 
                when length(day(crime_date)) = 1 then concat('0', day(crime_date))
                else to_varchar(day(crime_date))
            end) as date_id,
        crime_date as date,
        dayofweek(crime_date) as day_of_week,
        month(crime_date) as month,
        case month
            when 1 then 'ENERO'
            when 2 then 'FEBRERO'
            when 3 then 'MARZO'
            when 4 then 'ABRIL'
            when 5 then 'MAYO'
            when 6 then 'JUNIO'
            when 7 then 'JULIO'
            when 8 then 'AGOSTO'
            when 9 then 'SEPTIEMBRE'
            when 10 then 'OCTUBRE'
            when 11 then 'NOVIEMBRE'
            when 12 then 'DICIEMBRE'
        end as month_name,
        year(crime_date) as year,
        quarter(crime_date) as trimester,
        case
            when month >= 1 and month <= 4 then 1
            when month >= 5 and month <= 8 then 2
            when month >= 9 and month <= 12 then 3
        end as quarter,
        case 
            when month >= 1 and month <= 6 then 1
            when month >= 7 and month <= 12 then 2
        end as semester
    from stg_crimes
)
select *
from dates