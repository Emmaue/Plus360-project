{{ config(materialized='table') }}

with base as (
    select
        cast(time_key as int) as time_sk,
        hour,
        minute,
        second,
        am_pm,
        initcap(trim(partofday)) as time_of_day,

        -- derive 24-hour format
        case 
            when upper(am_pm) = 'PM' and hour < 12 then hour + 12
            when upper(am_pm) = 'AM' and hour = 12 then 0
            else hour
        end as hour_24,

        -- business hours: 9am - 5pm (09:00 to 17:00 inclusive)
        case 
            when (
                (upper(am_pm) = 'AM' and hour between 9 and 11)
                or (upper(am_pm) = 'PM' and hour between 12 and 5)
            ) then true 
            else false 
        end as is_business_hour
    from pulse360_db.raw_schema.time_dim
)

select
    time_sk,
    hour,
    minute,
    second,
    am_pm,
    time_of_day,
    hour_24,
    is_business_hour
from base
