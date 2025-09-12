{{ config(materialized='view') }}

with date_dim as (
    select
        {{ generate_pk(['date_key']) }} as date_pk,
        coalesce(cast(date as date), to_date('1900-01-01')) as date,
        coalesce(cast(day as varchar), 'Unknown') as day,
        coalesce(cast(month as varchar), 'Unknown') as month,
        coalesce(cast(year as int), 0) as year,
        coalesce(cast(quarter_of_year as varchar), 'Unknown') as quarter_of_year
    from pulse360_db.raw_schema.DATE_DIM  -- Direct reference instead of source()
)

select
    date_pk,
    date,
    day,
    month,
    year,
    quarter_of_year
from date_dim