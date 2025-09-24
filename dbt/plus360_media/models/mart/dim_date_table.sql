{{ config(materialized='table') }}

with base as (
  select
    cast(date_pk as int) as date_sk,
    date,
    day,
    month,
    year,
    quarter_of_year,

    -- stable weekday numbers: use DAYOFWEEK (0=Sun..6=Sat) AND ISO version
    dayofweek(date) as day_of_week_num,          -- default: 0=Sun..6=Sat
    dayofweekiso(date) as day_of_week_iso,       -- ISO: 1=Mon..7=Sun

    -- pretty names (trim to remove padding)
    initcap(trim(to_char(date, 'Day'))) as day_of_week_name,
    initcap(trim(to_char(date, 'Month'))) as month_name,

    -- pick a robust weekend flag (ISO-based, recommended)
    case when dayofweekiso(date) in (6,7) then true else false end as is_weekend

  from {{ ref('stg_date_dim') }}
)

select
  date_sk,
  date,
  day,
  month,
  year,
  quarter_of_year,
  day_of_week_num,
  day_of_week_iso,
  day_of_week_name,
  month_name,
  is_weekend
from base
