{{ config(materialized='view') }}

-- 1. Newsletter engagement (aggregate by newsletter + date)
with newsletter as (
    select
        cast(newsletter_id as varchar) as content_id,
        null as customer_id,  -- no customer info at source
        'Newsletter' as platform,
        cast(date_sent as date) as event_date,
        sum(opens) as opens_count,
        sum(clicks) as clicks_count,
        null as likes_count,
        null as comments_count,
        null as shares_count,
        null as watch_time_seconds,
        'CRM_Newsletter' as source_system
    from {{ ref('stg_newsletter') }}
    group by 1,2,3,4,10
),

-- 2. YouTube engagement (aggregate by customer–content–date)
youtube as (
    select
        cast(content_id as varchar) as content_id,
        cast(customer_id as varchar) as customer_id,
        'YouTube' as platform,
        cast(engagement_date as date) as event_date,
        sum(case when likes = 1 then 1 else 0 end) as likes_count,
        sum(case when comments = 1 then 1 else 0 end) as comments_count,
        sum(case when shares = 1 then 1 else 0 end) as shares_count,
        null as opens_count,
        null as clicks_count,
        sum(watch_time_seconds) as watch_time_seconds,
        'YouTube_API' as source_system
    from {{ ref('stg_youtube_engagement') }}
    group by 1,2,3,4,11
),

-- 3. Union
unioned as (
    select * from newsletter
    union all
    select * from youtube
)

-- 4. Final output (one row per content_id–customer_id–event_date)
select
    {{ generate_pk(['content_id','customer_id','event_date','platform']) }} as engagement_pk,
    content_id,
    customer_id,
    platform,
    event_date,
    coalesce(opens_count, 0) as opens_count,
    coalesce(clicks_count, 0) as clicks_count,
    coalesce(likes_count, 0) as likes_count,
    coalesce(comments_count, 0) as comments_count,
    coalesce(shares_count, 0) as shares_count,
    coalesce(watch_time_seconds, 0) as watch_time_seconds,
    to_char(event_date, 'YYYYMMDD')::int as date_key,
    source_system
from unioned
