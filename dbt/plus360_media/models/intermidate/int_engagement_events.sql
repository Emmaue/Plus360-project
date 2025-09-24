{{ config(materialized='view') }}

-- 1. Newsletter engagement (aggregated opens & clicks)
with newsletter as (
    -- Opens
    select
        cast(newsletter_id as varchar) as content_id,
        null as customer_id,  -- aggregate, no customer info
        'Newsletter' as platform,
        'open' as event_type,
        opens as engagement_count,
        null as watch_time_seconds,
        cast(date_sent as date) as event_date,
        cast(date_sent as timestamp) as event_time,
        'CRM_Newsletter' as source_system
    from {{ ref('stg_newsletter') }}

    union all

    -- Clicks
    select
        cast(newsletter_id as varchar) as content_id,
        null as customer_id,
        'Newsletter' as platform,
        'click' as event_type,
        clicks as engagement_count,
        null as watch_time_seconds,
        cast(date_sent as date) as event_date,
        cast(date_sent as timestamp) as event_time,
        'CRM_Newsletter' as source_system
    from {{ ref('stg_newsletter') }}
),

-- 2. YouTube engagement (event-level logs)
youtube as (
    select
        cast(content_id as varchar) as content_id,
        cast(customer_id as varchar) as customer_id,
        'YouTube' as platform,
        case
            when likes = 1 then 'like'
            when comments = 1 then 'comment'
            when shares = 1 then 'share'
            else 'unknown'
        end as event_type,
        1 as engagement_count,  -- one row per event
        watch_time_seconds,
        cast(ENGAGEMENT_DATE as date) as event_date,
        cast(ENGAGEMENT_DATE as timestamp) as event_time,
        'YouTube_API' as source_system
    from {{ ref('stg_youtube_engagement') }}
),

-- 3. Union newsletter + YouTube
unioned as (
    select * from newsletter
    union all
    select * from youtube
)

-- 4. Final cleaned output
select
    {{ generate_pk(['content_id','customer_id','event_time','event_type']) }} as event_pk,
    content_id,
    customer_id,
    platform,
    event_type,
    engagement_count,
    watch_time_seconds,
    event_date,
    to_char(event_date, 'YYYYMMDD')::int as date_key,
    to_char(event_time, 'HH24MISS')::int as time_key,
    event_time,
    source_system
from unioned
