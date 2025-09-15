{{ config(materialized='view') }}

with Youtube_engagement as (
    select
        {{ preserve_pk('ENGAGEMENT_ID') }} as Engagement_id,
        coalesce(CONTENT_ID, 0) as Content_id,
        coalesce(CUSTOMER_ID, 0) as Customer_id,
        coalesce(WATCH_TIME_SECONDS, 0) as Watch_time_seconds,
        coalesce(LIKES, 0) as Likes,
        coalesce(COMMENTS, 0) as Comments,
        coalesce(SHARES, 0) as Shares,
        coalesce(ENGAGEMENT_DATE, cast(null as date)) as Engagement_date
    from {{ source('raw', 'YOUTUBE_ENGAGEMENT_RAW') }}
)

select
    Engagement_id,
    Content_id,
    Customer_id,
    Watch_time_seconds,
    Likes,
    Comments,
    Shares,
    Engagement_date
from Youtube_engagement