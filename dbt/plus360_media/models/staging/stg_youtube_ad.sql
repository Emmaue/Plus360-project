{{ config(materialized='view') }}

with Youtube_ad as (
    select
        {{ preserve_pk('AD_ID') }} as Ad_id,
        coalesce(Content_id, 0) as Yt_content_id,
        coalesce(Ad_type, 'Unknown') as Ad_type,
        coalesce(Revenue_amount, 0.00) as Revenue_amount,
        currency,
        coalesce(Revenue_date, cast(null as date)) as Revenue_date
    from {{ source('raw', 'YOUTUBE_AD_REVENUE_RAW') }}
)

select
    Ad_id,
    Yt_content_id,
    Ad_type,
    Revenue_amount,
    currency,
    Revenue_date
from Youtube_ad