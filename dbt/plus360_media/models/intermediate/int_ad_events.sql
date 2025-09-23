{{ config(materialized='view') }}

with ads as (
    select
        cast(ad_id as varchar) as ad_id,
        cast(yt_content_id as varchar) as content_id,
        ad_type,
        revenue_amount,
        currency,
        cast(revenue_date as date) as revenue_date,
        'YouTube_Ads' as source_system
    from {{ ref('stg_youtube_ad') }}
)

select
    {{ generate_pk(['ad_id','content_id','revenue_date']) }} as ad_event_pk,
    ad_id,
    content_id,
    ad_type,
    revenue_amount,
    currency,
    revenue_date,
    to_char(revenue_date, 'YYYYMMDD')::int as date_key,
    source_system
from ads
