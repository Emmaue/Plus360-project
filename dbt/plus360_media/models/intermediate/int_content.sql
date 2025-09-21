{{ config(materialized='view') }}

-- Step 1: Dedup Newsletter
with newsletter as (
    select
        cast(newsletter_id as varchar) as content_id,
        date_sent as posted_date,
        newsletter_platform as platform,
        category,
        author,
        campaign_id,
        news_subject as title,
        tags,
        'Newsletter' as source_system
    from {{ ref('stg_newsletter') }}
    qualify row_number() over (partition by newsletter_id order by date_sent asc) = 1
),

-- Step 2: Dedup YouTube
youtube as (
    select
        cast(content_id as varchar) as content_id,
        upload_date as posted_date,
        'YouTube' as platform,
        category,
        channel_name as author,
        null as campaign_id,
        title,
        null as tags,
        'YouTube' as source_system
    from {{ ref('stg_youtube_content') }}
    qualify row_number() over (partition by content_id order by upload_date asc) = 1
),

-- Step 3: Content Types (static dimension style)
content_types as (
    select
        cast(content_type_id as varchar) as content_id,
        null as posted_date,
        'Internal' as platform,
        null as category,
        null as author,
        null as campaign_id,
        content_type as title,
        null as tags,
        'CRM' as source_system
    from {{ ref('stg_content_type') }}
),

-- Step 4: Union all into canonical content
unioned as (
    select * from newsletter
    union all
    select * from youtube
    union all
    select * from content_types
)

-- Step 5: Generate surrogate pk
select
    {{ generate_pk(['content_id', 'platform', 'source_system']) }} as content_pk,
    title,
    author,
    category,
    platform,
    posted_date,
    campaign_id,
    tags,
    source_system
from unioned
