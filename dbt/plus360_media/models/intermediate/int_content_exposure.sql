{{ config(materialized='view') }}

with newsletter_exposure as (

    select
        cast(n.newsletter_id as varchar) as content_id,
        null as customer_id,   -- exposure is newsletter sent, customer-level granularity may not exist yet
        'Newsletter' as platform,
        cast(n.date_sent as date) as exposure_date,
        'CRM' as source_system
    from {{ ref('stg_newsletter') }} n

),

youtube_exposure as (

    select
        cast(y.content_id as varchar) as content_id,
        null as customer_id,   -- YouTube doesn’t have customer_id in your staging
        'YouTube' as platform,
        cast(y.upload_date as date) as exposure_date,
        'YouTube' as source_system
    from {{ ref('stg_youtube_content') }} y

),

unioned as (

    select * from newsletter_exposure
    union all
    select * from youtube_exposure

)

select
    {{ generate_pk(['content_id','platform','exposure_date']) }} as exposure_id,
    content_id,
    customer_id,
    platform,
    exposure_date,
    source_system
from unioned
