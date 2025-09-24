-- config as table from int_content to allow for easier joins in BI tools
{{ config(materialized='table') }}

with base as (
    select
        content_pk,
        coalesce(title, 'Unknown') as title,
        coalesce(author, 'Unknown') as author,
        coalesce(category, 'Unknown') as category,
        coalesce(platform,'unknown') as platform,
        posted_date,
        campaign_id,
        tags,
        source_system
    from {{ ref('int_content') }}
)

select
    content_pk,
    title,
    author,
    category,
    platform,
    posted_date,
    campaign_id,
    tags,
    source_system
from base


