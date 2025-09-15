{{ config(materialized='view') }}

with Youtube_content as (
    select
        {{ preserve_pk('CONTENT_ID') }} as Content_id,
        coalesce(CHANNEL_NAME, 'Unknown Channel') as Channel_name,
        coalesce(TITLE, 'Untitled') as Title,
        coalesce(UPLOAD_DATE, cast(null as date)) as Upload_date,
        coalesce(DURATION_SECONDS, 0) as Duration_seconds,
        coalesce(CATEGORY, 'Uncategorized') as Category
    from pulse360_db.raw_schema.YOUTUBE_CONTENT_RAW
)
select
    Content_id,
    Channel_name,
    Title,
    Upload_date,
    Duration_seconds,
    Category
from Youtube_content