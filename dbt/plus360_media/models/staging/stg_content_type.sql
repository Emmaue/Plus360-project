{{ config(materialized='view') }}

with content as(
    Select
        {{ preserve_pk('Content_id') }} as Content_type_id,
        coalesce(TYPE, 'Unknown') as Content_type
    FROM {{ source('raw', 'CONTENT_TYPE')}}
)

Select
    Content_type_id,
    content_type
from content