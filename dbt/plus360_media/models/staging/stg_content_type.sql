{{ config(materialized='view') }}

with content as(
    Select
        {{ generate_pk(['CONTENT_ID']) }} as Content_id,
        coalesce(TYPE, 'Unknown') as Content_type
    FROM {{ source('raw', 'CONTENT_TYPE')}}
)

Select
    Content_id,
    content_type
from content