{{config (materialized ='view') }}

with platform as(
    Select
        {{ preserve_pk('Platform_id') }} as Platform_id,
        coalesce(cast(Platform as varchar), 'Unknown platform') as Platform
    From {{ source('raw','PLATFORMS') }}
)

Select
    Platform_id,
    platform
from platform