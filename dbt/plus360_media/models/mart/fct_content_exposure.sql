{{ config (
    materialized='incremental',
    unique_key='exposure_id',
    incremental_strategy='merge'
)}}

with base as (
    select
        exposure_id,
        content_id,
        customer_id,
        platform,
        exposure_date,
        source_system
    from {{ ref('int_content_exposure') }}
)

select * from base
