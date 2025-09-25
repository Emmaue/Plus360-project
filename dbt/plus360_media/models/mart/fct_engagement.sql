{{ config(
    materialized='incremental',
    unique_key='engagement_pk',
    incremental_strategy='merge'
) }}

with base as (
    select
        e.engagement_pk,
        c.content_pk,
        c.content_id,
        cust.customer_sk,
        d.date_sk,
        e.platform,
        e.event_date,
        e.source_system,

        -- metrics
        e.opens_count,
        e.clicks_count,
        e.likes_count,
        e.comments_count,
        e.shares_count,
        e.watch_time_seconds

    from {{ ref('int_engagement_events') }} e
    left join {{ ref('dim_content_table') }}  c
        on e.content_id = c.content_id
    left join {{ ref('dim_customers') }} cust
        on e.customer_id = cust.customer_id
    left join {{ ref('dim_date_table') }} d
        on e.date_key = d.date_sk

    {% if is_incremental() %}
        where e.event_date > (select max(event_date) from {{ this }})
    {% endif %}
)


select * from base
