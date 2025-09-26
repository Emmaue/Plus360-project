{{ config(
    materialized='incremental',
    unique_key='ad_event_pk',
    incremental_strategy='merge'
) }}

with base as (
    select
        a.ad_event_pk as ad_event_pk,
        c.content_pk as content_pk,
        c.content_id as content_id,
        d.date_sk as date_sk,
        a.ad_id as ad_id,
        a.ad_type as ad_type,
        a.revenue_amount as revenue_amount,
        a.currency as currency,
        a.revenue_date as revenue_date,
        a.source_system as source_system

    from {{ ref('int_ad_events') }} a
    left join {{ ref('dim_content_table') }}  c
        on a.content_id = c.content_id
    left join {{ ref('dim_date_table') }} d
        on a.date_key = d.date_sk




    {% if is_incremental() %}
        where a.revenue_date > (select max(revenue_date) from {{ this }})
    {% endif %}
)

Select
    ad_event_pk,
    content_pk,
    content_id,
    date_sk,
    ad_id,
    ad_type,
    coalesce(revenue_amount,0) as revenue_amount,
    coalesce(currency, 'USD') as currency,
    revenue_date,
    source_system
from base

