{{ config (
    materialized='incremental',
    unique_key='subscription_id',
    incremental_strategy='merge'
)}}

with base as (
    select
        subscription_id,
        customer_id,
        coalesce(full_name, 'n/a') as full_name,
        coalesce(country, 'No country') as country,
        coalesce(product_plan_id, 'No id')product_plan_id,
        coalesce(product_plan, 'No plan') as product_plan,
        coalesce(billing_cycle, 'No cycle') as billing_cycle,
        coalesce(subscription_fee, 0) as subscription_fee,
        start_date,
        end_date,
        churn_flag,
        renewal_count,
        deal_stage,
        source_system
    from {{ ref('int_subscription_periods') }}
)

select * from base
