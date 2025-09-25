{{ config(materialized='view') }}

with base as (

    select
        d.deal_id,
        d.customer_id,
        d.plan_type,
        d.amount,
        d.deal_stage,
        cast(d.start_date as date) as start_date,
        cast(d.end_date as date) as end_date,
        cast(d.created_date as date) as created_date,
        cast(d.closed_date as date) as closed_date
    from {{ source('raw', 'MAIN_DEALS') }} d

),

joined as (

    select
        b.deal_id,
        b.customer_id,
        c.full_name,
        c.country,
        p.product_plan_id,
        p.product_plan,
        p.billing_cycle,
        p.price as subscription_fee,
        b.start_date,
        b.end_date,
        b.amount as deal_amount,
        b.deal_stage,
        b.created_date,
        b.closed_date,
        'CRM' as source_system,
        
        -- churn flag: lost deals or expired subscriptions
        case
            when lower(b.deal_stage) = 'lost' then 1
            else 0
        end as churn_flag,

        -- renewal count: number of times customer has had a deal
        count(b.deal_id) over (partition by b.customer_id) as renewal_count,

        -- row_number to dedupe
        row_number() over (
            partition by b.customer_id
            order by b.end_date desc nulls last
        ) as rn

    from base b
    left join {{ ref('int_customers') }} c
        on b.customer_id = c.customer_id
    left join {{ ref('stg_product_plans') }} p
        on lower(b.plan_type) = lower(p.product_plan)

),

deduped as (

    select *
    from joined
    where rn = 1   -- keep latest subscription per customer

)

select
    {{ generate_pk(['deal_id', 'customer_id', 'product_plan_id']) }} as subscription_id,
    customer_id,
    full_name,
    country,
    product_plan_id,
    product_plan,
    billing_cycle,
    subscription_fee,
    start_date,
    end_date,
    churn_flag,
    renewal_count,
    deal_stage,
    source_system
from deduped
