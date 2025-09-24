{{ config(materialized='table') }}

with base as (
    select
        {{ generate_surrogate_key(['customer_id', 'signup_date']) }} as customer_sk,
        customer_id,
        signup_date,
        coalesce(country, 'Unknown') as country,
        coalesce(source_system, 'Unknown') as source_system,
        coalesce(customer_status, 'Unknown') as customer_status
    from {{ ref('int_customers') }}
)

select
    customer_sk,
    customer_id,
    signup_date,
    country,
    source_system,
    customer_status
from base
