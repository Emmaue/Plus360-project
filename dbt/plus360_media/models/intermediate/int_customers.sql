{{ config(materialized ='view') }}

with base as (
SELECT
    CUSTOMER_ID,
    concat(FIRST_NAME, ' ', LAST_NAME) AS full_name,
    FIRST_VALUE(phone) OVER (PARTITION BY customer_id ORDER BY signup_date) AS phone,
    signup_date,
    country,
    'crm' as source_system,
    row_number() over (partition by customer_id order by signup_date desc) as rn,
    customer_status
FROM
    {{ ref('stg_customers') }}
),

deduped as(
    Select
    CUSTOMER_ID,
    initcap(trim(full_name)) as full_name,
    phone,
    CASE
        WHEN signup_date < to_date('1900-01-01') or signup_date is null THEN to_date('1900-01-01')
        ELSE signup_date
    END AS signup_date,
    country,
    source_system,
    rn,
    customer_status
FROM base
WHERE rn = 1
)

Select
    CUSTOMER_ID,
    full_name,
    phone,
    signup_date,
    country,
    source_system,
    customer_status
from deduped

