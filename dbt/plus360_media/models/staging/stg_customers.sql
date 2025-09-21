{{ config(materialized= 'view')}}

with customer as(
Select
    {{ preserve_pk('customer_id') }} as Customer_id,
    First_name,
    Last_name,
    phone,
    coalesce(company, 'N/A') as company,
    coalesce(country, 'N/A') as country,
    coalesce(city, 'N/A') as city,
    cast(signup_date as date) as signup_date,
    coalesce(customer_status, 'No status') as customer_status
from {{ source('raw', 'CONTACTS_CLEAN') }}
)

Select
    customer_id,
    First_name,
    Last_name,
    phone,
    company,
    country,
    city,
    signup_date,
    customer_status
from customer