{{ config(materialized= 'view')}}

with customer as(
Select
    {{ generate_pk(['customer_id']) }} as Customer_id,
    coalesce(company, 'N/A') as company,
    coalesce(country, 'N/A') as country,
    coalesce(city, 'N/A') as city,
    cast(signup_date as date) as signup_date,
    coalesce(customer_status, 'Unknown') as customer_status
from {{ source('raw', 'CONTACTS_CLEAN') }}
)

Select
    customer_id,
    company,
    country,
    city,
    signup_date,
    customer_status
from customer