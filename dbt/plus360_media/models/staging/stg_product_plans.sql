{{ config(materialized ='view') }}

with product_plan as (
    Select
    {{ preserve_pk('Product_plan_id') }} as product_plan_id,
    coalesce(cast(Product_plan as varchar), 'No plan') as Product_plan,
    coalesce(cast(Billing_cycle as varchar), 'N/A') as Billing_cycle,
    coalesce(cast(Price as int), 'N/A') as Price
From {{source('raw','PRODUCT_PLANS')}}

)

Select
    Product_plan_id,
    Product_plan,
    Billing_cycle,
    Price
From Product_plan



