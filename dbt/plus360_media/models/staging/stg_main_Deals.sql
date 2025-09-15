{{ config(materialized ='view') }}

with main as(
    Select
        {{ preserve_pk('DEAL_ID') }} as Deal_id,
        Customer_id,
        coalesce(cast(Deal_name as Varchar), 'Unknown') as Deal_name,
        coalesce(cast(Amount as decimal), 0) as Amount,
        coalesce(cast(Deal_stage as Varchar), 'N/A') as Deal_stage,
        coalesce(cast(start_date as date), cast(null as date)) as start_date,
        coalesce(cast(End_date as date), cast(null as date)) as End_date,
        coalesce(cast(Plan_type as varchar), 'N/A') as Plan_type,
        coalesce(cast(Created_date as date), cast(null as date)) as Created_date,
        coalesce(cast(Closed_date as date), cast(null as date)) as Closed_date
    From {{ source('raw','MAIN_DEALS') }}
)

Select
    Deal_id,
    Customer_id,
    Deal_name,
    Amount,
    Deal_stage,
    Start_date,
    End_date,
    Plan_type,
    Created_date,
    Closed_date
From main
