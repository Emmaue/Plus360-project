{{ config(materialized = 'view') }}

with news as (
    select
        {{ preserve_pk('Newsletter_id') }} as Newsletter_id,
        Date_Sent,
        coalesce(Content_id, 'N/A') as Newsletter_content_id,
        coalesce(Platform, 'No platform') as Newsletter_platform, 
        coalesce(Category, 'No category') as Category,
        coalesce(Author, 'No author') as Author, 
        coalesce(Campaign_id, 'N/A') as Campaign_id,
        coalesce(Subject, 'No subject') as News_Subject,

        coalesce(try_cast(Recipients as int), 0) as Recipients,
        coalesce(try_cast(Open_rate as number(38,4)), 0) as Open_rate,
        coalesce(try_cast(Open_rate_pct as number(38,2)), 0) as Open_rate_pct,
        coalesce(try_cast(Click_rate as number(38,4)), 0) as Click_rate, 
        coalesce(try_cast(Click_rate_pct as number(38,2)), 0) as Click_rate_pct,

        coalesce(try_cast(Opens as int), 0) as Opens,
        coalesce(try_cast(Clicks as int), 0) as Clicks,
        coalesce(try_cast(Unsubscribe_count as int), 0) as Unsubscribe_count,
        coalesce(Tags, 'No tag') as Tags

    from Pulse360_db.raw_schema.Newsletter
)

select
    Newsletter_id,
    Date_Sent,
    Newsletter_content_id,
    Newsletter_platform,
    Category,
    Author,
    Campaign_id,
    News_Subject,
    Recipients,
    Open_rate,
    Open_rate_pct,
    Click_rate,
    Click_rate_pct,
    Opens,
    Clicks,
    Unsubscribe_count,
    Tags
from news
