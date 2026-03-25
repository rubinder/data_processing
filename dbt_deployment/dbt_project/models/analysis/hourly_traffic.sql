/*
    Hourly traffic patterns by page type.
    Shows impression volume and engagement over time.
*/
with aggregated as (
    select * from {{ ref('int_impressions_aggregated') }}
),

hourly as (
    select
        event_date,
        hour,
        page_type,
        count(distinct impression_id) as total_impressions,
        count(distinct user_id) as unique_users,
        round(avg(distinct_events), 2) as avg_funnel_depth,
        sum(case when max_event_reached >= 'd' then 1 else 0 end) as impressions_reaching_d,
        round(
            sum(case when max_event_reached >= 'd' then 1 else 0 end)::numeric
            / nullif(count(distinct impression_id), 0) * 100, 2
        ) as pct_reaching_d
    from aggregated
    group by event_date, hour, page_type
)

select * from hourly
order by event_date, hour, page_type
