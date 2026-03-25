/*
    High-level summary statistics per page type.
    Provides an overview of impression volume, user engagement, and funnel depth.
*/
with aggregated as (
    select * from {{ ref('int_impressions_aggregated') }}
),

summary as (
    select
        page_type,
        count(distinct impression_id) as total_impressions,
        count(distinct user_id) as unique_users,
        round(avg(distinct_events), 2) as avg_funnel_depth,
        max(distinct_events) as max_funnel_depth,
        round(avg(last_event_second - first_event_second), 2) as avg_duration_seconds,
        sum(case when max_event_reached >= 'd' then 1 else 0 end) as impressions_reaching_d,
        sum(case when max_event_reached >= 'e' then 1 else 0 end) as impressions_reaching_e,
        sum(case when max_event_reached >= 'f' then 1 else 0 end) as impressions_reaching_f,
        round(
            sum(case when max_event_reached >= 'd' then 1 else 0 end)::numeric
            / count(distinct impression_id) * 100, 2
        ) as pct_reaching_d,
        round(
            sum(case when max_event_reached >= 'e' then 1 else 0 end)::numeric
            / count(distinct impression_id) * 100, 2
        ) as pct_reaching_e,
        round(
            sum(case when max_event_reached >= 'f' then 1 else 0 end)::numeric
            / count(distinct impression_id) * 100, 2
        ) as pct_reaching_f
    from aggregated
    group by page_type
)

select * from summary
order by page_type
