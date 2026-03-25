/*
    User-level engagement metrics across all page types.
    Identifies user activity patterns and most-engaged page type.
*/
with aggregated as (
    select * from {{ ref('int_impressions_aggregated') }}
),

user_metrics as (
    select
        user_id,
        count(distinct impression_id) as total_impressions,
        count(distinct page_type) as page_types_visited,
        round(avg(distinct_events), 2) as avg_funnel_depth,
        max(distinct_events) as max_funnel_depth,
        sum(event_count) as total_events,
        min(first_event_at) as first_seen,
        max(last_event_at) as last_seen
    from aggregated
    group by user_id
),

most_engaged_page as (
    select distinct on (user_id)
        user_id,
        page_type as most_engaged_page_type,
        count(*) as impressions_on_page
    from aggregated
    group by user_id, page_type
    order by user_id, count(*) desc
)

select
    um.*,
    mep.most_engaged_page_type,
    mep.impressions_on_page as impressions_on_top_page
from user_metrics um
join most_engaged_page mep on um.user_id = mep.user_id
order by um.total_impressions desc
