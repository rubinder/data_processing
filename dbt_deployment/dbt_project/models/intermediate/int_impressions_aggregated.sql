/*
    Aggregates impression events by user, impression, and page type.
    Mirrors the spark aggregation job in spark_applications/aggregation.py.
*/
with impressions as (
    select * from {{ ref('stg_impressions') }}
),

aggregated as (
    select
        user_id,
        impression_id,
        page_type,
        event_date,
        hour,
        count(*) as event_count,
        count(distinct event_type) as distinct_events,
        min(second) as first_event_second,
        max(second) as last_event_second,
        min(event_timestamp) as first_event_at,
        max(event_timestamp) as last_event_at,
        max(event_type) as max_event_reached
    from impressions
    group by user_id, impression_id, page_type, event_date, hour
)

select * from aggregated
