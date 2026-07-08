-- High-level summary statistics per page type (ClickHouse port of the dbt
-- page_type_summary model). First reproduces int_impressions_aggregated by
-- collapsing raw events to one row per impression, then summarises per page.
WITH
    aggregated AS (
        SELECT
            user_id,
            impression_id,
            page_type,
            date AS event_date,
            hour,
            count(*) AS event_count,
            uniqExact(event_type) AS distinct_events,
            min(second) AS first_event_second,
            max(second) AS last_event_second,
            max(event_type) AS max_event_reached
        FROM {table}
        GROUP BY user_id, impression_id, page_type, date, hour
    )
SELECT
    page_type,
    uniqExact(impression_id) AS total_impressions,
    uniqExact(user_id) AS unique_users,
    round(avg(distinct_events), 2) AS avg_funnel_depth,
    max(distinct_events) AS max_funnel_depth,
    round(avg(last_event_second - first_event_second), 2) AS avg_duration_seconds,
    sum(max_event_reached >= 'd') AS impressions_reaching_d,
    sum(max_event_reached >= 'e') AS impressions_reaching_e,
    sum(max_event_reached >= 'f') AS impressions_reaching_f,
    round(sum(max_event_reached >= 'd') / uniqExact(impression_id) * 100, 2) AS pct_reaching_d,
    round(sum(max_event_reached >= 'e') / uniqExact(impression_id) * 100, 2) AS pct_reaching_e,
    round(sum(max_event_reached >= 'f') / uniqExact(impression_id) * 100, 2) AS pct_reaching_f
FROM aggregated
GROUP BY page_type
ORDER BY page_type;
