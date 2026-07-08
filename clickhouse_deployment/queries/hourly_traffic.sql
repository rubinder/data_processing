-- Hourly traffic patterns by page type (ClickHouse port of the dbt
-- hourly_traffic model). Reproduces int_impressions_aggregated, then reports
-- impression volume and engagement per date / hour / page_type.
WITH
    aggregated AS (
        SELECT
            user_id,
            impression_id,
            page_type,
            date AS event_date,
            hour,
            uniqExact(event_type) AS distinct_events,
            max(event_type) AS max_event_reached
        FROM {table}
        GROUP BY user_id, impression_id, page_type, date, hour
    )
SELECT
    event_date,
    hour,
    page_type,
    uniqExact(impression_id) AS total_impressions,
    uniqExact(user_id) AS unique_users,
    round(avg(distinct_events), 2) AS avg_funnel_depth,
    sum(max_event_reached >= 'd') AS impressions_reaching_d,
    round(
        sum(max_event_reached >= 'd') / nullIf(uniqExact(impression_id), 0) * 100,
        2
    ) AS pct_reaching_d
FROM aggregated
GROUP BY event_date, hour, page_type
ORDER BY event_date, hour, page_type;
