-- User-level engagement metrics across page types (ClickHouse port of the dbt
-- user_engagement model). Reproduces int_impressions_aggregated, rolls it up
-- per user, and joins the user's most-engaged page type (most impressions).
WITH
    base AS (
        SELECT
            *,
            toDateTime(date) + (hour * 3600 + minute * 60 + second) AS event_timestamp
        FROM {table}
    ),
    aggregated AS (
        SELECT
            user_id,
            impression_id,
            page_type,
            count(*) AS event_count,
            uniqExact(event_type) AS distinct_events,
            min(event_timestamp) AS first_event_at,
            max(event_timestamp) AS last_event_at
        FROM base
        GROUP BY user_id, impression_id, page_type
    ),
    user_metrics AS (
        SELECT
            user_id,
            uniqExact(impression_id) AS total_impressions,
            uniqExact(page_type) AS page_types_visited,
            round(avg(distinct_events), 2) AS avg_funnel_depth,
            max(distinct_events) AS max_funnel_depth,
            sum(event_count) AS total_events,
            min(first_event_at) AS first_seen,
            max(last_event_at) AS last_seen
        FROM aggregated
        GROUP BY user_id
    ),
    most_engaged_page AS (
        SELECT
            user_id,
            most_engaged_page_type,
            impressions_on_page
        FROM (
            SELECT
                user_id,
                page_type AS most_engaged_page_type,
                count(*) AS impressions_on_page,
                row_number() OVER (
                    PARTITION BY user_id ORDER BY count(*) DESC, page_type ASC
                ) AS rn
            FROM aggregated
            GROUP BY user_id, page_type
        )
        WHERE rn = 1
    )
SELECT
    um.user_id AS user_id,
    um.total_impressions AS total_impressions,
    um.page_types_visited AS page_types_visited,
    um.avg_funnel_depth AS avg_funnel_depth,
    um.max_funnel_depth AS max_funnel_depth,
    um.total_events AS total_events,
    um.first_seen AS first_seen,
    um.last_seen AS last_seen,
    mep.most_engaged_page_type AS most_engaged_page_type,
    mep.impressions_on_page AS impressions_on_top_page
FROM user_metrics AS um
INNER JOIN most_engaged_page AS mep ON um.user_id = mep.user_id
ORDER BY um.total_impressions DESC, um.user_id ASC;
