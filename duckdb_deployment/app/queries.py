"""Analytical queries against the embedded DuckDB impressions table.

Each function reproduces the logic of the corresponding dbt analysis model
(see ../dbt_deployment/dbt_project/models/analysis/*.sql) using standard
DuckDB SQL. The per-impression aggregation used by three of the queries mirrors
int_impressions_aggregated.sql and is inlined as a CTE here.
"""
import duckdb

# Per-impression aggregation, equivalent to int_impressions_aggregated.sql.
# One row per (user_id, impression_id, page_type, event_date, hour) with the
# funnel depth, terminal event and timing derived from the raw event rows.
_AGGREGATED_CTE = """
aggregated as (
    select
        user_id,
        impression_id,
        page_type,
        date as event_date,
        hour,
        count(*) as event_count,
        count(distinct event_type) as distinct_events,
        min(second) as first_event_second,
        max(second) as last_event_second,
        min(
            cast(date as timestamp)
            + to_hours(hour) + to_minutes(min) + to_seconds(second)
        ) as first_event_at,
        max(
            cast(date as timestamp)
            + to_hours(hour) + to_minutes(min) + to_seconds(second)
        ) as last_event_at,
        max(event_type) as max_event_reached
    from impressions
    group by user_id, impression_id, page_type, date, hour
)
"""


def _rows_to_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict]:
    """Convert the last executed result set into a list of dicts."""
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def funnel_analysis(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Funnel conversion analysis by page type.

    Mirrors funnel_analysis.sql: per page_type and event_type, the distinct
    impressions reaching that stage, the pct of the page total, and the pct
    retained from the previous stage (LAG over the a<b<c<d<e<f ordering).
    """
    sql = """
    with total_impressions as (
        select
            page_type,
            count(distinct impression_id) as total_impressions
        from impressions
        group by page_type
    ),
    event_counts as (
        select
            page_type,
            event_type,
            count(distinct impression_id) as impressions_at_stage
        from impressions
        group by page_type, event_type
    ),
    funnel as (
        select
            ec.page_type,
            ec.event_type,
            ec.impressions_at_stage,
            ti.total_impressions,
            round(
                (ec.impressions_at_stage * 100.0) / ti.total_impressions, 2
            ) as pct_of_total,
            round(
                (ec.impressions_at_stage * 100.0) / lag(ec.impressions_at_stage)
                    over (partition by ec.page_type order by ec.event_type),
                2
            ) as pct_from_previous_stage
        from event_counts ec
        join total_impressions ti on ec.page_type = ti.page_type
    )
    select * from funnel
    order by page_type, event_type
    """
    return _rows_to_dicts(conn.execute(sql))


def page_type_summary(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """High-level summary statistics per page type.

    Mirrors page_type_summary.sql, built on the inlined per-impression
    aggregation CTE.
    """
    sql = f"""
    with {_AGGREGATED_CTE},
    summary as (
        select
            page_type,
            count(distinct impression_id) as total_impressions,
            count(distinct user_id) as unique_users,
            round(avg(distinct_events), 2) as avg_funnel_depth,
            max(distinct_events) as max_funnel_depth,
            round(avg(last_event_second - first_event_second), 2)
                as avg_duration_seconds,
            sum(case when max_event_reached >= 'd' then 1 else 0 end)
                as impressions_reaching_d,
            sum(case when max_event_reached >= 'e' then 1 else 0 end)
                as impressions_reaching_e,
            sum(case when max_event_reached >= 'f' then 1 else 0 end)
                as impressions_reaching_f,
            round(
                sum(case when max_event_reached >= 'd' then 1 else 0 end) * 100.0
                / count(distinct impression_id), 2
            ) as pct_reaching_d,
            round(
                sum(case when max_event_reached >= 'e' then 1 else 0 end) * 100.0
                / count(distinct impression_id), 2
            ) as pct_reaching_e,
            round(
                sum(case when max_event_reached >= 'f' then 1 else 0 end) * 100.0
                / count(distinct impression_id), 2
            ) as pct_reaching_f
        from aggregated
        group by page_type
    )
    select * from summary
    order by page_type
    """
    return _rows_to_dicts(conn.execute(sql))


def user_engagement(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """User-level engagement metrics across all page types.

    Mirrors user_engagement.sql. Postgres ``distinct on`` is replaced with a
    QUALIFY window (row_number ordered by impression count desc, then page_type
    asc as a deterministic tie-break) to pick each user's most-engaged page.
    """
    sql = f"""
    with {_AGGREGATED_CTE},
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
        select
            user_id,
            page_type as most_engaged_page_type,
            count(*) as impressions_on_page
        from aggregated
        group by user_id, page_type
        qualify row_number() over (
            partition by user_id
            order by count(*) desc, page_type asc
        ) = 1
    )
    select
        um.user_id,
        um.total_impressions,
        um.page_types_visited,
        um.avg_funnel_depth,
        um.max_funnel_depth,
        um.total_events,
        um.first_seen,
        um.last_seen,
        mep.most_engaged_page_type,
        mep.impressions_on_page as impressions_on_top_page
    from user_metrics um
    join most_engaged_page mep on um.user_id = mep.user_id
    order by um.total_impressions desc, um.user_id
    """
    return _rows_to_dicts(conn.execute(sql))


def hourly_traffic(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Hourly traffic patterns by page type.

    Mirrors hourly_traffic.sql, built on the inlined per-impression
    aggregation CTE.
    """
    sql = f"""
    with {_AGGREGATED_CTE},
    hourly as (
        select
            event_date,
            hour,
            page_type,
            count(distinct impression_id) as total_impressions,
            count(distinct user_id) as unique_users,
            round(avg(distinct_events), 2) as avg_funnel_depth,
            sum(case when max_event_reached >= 'd' then 1 else 0 end)
                as impressions_reaching_d,
            round(
                sum(case when max_event_reached >= 'd' then 1 else 0 end) * 100.0
                / nullif(count(distinct impression_id), 0), 2
            ) as pct_reaching_d
        from aggregated
        group by event_date, hour, page_type
    )
    select * from hourly
    order by event_date, hour, page_type
    """
    return _rows_to_dicts(conn.execute(sql))
