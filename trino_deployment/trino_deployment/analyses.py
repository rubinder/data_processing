"""The four analyses, in Trino SQL over the Iceberg table.

Same semantics as the dbt models, the DuckDB endpoints, the ClickHouse
queries and the Polars pipelines: a per-impression aggregation (dbt's
``int_impressions_aggregated``) inlined as a CTE, then the four rollups.
Rounding matches dbt so the numbers reconcile to the cent.
"""
from __future__ import annotations

import sys

from trino_deployment import client
from trino_deployment.seed import CLICKHOUSE_TABLE, TABLE

# Every analysis reads five columns from a source. Iceberg has them as-is;
# ClickHouse kept the source's date/hour/minute/second and gets event_ts
# derived on the way in. Same downstream SQL, two engines under it.
SOURCES = {
    "iceberg": TABLE,
    "clickhouse": f"""(
    SELECT user_id, impression_id, CAST(page_type AS integer) AS page_type,
           CAST(date AS timestamp(6)) + hour * INTERVAL '1' HOUR
               + minute * INTERVAL '1' MINUTE + second * INTERVAL '1' SECOND AS event_ts,
           event_type
    FROM {CLICKHOUSE_TABLE}
) AS ch""",
}

AGGREGATED_CTE = f"""
aggregated AS (
    SELECT user_id,
           impression_id,
           page_type,
           date(event_ts)               AS event_date,
           hour(event_ts)               AS hour,
           count(*)                     AS event_count,
           count(DISTINCT event_type)   AS distinct_events,
           min(second(event_ts))        AS first_event_second,
           max(second(event_ts))        AS last_event_second,
           min(event_ts)                AS first_event_at,
           max(event_ts)                AS last_event_at,
           max(event_type)              AS max_event_reached
    FROM {TABLE}
    GROUP BY user_id, impression_id, page_type, date(event_ts), hour(event_ts)
)
"""

FUNNEL_ANALYSIS = f"""
WITH total_impressions AS (
    SELECT page_type, count(DISTINCT impression_id) AS total_impressions
    FROM {TABLE} GROUP BY page_type
),
event_counts AS (
    SELECT page_type, event_type, count(DISTINCT impression_id) AS impressions_at_stage
    FROM {TABLE} GROUP BY page_type, event_type
)
SELECT ec.page_type,
       ec.event_type,
       ec.impressions_at_stage,
       ti.total_impressions,
       round(CAST(ec.impressions_at_stage AS decimal(18, 4)) * 100 / ti.total_impressions, 2)
           AS pct_of_total,
       round(CAST(ec.impressions_at_stage AS decimal(18, 4)) * 100
             / lag(ec.impressions_at_stage) OVER (PARTITION BY ec.page_type ORDER BY ec.event_type), 2)
           AS pct_from_previous_stage
FROM event_counts ec
JOIN total_impressions ti ON ec.page_type = ti.page_type
ORDER BY page_type, event_type
"""

PAGE_TYPE_SUMMARY = f"""
WITH {AGGREGATED_CTE}
SELECT page_type,
       count(DISTINCT impression_id)                            AS total_impressions,
       count(DISTINCT user_id)                                  AS unique_users,
       round(avg(CAST(distinct_events AS double)), 2)           AS avg_funnel_depth,
       max(distinct_events)                                     AS max_funnel_depth,
       round(avg(CAST(last_event_second - first_event_second AS double)), 2)
                                                                AS avg_duration_seconds,
       sum(CASE WHEN max_event_reached >= 'd' THEN 1 ELSE 0 END) AS impressions_reaching_d,
       sum(CASE WHEN max_event_reached >= 'e' THEN 1 ELSE 0 END) AS impressions_reaching_e,
       sum(CASE WHEN max_event_reached >= 'f' THEN 1 ELSE 0 END) AS impressions_reaching_f,
       round(CAST(sum(CASE WHEN max_event_reached >= 'd' THEN 1 ELSE 0 END) AS decimal(18, 4))
             * 100 / count(DISTINCT impression_id), 2)         AS pct_reaching_d,
       round(CAST(sum(CASE WHEN max_event_reached >= 'e' THEN 1 ELSE 0 END) AS decimal(18, 4))
             * 100 / count(DISTINCT impression_id), 2)         AS pct_reaching_e,
       round(CAST(sum(CASE WHEN max_event_reached >= 'f' THEN 1 ELSE 0 END) AS decimal(18, 4))
             * 100 / count(DISTINCT impression_id), 2)         AS pct_reaching_f
FROM aggregated
GROUP BY page_type
ORDER BY page_type
"""

USER_ENGAGEMENT = f"""
WITH {AGGREGATED_CTE},
user_metrics AS (
    SELECT user_id,
           count(DISTINCT impression_id)                    AS total_impressions,
           count(DISTINCT page_type)                        AS page_types_visited,
           round(avg(CAST(distinct_events AS double)), 2)   AS avg_funnel_depth,
           max(distinct_events)                             AS max_funnel_depth,
           sum(event_count)                                 AS total_events,
           min(first_event_at)                              AS first_seen,
           max(last_event_at)                               AS last_seen
    FROM aggregated GROUP BY user_id
),
per_page AS (
    SELECT user_id, page_type, count(*) AS impressions_on_page,
           row_number() OVER (PARTITION BY user_id ORDER BY count(*) DESC, page_type) AS rn
    FROM aggregated GROUP BY user_id, page_type
)
SELECT um.user_id, um.total_impressions, um.page_types_visited, um.avg_funnel_depth,
       um.max_funnel_depth, um.total_events, um.first_seen, um.last_seen,
       pp.page_type AS most_engaged_page_type, pp.impressions_on_page AS impressions_on_top_page
FROM user_metrics um
JOIN per_page pp ON pp.user_id = um.user_id AND pp.rn = 1
ORDER BY um.total_impressions DESC, um.user_id
"""

HOURLY_TRAFFIC = f"""
WITH {AGGREGATED_CTE}
SELECT event_date, hour, page_type,
       count(DISTINCT impression_id)                            AS total_impressions,
       count(DISTINCT user_id)                                  AS unique_users,
       round(avg(CAST(distinct_events AS double)), 2)           AS avg_funnel_depth,
       sum(CASE WHEN max_event_reached >= 'd' THEN 1 ELSE 0 END) AS impressions_reaching_d,
       round(CAST(sum(CASE WHEN max_event_reached >= 'd' THEN 1 ELSE 0 END) AS decimal(18, 4))
             * 100 / nullif(count(DISTINCT impression_id), 0), 2) AS pct_reaching_d
FROM aggregated
GROUP BY event_date, hour, page_type
ORDER BY event_date, hour, page_type
"""

ANALYSES = {
    "funnel_analysis": FUNNEL_ANALYSIS,
    "page_type_summary": PAGE_TYPE_SUMMARY,
    "user_engagement": USER_ENGAGEMENT,
    "hourly_traffic": HOURLY_TRAFFIC,
}


def sql_for(name: str, source: str = "iceberg") -> str:
    """The analysis over ``source`` (``iceberg`` or ``clickhouse``)."""
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}; expected one of {list(SOURCES)}")
    return ANALYSES[name].replace(f"FROM {TABLE}", f"FROM {SOURCES[source]}")


def run(name: str, conn=None, source: str = "iceberg") -> client.Result:
    return client.query(sql_for(name, source), conn)


def main() -> int:
    args = sys.argv[1:]
    source = "clickhouse" if "--clickhouse" in args else "iceberg"
    names = [a for a in args if not a.startswith("--")] or list(ANALYSES)
    for name in names:
        result = run(name, source=source)
        print(f"== {name}: {len(result.rows)} rows")
        print("   " + " | ".join(result.columns))
        for row in result.rows[:8]:
            print("   " + " | ".join(str(v) for v in row))
        if len(result.rows) > 8:
            print(f"   ... {len(result.rows) - 8} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
