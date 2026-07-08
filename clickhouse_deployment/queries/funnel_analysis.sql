-- Funnel conversion analysis by page type (ClickHouse port of the dbt
-- funnel_analysis model). For each page_type and event stage a..f it counts
-- how many distinct impressions reached that stage, the percentage of the
-- page's total impressions, and the conversion rate from the previous stage.
--
-- {table} is templated so tests can point it at a local fixture table;
-- in the cluster it resolves to the Distributed `impressions` table.
WITH
    total_impressions AS (
        SELECT
            page_type,
            uniqExact(impression_id) AS total_impressions
        FROM {table}
        GROUP BY page_type
    ),
    event_counts AS (
        SELECT
            page_type,
            event_type,
            uniqExact(impression_id) AS impressions_at_stage
        FROM {table}
        GROUP BY page_type, event_type
    )
SELECT
    ec.page_type AS page_type,
    ec.event_type AS event_type,
    ec.impressions_at_stage AS impressions_at_stage,
    ti.total_impressions AS total_impressions,
    round(ec.impressions_at_stage / ti.total_impressions * 100, 2) AS pct_of_total,
    round(
        ec.impressions_at_stage
        / nullIf(
            lagInFrame(ec.impressions_at_stage) OVER (
                PARTITION BY ec.page_type
                ORDER BY ec.event_type ASC
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            0
        ) * 100,
        2
    ) AS pct_from_previous_stage
FROM event_counts AS ec
INNER JOIN total_impressions AS ti ON ec.page_type = ti.page_type
ORDER BY page_type ASC, event_type ASC;
