/*
    Funnel conversion analysis by page type.
    Shows how many impressions reach each event stage and conversion rates.

    Expected patterns based on data generation:
      - Page 1: ~10% reach d, 0% reach e or f
      - Page 2: ~30% reach d, ~10% reach e, 0% reach f
      - Page 3: ~50% reach d, ~20% reach e, ~10% reach f
*/
with impressions as (
    select * from "data_processing"."public_staging"."stg_impressions"
),

total_impressions as (
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
            (ec.impressions_at_stage::numeric / ti.total_impressions) * 100, 2
        ) as pct_of_total,
        round(
            (ec.impressions_at_stage::numeric / lag(ec.impressions_at_stage) over (
                partition by ec.page_type order by ec.event_type
            )) * 100, 2
        ) as pct_from_previous_stage
    from event_counts ec
    join total_impressions ti on ec.page_type = ti.page_type
)

select * from funnel
order by page_type, event_type