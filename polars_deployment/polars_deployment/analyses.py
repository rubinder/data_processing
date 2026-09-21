"""The four analyses as lazy Polars queries.

Each function takes a ``LazyFrame`` of raw impression events (the output of
``ParquetStore.scan``) and returns a ``LazyFrame``; nothing runs until the
caller collects. That is the point of the lazy API: the optimizer sees the
whole plan from the Parquet scan to the final sort, so it only reads the
columns an analysis touches and only the partition files its filters allow.

Each analysis reproduces the corresponding dbt model
(``dbt_deployment/dbt_project/models/analysis/*.sql``) and the DuckDB SQL in
``duckdb_deployment/app/queries.py``; the tests hold all three to the same
hand-derived numbers.
"""
from typing import Callable

import polars as pl

ENGINES = ("in-memory", "streaming")

_PCT = 100.0


def _round2(expr: pl.Expr) -> pl.Expr:
    """Round to 2 dp, half away from zero.

    Polars defaults to half-to-even; DuckDB and PostgreSQL round half away
    from zero, so 10.125 is 10.13 there and would be 10.12 here. The repo
    holds every engine to the same numbers, so the SQL convention wins.
    """
    return expr.round(2, mode="half_away_from_zero")


def _event_timestamp() -> pl.Expr:
    """``date + hour:min:second`` as a Datetime expression."""
    return pl.col("date").cast(pl.Datetime("us")) + pl.duration(
        hours=pl.col("hour").cast(pl.Int64),
        minutes=pl.col("min").cast(pl.Int64),
        seconds=pl.col("second").cast(pl.Int64),
    )


def aggregated(events: pl.LazyFrame) -> pl.LazyFrame:
    """Per-impression aggregation, mirroring int_impressions_aggregated.sql.

    One row per (user_id, impression_id, page_type, event_date, hour) with
    the funnel depth, terminal event and timing derived from the raw rows.
    """
    ts = _event_timestamp()
    return (
        events.group_by(["user_id", "impression_id", "page_type", "date", "hour"])
        .agg(
            pl.len().alias("event_count"),
            pl.col("event_type").n_unique().alias("distinct_events"),
            pl.col("second").min().alias("first_event_second"),
            pl.col("second").max().alias("last_event_second"),
            ts.min().alias("first_event_at"),
            ts.max().alias("last_event_at"),
            pl.col("event_type").max().alias("max_event_reached"),
        )
        .rename({"date": "event_date"})
    )


def funnel_analysis(events: pl.LazyFrame) -> pl.LazyFrame:
    """Funnel conversion by page type, mirroring funnel_analysis.sql.

    Per page_type and event_type: distinct impressions reaching the stage,
    pct of the page total, and pct retained from the previous stage (a
    shift over page_type in a<b<...<f order, the LAG in the SQL version).
    """
    totals = events.group_by("page_type").agg(
        pl.col("impression_id").n_unique().alias("total_impressions")
    )
    stages = events.group_by(["page_type", "event_type"]).agg(
        pl.col("impression_id").n_unique().alias("impressions_at_stage")
    )
    at_stage = pl.col("impressions_at_stage")
    return (
        stages.join(totals, on="page_type")
        .sort(["page_type", "event_type"])
        .with_columns(
            _round2(at_stage * _PCT / pl.col("total_impressions"))
            .alias("pct_of_total"),
            _round2(at_stage * _PCT / at_stage.shift(1).over("page_type"))
            .alias("pct_from_previous_stage"),
        )
        .select(
            "page_type", "event_type", "impressions_at_stage",
            "total_impressions", "pct_of_total", "pct_from_previous_stage",
        )
    )


def _reaching(stage: str) -> pl.Expr:
    return (pl.col("max_event_reached") >= stage).sum()


def page_type_summary(events: pl.LazyFrame) -> pl.LazyFrame:
    """High-level statistics per page type, mirroring page_type_summary.sql."""
    total = pl.col("total_impressions")
    return (
        aggregated(events)
        .group_by("page_type")
        .agg(
            pl.col("impression_id").n_unique().alias("total_impressions"),
            pl.col("user_id").n_unique().alias("unique_users"),
            _round2(pl.col("distinct_events").mean()).alias("avg_funnel_depth"),
            pl.col("distinct_events").max().alias("max_funnel_depth"),
            _round2(
                (pl.col("last_event_second") - pl.col("first_event_second")).mean()
            ).alias("avg_duration_seconds"),
            _reaching("d").alias("impressions_reaching_d"),
            _reaching("e").alias("impressions_reaching_e"),
            _reaching("f").alias("impressions_reaching_f"),
        )
        .with_columns(
            _round2(pl.col("impressions_reaching_d") * _PCT / total)
            .alias("pct_reaching_d"),
            _round2(pl.col("impressions_reaching_e") * _PCT / total)
            .alias("pct_reaching_e"),
            _round2(pl.col("impressions_reaching_f") * _PCT / total)
            .alias("pct_reaching_f"),
        )
        .sort("page_type")
    )


def user_engagement(events: pl.LazyFrame) -> pl.LazyFrame:
    """User-level engagement, mirroring user_engagement.sql.

    The most-engaged page is chosen by sorting (impressions desc, page_type
    asc as the tie-break) and taking the first row per user, the Polars
    idiom for Postgres ``DISTINCT ON`` / DuckDB ``QUALIFY row_number() = 1``.
    """
    agg = aggregated(events)
    user_metrics = agg.group_by("user_id").agg(
        pl.col("impression_id").n_unique().alias("total_impressions"),
        pl.col("page_type").n_unique().alias("page_types_visited"),
        _round2(pl.col("distinct_events").mean()).alias("avg_funnel_depth"),
        pl.col("distinct_events").max().alias("max_funnel_depth"),
        pl.col("event_count").sum().alias("total_events"),
        pl.col("first_event_at").min().alias("first_seen"),
        pl.col("last_event_at").max().alias("last_seen"),
    )
    most_engaged = (
        agg.group_by(["user_id", "page_type"])
        .agg(pl.len().alias("impressions_on_page"))
        .sort(
            ["user_id", "impressions_on_page", "page_type"],
            descending=[False, True, False],
        )
        .group_by("user_id", maintain_order=True)
        .agg(
            pl.col("page_type").first().alias("most_engaged_page_type"),
            pl.col("impressions_on_page").first().alias("impressions_on_top_page"),
        )
    )
    return (
        user_metrics.join(most_engaged, on="user_id")
        .select(
            "user_id", "total_impressions", "page_types_visited",
            "avg_funnel_depth", "max_funnel_depth", "total_events",
            "first_seen", "last_seen", "most_engaged_page_type",
            "impressions_on_top_page",
        )
        .sort(["total_impressions", "user_id"], descending=[True, False])
    )


def hourly_traffic(events: pl.LazyFrame) -> pl.LazyFrame:
    """Hourly traffic by page type, mirroring hourly_traffic.sql."""
    return (
        aggregated(events)
        .group_by(["event_date", "hour", "page_type"])
        .agg(
            pl.col("impression_id").n_unique().alias("total_impressions"),
            pl.col("user_id").n_unique().alias("unique_users"),
            _round2(pl.col("distinct_events").mean()).alias("avg_funnel_depth"),
            _reaching("d").alias("impressions_reaching_d"),
        )
        .with_columns(
            _round2(
                pl.col("impressions_reaching_d") * _PCT / pl.col("total_impressions")
            ).alias("pct_reaching_d")
        )
        .sort(["event_date", "hour", "page_type"])
    )


ANALYSES: dict[str, Callable[[pl.LazyFrame], pl.LazyFrame]] = {
    "funnel": funnel_analysis,
    "page-type-summary": page_type_summary,
    "user-engagement": user_engagement,
    "hourly-traffic": hourly_traffic,
}


def run(name: str, events: pl.LazyFrame, engine: str = "in-memory") -> pl.DataFrame:
    """Build the named analysis over ``events`` and collect it.

    ``engine`` is ``"in-memory"`` (the default engine: whole intermediate
    frames in RAM, fastest when they fit) or ``"streaming"`` (the plan runs
    in batches so peak memory is bounded by batch size, not input size).
    """
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, got {engine!r}")
    return ANALYSES[name](events).collect(engine=engine)
