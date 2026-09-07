"""PySpark job that aggregates impression data.

Optionally enriches events with the user dimension before aggregating. That
join is the pipeline's hot-key path: a handful of user_ids (bots, load-test
accounts) carry most rows, so a shuffle on ``user_id`` pins one task with
most of the data (debugging case 01). The strategy order applied here is the
one case 01 concluded with:

1. **broadcast** (default) — the dimension is a few thousand rows. A
   ``broadcast()`` hint removes the shuffle on the join key entirely, so the
   skew is irrelevant rather than merely tolerated, and the hint means Spark
   cannot decline because it mis-estimated the side's size.
2. **AQE** — enabled at the session level (``utils/session.py``) with a skew
   threshold lowered so mid-sized partitions actually get split.
3. **salted** — only when the dimension is itself too large to broadcast.
   ``--join-strategy salted`` routes through ``salted_join``.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_applications.salted_join import SALT_RANGE, salted_join
from spark_applications.utils.mode import add_mode_argument, parse_mode
from spark_applications.utils.observability import get_logger, log_event
from spark_applications.utils.session import get_spark_session
from spark_applications.utils.storage import get_storage_adapter

JOIN_STRATEGIES = ("broadcast", "salted")
USER_KEY = "user_id"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Aggregate impression data"
    )
    add_mode_argument(parser)
    parser.add_argument(
        "--date", type=str, required=True,
        help="Date to aggregate (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--hour", type=str, required=True,
        help="Hour to aggregate (0-23)",
    )
    parser.add_argument(
        "--user-dimension", dest="user_dimension", type=str, default=None,
        help=(
            "Storage path of a user dimension (user_id, segment, ...) to "
            "join onto the events before aggregating. Omit to skip enrichment."
        ),
    )
    parser.add_argument(
        "--join-strategy", dest="join_strategy",
        choices=JOIN_STRATEGIES, default="broadcast",
        help=(
            "How to join the user dimension. 'broadcast' (default) for a "
            "dimension that fits in executor memory; 'salted' only when it "
            "is too large to broadcast (see debugging case 01)."
        ),
    )
    return parser.parse_args(argv)


def enrich_with_users(
    events: DataFrame,
    users: DataFrame,
    strategy: str = "broadcast",
    salt_range: int = SALT_RANGE,
) -> DataFrame:
    """Left-join the user dimension onto events on ``user_id``.

    Left, not inner: an event whose user is missing from the dimension still
    counts; it just carries null dimension attributes.

    ``strategy`` is ``"broadcast"`` (hinted, so the planner cannot fall back
    to a sort-merge join because it lacks statistics) or ``"salted"``.
    """
    if strategy not in JOIN_STRATEGIES:
        raise ValueError(
            f"unknown join strategy {strategy!r}; "
            f"choose from {', '.join(JOIN_STRATEGIES)}"
        )
    if strategy == "broadcast":
        return events.join(F.broadcast(users), on=USER_KEY, how="left")
    return salted_join(
        events, users, USER_KEY, how="left", salt_range=salt_range
    )


def aggregate_impressions(
    df: DataFrame, extra_group_cols: list[str] | None = None
) -> DataFrame:
    """Aggregate impression data by user_id, impression_id, page_type.

    ``extra_group_cols`` adds dimension attributes (e.g. ``segment`` from
    :func:`enrich_with_users`) to the grouping key. They are functionally
    dependent on ``user_id`` so they do not change the grain.

    Computes:
    - event_count: total number of events
    - distinct_events: count of distinct event types
    - first_event_second: earliest second value
    - last_event_second: latest second value
    """
    group_cols = ["user_id", "impression_id", "page_type"]
    group_cols += list(extra_group_cols or [])
    return (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("event_type").alias("distinct_events"),
            F.min("second").alias("first_event_second"),
            F.max("second").alias("last_event_second"),
        )
    )


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    mode = parse_mode(args)

    spark = get_spark_session("Aggregation", mode)
    storage = get_storage_adapter(mode)
    log = get_logger("aggregation")

    try:
        # Read processed impressions data
        source_path = "impressions"
        log_event(
            log, "aggregation_start", date=args.date, hour=args.hour,
        )
        df = storage.read_partitioned(spark, source_path)

        # Filter on the partition columns so Spark prunes to just the relevant
        # date/hour directories instead of scanning the whole table. hour is
        # cast to int so the comparison is correct whether the partition value
        # is read back as a string or an int.
        df_filtered = df.filter(
            (F.col("date") == args.date)
            & (F.col("hour").cast("int") == int(args.hour))
        )

        # Optional enrichment with the user dimension — the hot-key join.
        extra_group_cols: list[str] = []
        if args.user_dimension:
            users = storage.read_partitioned(spark, args.user_dimension)
            df_filtered = enrich_with_users(
                df_filtered, users, strategy=args.join_strategy
            )
            extra_group_cols = [
                c for c in users.columns if c != USER_KEY
            ]
            log_event(
                log, "enrichment_applied", date=args.date, hour=args.hour,
                user_dimension=args.user_dimension,
                join_strategy=args.join_strategy,
            )

        # Aggregate. We avoid count() actions for progress logging: each one is
        # a full job that forces the lineage to recompute before the write.
        agg_df = aggregate_impressions(df_filtered, extra_group_cols)

        # Write output (dynamic partition overwrite -> idempotent re-runs)
        output_path = "impressions_aggregated"
        storage.write_output(
            agg_df,
            output_path,
            partition_cols=["page_type"],
        )

        log_event(
            log, "aggregation_complete", date=args.date, hour=args.hour,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
