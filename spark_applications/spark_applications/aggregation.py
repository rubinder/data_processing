"""PySpark job that aggregates impression data."""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_applications.utils.mode import Mode, add_mode_argument, parse_mode
from spark_applications.utils.observability import get_logger, log_event
from spark_applications.utils.session import get_spark_session
from spark_applications.utils.storage import get_storage_adapter


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
    return parser.parse_args(argv)


def aggregate_impressions(df: DataFrame) -> DataFrame:
    """Aggregate impression data by user_id, impression_id, page_type.

    Computes:
    - event_count: total number of events
    - distinct_events: count of distinct event types
    - first_event_second: earliest second value
    - last_event_second: latest second value
    """
    return (
        df.groupBy("user_id", "impression_id", "page_type")
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

        # Aggregate. We avoid count() actions for progress logging: each one is
        # a full job that forces the lineage to recompute before the write.
        agg_df = aggregate_impressions(df_filtered)

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
