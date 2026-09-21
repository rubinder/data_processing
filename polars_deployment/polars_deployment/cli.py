"""Command-line entry point: load | query | explain | partitions.

Environment:
  POLARS_DATA_DIR  root of the Parquet store (default ./data/impressions)
  API_BASE_URL     web server base URL (default http://localhost:8000)
"""
import argparse
import json
import os
import sys
from datetime import date, datetime

import polars as pl

from polars_deployment import analyses
from polars_deployment.loader import load_impressions
from polars_deployment.store import ParquetStore

DEFAULT_DATA_DIR = "./data/impressions"
DEFAULT_API_BASE_URL = "http://localhost:8000"


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


def _add_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--page_type", type=int, help="restrict to one page type")
    parser.add_argument("--date", help="restrict to one date (YYYY-MM-DD)")
    parser.add_argument("--hour", type=int, help="restrict to one hour")


def _apply_filters(lf: pl.LazyFrame, args: argparse.Namespace) -> pl.LazyFrame:
    """Filters on partition columns; the optimizer turns them into file pruning."""
    if args.page_type is not None:
        lf = lf.filter(pl.col("page_type") == args.page_type)
    if args.date is not None:
        lf = lf.filter(pl.col("date") == date.fromisoformat(args.date))
    if args.hour is not None:
        lf = lf.filter(pl.col("hour") == args.hour)
    return lf


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polars_deployment", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--data-dir", default=os.environ.get("POLARS_DATA_DIR", DEFAULT_DATA_DIR),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    load = sub.add_parser("load", help="fetch one partition from the API")
    load.add_argument("--page_type", type=int, required=True)
    load.add_argument("--date", required=True)
    load.add_argument("--hour", type=int, required=True)
    load.add_argument(
        "--api-base-url",
        default=os.environ.get("API_BASE_URL", DEFAULT_API_BASE_URL),
    )

    query = sub.add_parser("query", help="run an analysis and print the result")
    query.add_argument("name", choices=sorted(analyses.ANALYSES))
    query.add_argument("--engine", choices=analyses.ENGINES, default="in-memory")
    query.add_argument("--format", choices=["table", "json"], default="table")
    _add_filter_args(query)

    explain = sub.add_parser(
        "explain", help="print the optimized plan for an analysis"
    )
    explain.add_argument("name", choices=sorted(analyses.ANALYSES))
    explain.add_argument(
        "--unoptimized", action="store_true",
        help="also print the plan before optimization",
    )
    _add_filter_args(explain)

    sub.add_parser("partitions", help="list the partitions in the store")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = ParquetStore(args.data_dir)

    if args.command == "load":
        load_impressions(
            store, args.api_base_url, args.page_type, args.date, args.hour
        )
        return 0

    if args.command == "partitions":
        parts = store.partitions()
        for page_type, day, hour in parts:
            print(f"page_type={page_type} date={day.isoformat()} hour={hour}")
        print(f"{len(parts)} partition(s) under {store.root}")
        return 0

    events = _apply_filters(store.scan(), args)
    plan = analyses.ANALYSES[args.name](events)

    if args.command == "explain":
        if args.unoptimized:
            print("== unoptimized ==")
            print(plan.explain(optimized=False))
            print("== optimized ==")
        print(plan.explain())
        return 0

    result = plan.collect(engine=args.engine)
    if args.format == "json":
        print(json.dumps(result.to_dicts(), default=_json_default, indent=2))
    else:
        with pl.Config(tbl_rows=50, tbl_cols=20, tbl_width_chars=160):
            print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
