"""Deterministic synthetic impression partitions at a chosen scale.

Reproduces the web server generator's funnel shape (``FUNNEL_RATES`` per
page_type, events at successive seconds within a minute) but builds each
partition as a Polars frame: the per-impression funnel depth is rolled in
Python (``random.Random(seed)`` for reproducibility), and the explosion into
one row per event is a vectorised ``int_ranges`` + ``explode``.

Usage::

    python benchmarks/synth.py --data-dir benchmarks/data --days 2
"""
import argparse
import datetime as dt
import random
import uuid

import polars as pl

from polars_deployment.schema import COLUMNS, EVENT_TYPES, SCHEMA
from polars_deployment.store import ParquetStore

# Cumulative probability of reaching at least each event type, per page_type
# (copied from web_server_code/web_server_code/generator.py).
FUNNEL_RATES: dict[int, list[float]] = {
    1: [1.0, 0.80, 0.40, 0.10, 0.0, 0.0],
    2: [1.0, 0.85, 0.55, 0.30, 0.10, 0.0],
    3: [1.0, 0.90, 0.70, 0.50, 0.20, 0.10],
}

DEFAULT_IMPRESSIONS_PER_HOUR = 20_000
DEFAULT_USERS = 1_200
START_DATE = dt.date(2026, 1, 1)


def _max_event_index(rng: random.Random, funnel: list[float]) -> int:
    roll = rng.random()
    max_idx = 0
    for i, rate in enumerate(funnel):
        if roll < rate:
            max_idx = i
    return max_idx


def generate_partition(
    page_type: int,
    date: dt.date,
    hour: int,
    users: list[str],
    impressions: int = DEFAULT_IMPRESSIONS_PER_HOUR,
    seed: int = 0,
) -> pl.DataFrame:
    """One (page_type, date, hour) partition of impression events."""
    rng = random.Random(
        seed * 1_000_003 + page_type * 100_000 + date.toordinal() * 100 + hour
    )
    funnel = FUNNEL_RATES[page_type]
    user_ids, minutes, max_idx = [], [], []
    seen: set[tuple[str, int]] = set()
    while len(user_ids) < impressions:
        user = rng.choice(users)
        minute = rng.randrange(60)
        if (user, minute) in seen:
            continue
        seen.add((user, minute))
        user_ids.append(user)
        minutes.append(minute)
        max_idx.append(_max_event_index(rng, funnel))
    base_second = [rng.randint(0, 59 - m) for m in max_idx]

    frame = pl.DataFrame(
        {
            "user_id": user_ids,
            "min": minutes,
            "max_idx": max_idx,
            "base_second": base_second,
        }
    ).with_columns(
        pl.concat_str(
            [pl.col("user_id"), pl.lit(f":{page_type}:{date}:{hour}:"), pl.col("min")]
        )
        .map_elements(
            lambda name: str(uuid.uuid5(uuid.NAMESPACE_DNS, name)),
            return_dtype=pl.String,
        )
        .alias("impression_id"),
        pl.int_ranges(0, pl.col("max_idx") + 1).alias("event_idx"),
    )
    return (
        frame.explode("event_idx", empty_as_null=False)
        .with_columns(
            pl.lit(page_type).alias("page_type"),
            pl.lit(date).alias("date"),
            pl.lit(hour).alias("hour"),
            (pl.col("base_second") + pl.col("event_idx")).alias("second"),
            pl.col("event_idx")
            .replace_strict(
                list(range(len(EVENT_TYPES))), EVENT_TYPES, return_dtype=pl.String
            )
            .alias("event_type"),
        )
        .select(COLUMNS)
        .cast(SCHEMA)
    )


def build_dataset(
    store: ParquetStore,
    days: int,
    impressions_per_hour: int = DEFAULT_IMPRESSIONS_PER_HOUR,
    users: int = DEFAULT_USERS,
    seed: int = 0,
) -> int:
    """Write ``days`` x 24 hours x 3 page types of partitions. Returns rows."""
    rng = random.Random(seed)
    user_pool = [str(uuid.UUID(int=rng.getrandbits(128))) for _ in range(users)]
    total = 0
    for day in range(days):
        date = START_DATE + dt.timedelta(days=day)
        for hour in range(24):
            for page_type in FUNNEL_RATES:
                df = generate_partition(
                    page_type, date, hour, user_pool, impressions_per_hour, seed
                )
                store.write_partition(df, page_type, date, hour)
                total += df.height
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument(
        "--impressions-per-hour", type=int, default=DEFAULT_IMPRESSIONS_PER_HOUR
    )
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args(argv)
    store = ParquetStore(args.data_dir)
    rows = build_dataset(
        store, args.days, args.impressions_per_hour, args.users, args.seed
    )
    print(f"wrote {rows:,} rows in {len(store.files())} partitions to {store.root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
