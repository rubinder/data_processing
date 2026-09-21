"""Measure the four analyses under four execution strategies on one machine.

The question this answers: when the data fits on one box, what do the
single-node engines cost in time and memory, and does the lazy/streaming
machinery actually buy anything over "read it all, then compute"?

Variants (each is the same four analyses over the same Parquet files):

* ``eager``      read every file into one DataFrame, then compute. This is
                 the pandas-shaped workflow: the whole table is resident
                 before the first aggregation runs, and a filter on
                 ``page_type`` is applied after the read.
* ``lazy``       ``scan_parquet`` + the lazy plan, collected on the default
                 in-memory engine. Projection and predicate pushdown reach
                 the reader; hive filters skip files.
* ``streaming``  the same plan collected with ``engine="streaming"``: the
                 pipeline runs in batches so peak memory is bounded by the
                 batch size and the group-by state, not by the input.
* ``duckdb``     the SQL from ``duckdb_deployment/app/queries.py`` run by
                 DuckDB over the same files via ``read_parquet(...,
                 hive_partitioning=true)``. Skipped if that module or the
                 ``duckdb`` package is absent.

Workloads: the four analyses over the whole store, ``hourly-traffic``
restricted to one page_type (does the filter prune files?), and
``sink-events-d-plus``: filter the raw events to stages d and beyond and
write them to a single Parquet file. The last one is the ETL-shaped pass
where a streaming engine can keep memory flat, because nothing in the
pipeline needs to see the whole input at once.

Method notes:

* **Every (variant, analysis) pair runs in a fresh subprocess** so the peak
  RSS reported is that pair's alone, not the high-water mark of whatever
  ran earlier in the process.
* **Results are verified, not assumed.** Every variant's output for every
  analysis is compared against the ``lazy`` output (sorted, numerics as
  Float64 rounded to 2 dp, timestamps as strings). A mismatch fails the run.
* **Median of N repeats**, the first repeat included: these are batch jobs,
  and a batch job does not get a warm-up.
* Both Polars and DuckDB use every core by default; nothing is pinned.

Usage::

    python benchmarks/bench_engines.py --days 2 --repeat 3
    python benchmarks/bench_engines.py --data-dir /data/bench --regenerate
"""
import argparse
import datetime as dt
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
MODULE_ROOT = HERE.parent
REPO_ROOT = MODULE_ROOT.parent
sys.path.insert(0, str(MODULE_ROOT))

from polars_deployment import analyses  # noqa: E402
from polars_deployment.schema import HIVE_SCHEMA  # noqa: E402
from polars_deployment.store import DATA_FILE, ParquetStore  # noqa: E402

VARIANTS = ["eager", "lazy", "streaming", "duckdb"]
FILTERED_SUFFIX = "@page_type=1"
SINK = "sink-events-d-plus"
ANALYSIS_KEYS = sorted(analyses.ANALYSES) + ["hourly-traffic" + FILTERED_SUFFIX, SINK]
REFERENCE = "lazy"

# Sort keys used to normalise result frames before comparison.
_SORT_KEYS = {
    "funnel": ["page_type", "event_type"],
    "page-type-summary": ["page_type"],
    "user-engagement": ["user_id"],
    "hourly-traffic": ["event_date", "hour", "page_type"],
    SINK: ["page_type", "event_type"],
}


def _split(key: str) -> tuple[str, int | None]:
    if key.endswith(FILTERED_SUFFIX):
        return key[: -len(FILTERED_SUFFIX)], 1
    return key, None


def _maxrss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


# --------------------------------------------------------------------- child

def _duckdb_sql(name: str) -> str:
    """Pull the SQL text out of duckdb_deployment's query function.

    Those functions build the SQL and execute it in one go; a capturing
    stand-in for the connection records the statement instead of running
    it, so DuckDB here runs exactly the SQL the DuckDB module ships.
    """
    sys.path.insert(0, str(REPO_ROOT / "duckdb_deployment"))
    from app import queries  # noqa: E402  (duckdb_deployment/app)

    class _Captured(Exception):
        pass

    class _Capture:
        sql = None

        def execute(self, sql):
            self.sql = sql
            raise _Captured

    fn = {
        "funnel": queries.funnel_analysis,
        "page-type-summary": queries.page_type_summary,
        "user-engagement": queries.user_engagement,
        "hourly-traffic": queries.hourly_traffic,
    }[name]
    capture = _Capture()
    try:
        fn(capture)
    except _Captured:
        pass
    return capture.sql


def _sink_summary(path: Path) -> pl.DataFrame:
    """Row counts per (page_type, event_type) of a sunk file, for verification."""
    return (
        pl.scan_parquet(path)
        .group_by(["page_type", "event_type"])
        .agg(
            pl.len().alias("rows"),
            pl.col("impression_id").n_unique().alias("impressions"),
        )
        .collect()
    )


def _sink_runner(variant: str, store: ParquetStore, glob: str, out: Path):
    """The ETL-shaped workload: filter raw events to d+ and write one file."""
    target = out / "_sink.parquet"
    out.mkdir(parents=True, exist_ok=True)
    predicate = pl.col("event_type") >= "d"

    if variant == "duckdb":
        import duckdb

        conn = duckdb.connect(":memory:")

        def run():
            conn.execute(
                f"copy (select * from read_parquet('{glob}', hive_partitioning=true) "
                f"where event_type >= 'd') to '{target}' "
                "(format parquet, compression zstd)"
            )
            return _sink_summary(target)

    elif variant == "eager":

        def run():
            df = pl.read_parquet(glob, hive_partitioning=True, hive_schema=HIVE_SCHEMA)
            df.filter(predicate).write_parquet(target, compression="zstd")
            return _sink_summary(target)

    elif variant == "lazy":

        def run():
            store.scan().filter(predicate).collect().write_parquet(
                target, compression="zstd"
            )
            return _sink_summary(target)

    else:

        def run():
            store.scan().filter(predicate).sink_parquet(
                target, compression="zstd", engine="streaming"
            )
            return _sink_summary(target)

    return run


def _run_child(variant: str, key: str, data_dir: Path, repeat: int, out: Path):
    name, page_type = _split(key)
    store = ParquetStore(data_dir)
    glob = str(store.root / "**" / DATA_FILE)
    rss_after_import = _maxrss_bytes()

    if key == SINK:
        run = _sink_runner(variant, store, glob, out)

    elif variant == "duckdb":
        import duckdb

        conn = duckdb.connect(":memory:")
        where = f" where page_type = {page_type}" if page_type else ""
        conn.execute(
            "create view impressions as select * from "
            f"read_parquet('{glob}', hive_partitioning=true){where}"
        )
        sql = _duckdb_sql(name)

        def run():
            return conn.sql(sql).pl()

    elif variant == "eager":
        build = analyses.ANALYSES[name]

        def run():
            df = pl.read_parquet(
                glob, hive_partitioning=True, hive_schema=HIVE_SCHEMA
            )
            if page_type:
                df = df.filter(pl.col("page_type") == page_type)
            return build(df.lazy()).collect()

    else:
        build = analyses.ANALYSES[name]
        engine = "streaming" if variant == "streaming" else "in-memory"

        def run():
            lf = store.scan()
            if page_type:
                lf = lf.filter(pl.col("page_type") == page_type)
            return build(lf).collect(engine=engine)

    times = []
    result = None
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = run()
        times.append(time.perf_counter() - t0)

    out.mkdir(parents=True, exist_ok=True)
    result.write_parquet(out / f"{key}.parquet")
    sink = out / "_sink.parquet"
    if sink.exists():
        sink.unlink()
    print(json.dumps({
        "variant": variant,
        "analysis": key,
        "times_s": times,
        "median_s": statistics.median(times),
        "min_s": min(times),
        "rows": result.height,
        "maxrss_bytes": _maxrss_bytes(),
        "rss_after_import_bytes": rss_after_import,
    }))


# -------------------------------------------------------------------- parent

def _normalise(df: pl.DataFrame, name: str) -> pl.DataFrame:
    casts = {}
    for col, dtype in df.schema.items():
        if dtype.is_numeric():
            casts[col] = pl.col(col).cast(pl.Float64).round(2)
        elif dtype in (pl.Datetime, pl.Date) or isinstance(dtype, pl.Datetime):
            casts[col] = pl.col(col).cast(pl.String)
    return df.with_columns(list(casts.values())).sort(_SORT_KEYS[name])


def _verify(results_dir: Path, variants: list[str]) -> list[str]:
    problems = []
    for key in ANALYSIS_KEYS:
        name, _ = _split(key)
        ref = _normalise(
            pl.read_parquet(results_dir / REFERENCE / f"{key}.parquet"), name
        )
        for variant in variants:
            if variant == REFERENCE:
                continue
            got = _normalise(
                pl.read_parquet(results_dir / variant / f"{key}.parquet"), name
            )
            if got.columns != ref.columns:
                problems.append(
                    f"{variant}/{key}: columns {got.columns} != {ref.columns}"
                )
                continue
            if got.height != ref.height:
                problems.append(f"{variant}/{key}: {got.height} rows != {ref.height}")
                continue
            for col in ref.columns:
                a, b = ref[col], got[col]
                if a.dtype == pl.Float64:
                    diff = (a - b).abs().fill_null(0.0).max()
                    nulls_match = (a.is_null() == b.is_null()).all()
                    if not nulls_match or (diff is not None and diff > 1e-6):
                        problems.append(
                            f"{variant}/{key}: column {col} differs (max abs {diff})"
                        )
                elif not a.equals(b):
                    problems.append(f"{variant}/{key}: column {col} differs")
    return problems


def _dataset_stats(store: ParquetStore) -> dict:
    files = store.files()
    rows = store.scan().select(pl.len()).collect().item()
    return {
        "files": len(files),
        "partitions": len(store.partitions()),
        "parquet_bytes": sum(f.stat().st_size for f in files),
        "rows": rows,
    }


def _environment() -> dict:
    env = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "polars": pl.__version__,
    }
    try:
        import duckdb
        env["duckdb"] = duckdb.__version__
    except ImportError:
        env["duckdb"] = None
    return env


def _mb(n: int) -> float:
    return round(n / (1024 * 1024), 1)


def _write_markdown(report: dict, path: Path) -> None:
    ds, env = report["dataset"], report["environment"]
    variants = report["variants"]
    lines = [
        "# Single-node engine benchmark",
        "",
        f"Generated {report['generated_at']} on {env['platform']} "
        f"({env['cpu_count']} cores), Python {env['python']}, "
        f"Polars {env['polars']}, DuckDB {env['duckdb']}.",
        "",
        f"Dataset: {ds['rows']:,} event rows in {ds['files']} partition files, "
        f"{_mb(ds['parquet_bytes'])} MB of zstd Parquet "
        f"({report['days']} day(s) x 24 hours x 3 page types).",
        "",
        f"Median wall-clock of {report['repeat']} runs, seconds:",
        "",
        "| analysis | " + " | ".join(variants) + " |",
        "|---|" + "---|" * len(variants),
    ]
    for key in ANALYSIS_KEYS:
        row = [f"{report['results'][v][key]['median_s']:.3f}" for v in variants]
        lines.append(f"| {key} | " + " | ".join(row) + " |")
    lines += [
        "",
        "Peak resident set size of the measuring subprocess, MB:",
        "",
        "| analysis | " + " | ".join(variants) + " |",
        "|---|" + "---|" * len(variants),
    ]
    for key in ANALYSIS_KEYS:
        row = [f"{_mb(report['results'][v][key]['maxrss_bytes'])}" for v in variants]
        lines.append(f"| {key} | " + " | ".join(row) + " |")
    lines += [
        "",
        "Verification: "
        + ("all variants agree with `lazy` on every analysis."
           if not report["verification_problems"]
           else "MISMATCHES: " + "; ".join(report["verification_problems"])),
        "",
    ]
    path.write_text("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--child", choices=VARIANTS, help=argparse.SUPPRESS)
    parser.add_argument("--analysis", choices=ANALYSIS_KEYS, help=argparse.SUPPRESS)
    parser.add_argument("--results-tmp", help=argparse.SUPPRESS)
    parser.add_argument("--data-dir", default=str(HERE / "data"))
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--impressions-per-hour", type=int, default=20_000)
    parser.add_argument("--regenerate", action="store_true")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--variants", nargs="+", default=VARIANTS, choices=VARIANTS)
    parser.add_argument("--out", default=str(HERE / "results"))
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir)
    if args.child:
        _run_child(
            args.child, args.analysis, data_dir, args.repeat, Path(args.results_tmp)
        )
        return 0

    store = ParquetStore(data_dir)
    if args.regenerate or not store.files():
        from benchmarks.synth import build_dataset

        print(f"Generating {args.days} day(s) of partitions under {data_dir} ...")
        t0 = time.perf_counter()
        rows = build_dataset(store, args.days, args.impressions_per_hour)
        print(f"  {rows:,} rows in {time.perf_counter() - t0:.1f}s")

    variants = list(args.variants)
    if "duckdb" in variants:
        try:
            import duckdb  # noqa: F401
            assert (REPO_ROOT / "duckdb_deployment" / "app" / "queries.py").exists()
        except (ImportError, AssertionError):
            print(
                "duckdb variant skipped: needs the duckdb package "
                "and ../duckdb_deployment"
            )
            variants.remove("duckdb")

    results_tmp = Path(args.out) / "_frames"
    results: dict[str, dict] = {v: {} for v in variants}
    for key in ANALYSIS_KEYS:
        for variant in variants:
            cmd = [
                sys.executable, str(Path(__file__)),
                "--child", variant, "--analysis", key,
                "--data-dir", str(data_dir), "--repeat", str(args.repeat),
                "--results-tmp", str(results_tmp / variant),
            ]
            proc = subprocess.run(
                cmd, capture_output=True, text=True, cwd=str(MODULE_ROOT)
            )
            if proc.returncode != 0:
                print(proc.stdout)
                print(proc.stderr)
                raise SystemExit(f"{variant}/{key} failed")
            rec = json.loads(proc.stdout.strip().splitlines()[-1])
            results[variant][key] = rec
            print(
                f"  {variant:10s} {key:32s} median {rec['median_s']:7.3f}s  "
                f"peak RSS {_mb(rec['maxrss_bytes']):8.1f} MB  rows {rec['rows']}"
            )

    problems = _verify(results_tmp, variants)
    report = {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "environment": _environment(),
        "dataset": _dataset_stats(store),
        "days": args.days,
        "repeat": args.repeat,
        "variants": variants,
        "results": results,
        "verification_problems": problems,
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "latest.json").write_text(json.dumps(report, indent=2))
    _write_markdown(report, out / "latest.md")
    print(f"\nWrote {out / 'latest.json'} and {out / 'latest.md'}")
    if problems:
        print("VERIFICATION FAILED:")
        for p in problems:
            print("  " + p)
        return 1
    print("Verification: all variants agree.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
