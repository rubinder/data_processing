"""Case 07 — the same UDF, an order of magnitude apart.

Nothing here is broken. The job produces correct results and never raises; it
is simply paying a serialization tax on every row. This is the most common
avoidable cost in a PySpark codebase, and the plan names it outright — one
operator for the slow path, a different one for the fast path.

Run: ``uv run python -m spark_applications.debugging.run --case 7``
"""

import time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    count_operator,
    python_eval_nodes,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import get_debug_session

CASE_ID = "07"
TITLE = "Python UDF -> pandas UDF (BatchEvalPython vs ArrowEvalPython)"

SYMPTOM = """
No error. The enrichment stage is just disproportionately slow:

  - the stage's CPU time is many times its input size would suggest
  - executor CPU sits near 100% while shuffle and I/O are near idle
  - the Spark UI shows most time inside a BatchEvalPython node
  - `top` on a worker shows dozens of `python3` processes alongside the JVM

Rewriting the function body makes almost no difference, because the time is
not going into the function.
"""

CAUSE = """
A plain `@udf` is evaluated one row at a time in a separate Python process.
For every single row Spark must:

  1. serialize the row from the JVM (pickle),
  2. write it over a socket to a Python worker,
  3. deserialize it, call your function on one value,
  4. serialize the result, send it back, deserialize into the JVM.

The function body might be a multiplication; the overhead around it is four
serialization steps and a process boundary. At a hundred million rows that
overhead, not the arithmetic, is the job.

A `@pandas_udf` moves the same data in Arrow batches: one transfer per few
thousand rows instead of per row, no pickling (Arrow is already columnar and
shared-memory friendly), and your function is called once per batch with a
pandas Series — so the loop runs in NumPy's C code rather than the CPython
interpreter.

The plan distinguishes them explicitly, which makes this a one-line audit:

    BatchEvalPython [<lambda>(second#12)]     <- row-at-a-time, slow path
    ArrowEvalPython [ratio(second#12)]        <- Arrow batches, fast path

Grep any plan for BatchEvalPython; every occurrence is a candidate.
"""

RESOLUTION = """
Convert the UDF to a pandas UDF. The signature changes from scalars to
Series, and the body becomes a vectorised expression:

    # BEFORE — BatchEvalPython, one row at a time
    @udf(DoubleType())
    def engagement_rate(second, minute):
        if minute == 0:
            return 0.0
        return float(second) / float(minute)

    # AFTER — ArrowEvalPython, one call per Arrow batch
    @pandas_udf(DoubleType())
    def engagement_rate(second: pd.Series, minute: pd.Series) -> pd.Series:
        ratio = second / minute.replace(0, pd.NA)
        return ratio.fillna(0.0).astype("float64")

Three rules for the conversion:

  - take and return pd.Series, never scalars
  - operate on whole Series (vectorised); a .apply() inside the pandas UDF
    reintroduces the per-row Python loop you were removing
  - handle NULLs as pandas NA, not as None checks

And before either: check whether built-in column functions can express it.
They beat both — no Python worker at all.
"""

NOTES = [
    "Rough ordering, best to worst: built-in column functions > pandas UDF "
    "> Python UDF. Reach for a pandas UDF only when built-ins genuinely "
    "cannot express the logic.",

    "A pandas UDF that calls .apply() or iterates the Series is a Python "
    "UDF with extra steps. The win comes from vectorisation, not from the "
    "decorator.",

    "Tune spark.sql.execution.arrow.maxRecordsPerBatch (default 10,000) if "
    "batches are too large for executor memory. Larger batches amortise "
    "overhead better but raise peak memory per task.",

    "Requires pyarrow and pandas on every executor, not just the driver — "
    "a mismatch shows up as an ImportError inside a PythonException "
    "(case 06) rather than at submit time.",

    "Arrow silently falls back to the non-Arrow path on unsupported types "
    "(nested structs, UDTs, some decimals). Set "
    "spark.sql.execution.arrow.pyspark.fallback.enabled=false to make that "
    "fallback raise instead of quietly costing you the speedup.",

    "Verify by grepping the plan, not by timing alone: ArrowEvalPython in "
    "the plan is proof the fast path is in use. Timings on a laptop are "
    "noisy and understate the win, because the per-row overhead scales "
    "with row count and cluster round-trips.",

    "The type hints on a pandas UDF are load-bearing — Spark uses them to "
    "pick the UDF variant (Series->Series, Iterator, grouped map). Omitting "
    "them changes behaviour, unlike normal Python.",
]


@F.udf(DoubleType())
def engagement_rate_python(second, minute):
    """Row-at-a-time. One pickle round trip per row."""
    if not minute:
        return 0.0
    return float(second) / float(minute)


@pandas_udf(DoubleType())
def engagement_rate_pandas(
    second: pd.Series, minute: pd.Series
) -> pd.Series:
    """Batch-at-a-time. One Arrow transfer per few thousand rows."""
    ratio = second / minute.replace(0, pd.NA)
    return ratio.fillna(0.0).astype("float64")


def build_broken(spark, rows: int = 2_000_000):
    """Enrichment via a plain Python UDF -> BatchEvalPython."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "engagement_rate",
        engagement_rate_python(F.col("second"), F.col("min")),
    )


def build_fixed(spark, rows: int = 2_000_000):
    """The same enrichment via a pandas UDF -> ArrowEvalPython."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "engagement_rate",
        engagement_rate_pandas(F.col("second"), F.col("min")),
    )


def build_native(spark, rows: int = 2_000_000):
    """No Python worker at all — the option to check before either UDF."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "engagement_rate",
        F.when(F.col("min") == 0, F.lit(0.0)).otherwise(
            F.col("second").cast("double") / F.col("min").cast("double")
        ),
    )


def _time_execution(df) -> float:
    """Execute with a noop write and return elapsed seconds.

    A noop write forces every row through the plan without collecting
    anything to the driver, so the measurement is of the computation rather
    than of the transfer home (case 02).
    """
    start = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    return time.perf_counter() - start


def results_match(spark, rows: int = 5_000) -> bool:
    """Confirm the two implementations agree before comparing their speed.

    A faster wrong answer is not an optimisation.
    """
    broken = build_broken(spark, rows=rows).select(
        "user_id", "second", "min", "engagement_rate"
    )
    fixed = build_fixed(spark, rows=rows).select(
        "user_id", "second", "min", "engagement_rate"
    )
    # Round to absorb float representation differences between the scalar
    # and vectorised division paths.
    rounded = lambda df: df.withColumn(
        "engagement_rate", F.round("engagement_rate", 9)
    )
    return rounded(broken).exceptAll(rounded(fixed)).isEmpty()


def diagnose(spark, rows: int = 200_000) -> Diagnosis:
    """Compare plans, verify equivalence, then time both paths."""
    broken_df = build_broken(spark, rows=rows)
    fixed_df = build_fixed(spark, rows=rows)
    native_df = build_native(spark, rows=rows)

    broken_plan = capture_plan(broken_df)
    fixed_plan = capture_plan(fixed_df)
    native_plan = capture_plan(native_df)

    identical = results_match(spark)

    # Warm the JVM and the Python workers so the first-run cost of starting a
    # worker is not attributed to the UDF itself.
    _time_execution(build_broken(spark, rows=1_000))
    _time_execution(build_fixed(spark, rows=1_000))

    broken_seconds = _time_execution(broken_df)
    fixed_seconds = _time_execution(fixed_df)
    native_seconds = _time_execution(native_df)

    speedup = broken_seconds / fixed_seconds if fixed_seconds else float("nan")

    return Diagnosis(
        case_id=CASE_ID,
        title=TITLE,
        symptom=SYMPTOM,
        cause=CAUSE,
        resolution=RESOLUTION,
        broken_plan=broken_plan,
        fixed_plan=fixed_plan,
        evidence=[
            Evidence(
                look_for="Python evaluation operator",
                broken=", ".join(python_eval_nodes(broken_plan)),
                fixed=", ".join(python_eval_nodes(fixed_plan)),
            ),
            Evidence(
                look_for="BatchEvalPython nodes (the slow path)",
                broken=str(count_operator(broken_plan, "BatchEvalPython")),
                fixed=str(count_operator(fixed_plan, "BatchEvalPython")),
            ),
            Evidence(
                look_for="Built-in alternative",
                broken="n/a",
                fixed=", ".join(python_eval_nodes(native_plan))
                or "no Python worker at all",
            ),
        ],
        metrics={
            "results identical": "yes" if identical else "NO — investigate",
            f"python UDF ({rows:,} rows)": f"{broken_seconds:.2f}s",
            f"pandas UDF ({rows:,} rows)": f"{fixed_seconds:.2f}s",
            f"built-ins ({rows:,} rows)": f"{native_seconds:.2f}s",
            "speedup (pandas vs python)": f"{speedup:.1f}x",
            "caveat": (
                "~2x here, not the 10x+ often quoted. Two reasons: this UDF "
                "body is one division, so almost all the saving is "
                "serialization rather than vectorised computation; and "
                "local[*] shares memory between JVM and workers, so the "
                "transfer this fix removes is unusually cheap. Expect a "
                "wider gap with a heavier function body and real network "
                "hops. Treat the plan operator, not this timing, as the "
                "signal."
            ),
        },
        notes=NOTES,
    )


def main():
    spark = get_debug_session("Debug-07-PandasUDF", adaptive=False)
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
