"""Case 06 — a Java stack trace whose real cause is Python.

Where case 05 fails at analysis time with a clear message, this one fails
mid-execution and buries a one-line Python bug under thirty frames of Scala.
The skill being practised here is reading the trace: knowing which part is
signal and which is the runner's plumbing.

Run: ``uv run python -m spark_applications.debugging.run --case 6``
"""

import traceback

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import (
    capture_plan,
    python_eval_nodes,
)
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import get_debug_session

CASE_ID = "06"
TITLE = "PythonException — finding the real cause in a Java stack trace"

SYMPTOM = """
The job runs, processes several stages, then dies. The traceback is ~40 lines
and almost all of it is Spark internals:

    org.apache.spark.SparkException: Job aborted due to stage failure:
    Task 3 in stage 7.0 failed 4 times, most recent failure:
    Lost task 3.3 in stage 7.0 (TID 214) (10.0.4.31 executor 2):
    org.apache.spark.api.python.PythonException: Traceback (most recent call last):
      File ".../worker.py", line 830, in main
      File ".../serializers.py", line 174, in _read_with_length
      File "/app/jobs/enrich.py", line 42, in events_per_minute
        return int(second / count)
                   ~~~~~~~^~~~~~~
    ZeroDivisionError: division by zero

        at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:572)
        at org.apache.spark.sql.execution.python.BasePythonUDFRunner$$anon$1.read(PythonUDFRunner.scala:94)
        ... 28 more frames of Scala ...

It worked yesterday. It works on the sample. Retries do not help — the task
fails all four attempts, because the same bad row is in the same partition
every time.
"""

CAUSE = """
Two separate things to read out of that trace.

**Which lines matter.** Everything from `at org.apache.spark...` down is the
Python-worker plumbing that relays the failure; it is identical for every UDF
failure and says nothing about your bug. The signal is the Python traceback
*above* it, and specifically its last two lines:

    File "/app/jobs/enrich.py", line 42, in events_per_minute
    ZeroDivisionError: division by zero

That is the whole diagnosis: line 42, dividing by a zero `count`.

**Why retries do not help.** `failed 4 times` looks like a flaky-infra
signature, but a deterministic data bug fails identically on every attempt —
the offending row is routed to the same partition by the same hash. Four
identical failures is evidence of a data bug, not a transient one.

**What the trace does NOT tell you** is which row. The UDF sees values with
no row context, so the message names the exception but not the input that
caused it. That is the actual difficulty of this bug, and the reason the fix
below is about making the failure informative, not just making it stop.
"""

RESOLUTION = """
Handle the degenerate input explicitly, and make the failure identify itself
if you do raise:

    # BEFORE — dies on the first zero, says nothing about which row
    @F.udf(IntegerType())
    def events_per_minute(second, count):
        return int(second / count)

    # AFTER — the degenerate case is a decision, not an accident
    @F.udf(IntegerType())
    def events_per_minute(second, count):
        if not count:
            return None
        return int(second / count)

Better still, delete the UDF. This one is expressible in built-ins, which are
faster, never raise a PythonException, and let the optimiser see through them
(case 03):

    F.when(F.col("min") == 0, None)
     .otherwise((F.col("second") / F.col("min")).cast("int"))
"""

NOTES = [
    "Read a Spark trace from the INSIDE OUT: find the innermost Python "
    "traceback, read its last two lines, ignore the Scala frames.",

    "Where you read the trace changes what you see. PySpark strips the "
    "Scala frames from the Python-side exception by default "
    "(spark.sql.pyspark.jvmStacktrace.enabled=false), so an interactive "
    "session shows a short clean traceback while the driver/executor LOG "
    "shows the full 40-line version. Set that config to true when you need "
    "the JVM frames — chasing a failure inside a data source or a Scala "
    "UDF, where the Python half is not the interesting part.",

    "'failed 4 times' means deterministic, not flaky. Genuinely transient "
    "failures usually succeed on retry; a data bug reproduces exactly "
    "because partitioning is deterministic.",

    "To find the offending row, put the input in the error: "
    "raise ValueError(f'bad count for second={second}: {count}'). The "
    "message travels back in the PythonException, so the next failure "
    "names the data.",

    "A UDF returning None must have a nullable return type — the default "
    "for udf() — or the None becomes a confusing serialization error "
    "instead.",

    "Prefer built-in column functions to UDFs whenever the logic can be "
    "expressed in them: no Python worker, no serialization, no opaque "
    "block for the optimiser, and roughly an order of magnitude faster "
    "(case 07 measures this).",

    "spark.python.worker.faulthandler.enabled=true adds the Python "
    "faulthandler traceback for worker crashes that produce no Python "
    "exception at all (segfaults, OOM-killed workers).",

    "This repo's quality.split_on_contract (utils/quality.py) is the "
    "batch-level version of the same principle: route bad rows to a "
    "quarantine path instead of letting one row kill the job.",
]


@F.udf(IntegerType())
def events_per_minute_broken(second, count):
    """Divides without checking the denominator. Raises on count == 0."""
    return int(second / count)


@F.udf(IntegerType())
def events_per_minute_fixed(second, count):
    """Returns NULL for the degenerate case instead of raising."""
    if not count:
        return None
    return int(second / count)


def build_broken(spark, rows: int = 2_000):
    """A frame whose evaluation raises inside the Python worker."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "events_per_minute",
        events_per_minute_broken(F.col("second"), F.col("min")),
    )


def build_fixed(spark, rows: int = 2_000):
    """Same computation, degenerate input handled."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "events_per_minute",
        events_per_minute_fixed(F.col("second"), F.col("min")),
    )


def build_fixed_native(spark, rows: int = 2_000):
    """The same logic without a UDF at all — the better fix."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    return events.withColumn(
        "events_per_minute",
        F.when(F.col("min") == 0, None).otherwise(
            (F.col("second") / F.col("min")).cast("int")
        ),
    )


def extract_python_cause(error: BaseException) -> str:
    """Pull the Python cause line out of a Spark PythonException.

    This is the "read it inside out" rule as code: find the last line of the
    embedded Python traceback that names an exception type, and drop the
    Scala frames entirely.
    """
    text = "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )
    candidates = [
        line.strip()
        for line in text.splitlines()
        # Scala frames are indented with a tab and start with "at ".
        if line.strip() and not line.lstrip().startswith("at ")
    ]
    for line in reversed(candidates):
        if "Error" in line or "Exception" in line:
            if not line.startswith("org.apache.spark"):
                return line
    return candidates[-1] if candidates else "unknown"


def _failure_shape(spark, rows: int, jvm_stacktrace: bool) -> dict:
    """Trigger the failure and measure the shape of the resulting traceback.

    ``jvm_stacktrace`` toggles spark.sql.pyspark.jvmStacktrace.enabled, which
    decides whether the Scala frames are included in the exception Python
    sees.
    """
    spark.conf.set(
        "spark.sql.pyspark.jvmStacktrace.enabled", str(jvm_stacktrace).lower()
    )
    try:
        build_broken(spark, rows=rows).collect()
    except Exception as error:
        text = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        lines = text.splitlines()
        return {
            "type": type(error).__name__,
            "total": len(lines),
            "java": sum(
                1 for line in lines if line.lstrip().startswith("at ")
            ),
            "cause": extract_python_cause(error),
        }
    finally:
        spark.conf.unset("spark.sql.pyspark.jvmStacktrace.enabled")
    return {
        "type": "no exception raised",
        "total": 0,
        "java": 0,
        "cause": "n/a",
    }


def diagnose(spark, rows: int = 2_000) -> Diagnosis:
    """Trigger the real failure and show what surviving it looks like."""
    clean = _failure_shape(spark, rows, jvm_stacktrace=False)
    verbose = _failure_shape(spark, rows, jvm_stacktrace=True)

    broken_error = clean["type"]
    python_cause = clean["cause"]

    fixed_df = build_fixed(spark, rows=rows)
    native_df = build_fixed_native(spark, rows=rows)

    fixed_plan = capture_plan(fixed_df)
    native_plan = capture_plan(native_df)

    null_rows = fixed_df.filter(F.col("events_per_minute").isNull()).count()

    return Diagnosis(
        case_id=CASE_ID,
        title=TITLE,
        symptom=SYMPTOM,
        cause=CAUSE,
        resolution=RESOLUTION,
        broken_plan=capture_plan(build_broken(spark, rows=rows)),
        fixed_plan=native_plan,
        evidence=[
            Evidence(
                look_for="Exception type",
                broken=broken_error,
                fixed="none — completes",
            ),
            Evidence(
                look_for="Real cause (innermost Python line)",
                broken=python_cause,
                fixed="n/a",
            ),
            Evidence(
                look_for="Python worker node in the plan",
                broken=", ".join(python_eval_nodes(fixed_plan)) or "none",
                fixed=", ".join(python_eval_nodes(native_plan))
                or "none — built-ins need no Python worker",
            ),
        ],
        metrics={
            "python-side traceback (default)": (
                f"{clean['total']} lines, {clean['java']} Scala frames"
            ),
            "python-side traceback (jvmStacktrace=true)": (
                f"{verbose['total']} lines, {verbose['java']} Scala frames"
            ),
            "why they differ": (
                "spark.sql.pyspark.jvmStacktrace.enabled is false by "
                "default, so PySpark already strips the Scala frames from "
                "the exception Python sees. The 40-line trace in the "
                "SYMPTOM above is what lands in the DRIVER/EXECUTOR LOG — "
                "which is where you actually read it on a cluster"
            ),
            "lines that mattered": "2 (the file:line and the exception type)",
            "rows the fix nulls out": f"{null_rows:,} of {rows:,}",
            "retry behaviour": (
                "deterministic — the same row hashes to the same partition, "
                "so all 4 attempts fail identically"
            ),
        },
        notes=NOTES,
    )


def main():
    spark = get_debug_session("Debug-06-UdfTaskFailure", adaptive=False)
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
