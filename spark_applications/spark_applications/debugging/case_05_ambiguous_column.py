"""Case 05 — AnalysisException on a column that plainly exists.

The first genuinely *thrown* error in this series. It fails fast, at analysis
time, before a single row is read — which is the good news. The bad news is
that the message names a column you can see in both schemas and calls it
missing-ish, so the instinct is to go looking for a typo that isn't there.

Run: ``uv run python -m spark_applications.debugging.run --case 5``
"""

from pyspark.sql import functions as F

from spark_applications.debugging import fixtures
from spark_applications.debugging.explain_tools import capture_plan
from spark_applications.debugging.report import Diagnosis, Evidence
from spark_applications.debugging.session import get_debug_session

CASE_ID = "05"
TITLE = "AMBIGUOUS_REFERENCE — a self-join makes one name mean two things"

SYMPTOM = """
A self-join that computes each impression's first and last event fails
immediately:

    pyspark.errors.exceptions.captured.AnalysisException:
    [AMBIGUOUS_REFERENCE] Reference `page_type` is ambiguous,
    could be: [`page_type`, `page_type`].

`page_type` is right there in the schema. df.columns shows it. The confusing
part is the "could be" list, which in the unaliased case names the same
string twice and so tells you nothing about where the two came from.

Fails at analysis time, so it costs nothing at runtime — but it also means no
partial output and no stage to inspect in the UI.
"""

CAUSE = """
Joining a DataFrame to itself puts two columns named `page_type` in the
result — one from each side. They are distinct attributes internally (Spark
tracks them by expression ID, the `#651` suffixes you see in a plan), but
`df.select("page_type")` resolves by *name*, and the name matches both.
Spark refuses to guess, correctly: picking the wrong side would silently
produce wrong numbers.

This is not specific to self-joins. Any join where both sides share a column
name that is not a join key does it. Self-joins just guarantee it for every
single column at once.

Note the `on="col"` string form does NOT trigger this for the join key
itself: `a.join(b, on="impression_id")` collapses the key to one column.
That is why the error usually points at some *other* column and the join key
looks innocent.
"""

RESOLUTION = """
Alias each side and qualify every reference:

    # BEFORE — "page_type" matches two attributes
    joined = events.join(events, events.impression_id == events.impression_id)
    joined.select("page_type")

    # AFTER — each side has a name; every reference says which one it means
    first = events.alias("first")
    last = events.alias("last")
    joined = first.join(
        last, F.col("first.impression_id") == F.col("last.impression_id")
    )
    joined.select(F.col("first.page_type").alias("page_type"))

Aliasing also fixes the unhelpful message: with aliases in place the error
becomes `could be: [`first`.`page_type`, `last`.`page_type`]`, which names
the actual sources.
"""

NOTES = [
    "Alias BEFORE the join, not after. Once the ambiguity exists in the "
    "joined frame, aliasing the result cannot separate the two attributes.",

    "Select down to the columns you need before joining. A self-join that "
    "only carries impression_id and second cannot collide on page_type. "
    "This is the fix that also makes the join cheaper.",

    "Read the expression IDs in the plan (page_type#651 vs page_type#672) "
    "to tell the two apart. That suffix is how Spark distinguishes them and "
    "is the only reliable way to confirm which side a column came from.",

    "The scarier variant of this bug does NOT raise. If you carry both "
    "columns through and later drop(\"page_type\"), Spark drops BOTH. "
    "Aggregations then silently lose a column rather than failing — the "
    "same root cause, no error.",

    "Do not 'fix' this with a blanket toDF(*new_names) rename: it "
    "positionally renames and will happily swap two same-typed columns if "
    "the join order ever changes.",
]


def build_broken(spark, rows: int = 2_000):
    """Self-join with no aliases; selecting a shared column is ambiguous."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    joined = events.join(
        events, events["impression_id"] == events["impression_id"]
    )
    # Deferred: the AnalysisException is raised by this select, at analysis
    # time. Returning the joined frame lets the caller trigger it explicitly.
    return joined


def trigger_broken(spark, rows: int = 2_000):
    """Run the ambiguous select so the real exception surfaces."""
    return build_broken(spark, rows=rows).select("page_type")


def build_fixed(spark, rows: int = 2_000):
    """Alias both sides and qualify every column reference."""
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    first = events.alias("first")
    last = events.alias("last")

    return (
        first.join(
            last,
            F.col("first.impression_id") == F.col("last.impression_id"),
        )
        .select(
            F.col("first.impression_id").alias("impression_id"),
            F.col("first.page_type").alias("page_type"),
            F.col("first.second").alias("first_second"),
            F.col("last.second").alias("last_second"),
        )
    )


def _capture_error(callable_) -> str:
    """Run something expected to fail; return the exception text."""
    try:
        callable_()
    except Exception as error:
        return f"{type(error).__name__}: {str(error).strip()}"
    return "no exception raised"


def diagnose(spark, rows: int = 2_000) -> Diagnosis:
    """Trigger the real exception and contrast it with the working plan."""
    broken_error = _capture_error(lambda: trigger_broken(spark, rows=rows))

    fixed_df = build_fixed(spark, rows=rows)
    fixed_plan = capture_plan(fixed_df)

    # The aliased form produces a materially better message. Show that the
    # alias is worth adding even when it does not fix the ambiguity itself.
    events = fixtures.impression_events(spark, rows=rows, skew_ratio=0.0)
    aliased_error = _capture_error(
        lambda: events.alias("first")
        .join(
            events.alias("last"),
            F.col("first.impression_id") == F.col("last.impression_id"),
        )
        .select("page_type")
    )

    return Diagnosis(
        case_id=CASE_ID,
        title=TITLE,
        symptom=SYMPTOM,
        cause=CAUSE,
        resolution=RESOLUTION,
        fixed_plan=fixed_plan,
        evidence=[
            Evidence(
                look_for="Result of the select",
                broken=broken_error,
                fixed=f"{len(fixed_df.columns)} unambiguous columns: "
                      f"{', '.join(fixed_df.columns)}",
            ),
        ],
        metrics={
            "unaliased message": broken_error,
            "aliased message": aliased_error,
            "why alias anyway": (
                "both raise, but only the aliased message names which side "
                "each candidate came from"
            ),
            "when it is raised": (
                "analysis time — before any data is read, so there is no "
                "stage in the Spark UI to inspect"
            ),
        },
        notes=NOTES,
    )


def main():
    spark = get_debug_session("Debug-05-AmbiguousColumn", adaptive=False)
    try:
        print(diagnose(spark).render())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
