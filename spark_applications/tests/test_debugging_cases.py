"""Integration tests for the debugging cases.

These run real Spark. Each test asserts the thing the case *claims*: that the
fix changes the plan in the stated way, and that broken and fixed produce the
same answer. If Spark's optimiser changes behaviour in a future version,
these fail — which is the point. A worked example that quietly stops being
true is worse than none.

Row counts are kept small; the plan shapes do not depend on volume.
"""

import pytest
from pyspark.sql import functions as F

from spark_applications.debugging import (
    case_01_skewed_join,
    case_02_driver_collect_oom,
    case_03_partition_pruning,
    case_04_shuffle_small_files,
    case_05_ambiguous_column,
    case_06_udf_task_failure,
    case_07_python_udf_to_pandas_udf,
    fixtures,
)
from spark_applications.debugging.explain_tools import (
    capture_plan,
    count_operator,
    exchange_count,
    exchange_partitionings,
    join_strategies,
    partition_filters,
    python_eval_nodes,
)
from spark_applications.debugging.session import apply_conf, restore_conf

ROWS = 2_000


class TestCase01SkewedJoin:
    """The fix must change the join strategy, not just the runtime."""

    def test_no_broadcast_forces_sort_merge_join(self, spark):
        previous = apply_conf(spark, case_01_skewed_join.BROKEN_CONF)
        try:
            plan = capture_plan(
                case_01_skewed_join.build_broken(spark, rows=ROWS)
            )
        finally:
            restore_conf(spark, previous)

        assert join_strategies(plan) == ["SortMergeJoin"]

    def test_broadcast_eliminates_the_shuffle_join(self, spark):
        previous = apply_conf(spark, case_01_skewed_join.FIXED_CONF)
        try:
            plan = capture_plan(
                case_01_skewed_join.build_fixed(spark, rows=ROWS)
            )
        finally:
            restore_conf(spark, previous)

        assert join_strategies(plan) == ["BroadcastHashJoin"]

    def test_fix_reduces_shuffle_count(self, spark):
        previous = apply_conf(spark, case_01_skewed_join.BROKEN_CONF)
        try:
            broken = exchange_count(
                capture_plan(
                    case_01_skewed_join.build_broken(spark, rows=ROWS)
                )
            )
        finally:
            restore_conf(spark, previous)

        previous = apply_conf(spark, case_01_skewed_join.FIXED_CONF)
        try:
            fixed = exchange_count(
                capture_plan(case_01_skewed_join.build_fixed(spark, rows=ROWS))
            )
        finally:
            restore_conf(spark, previous)

        assert fixed < broken

    def test_both_paths_agree(self, spark):
        """A faster wrong answer is not a fix."""
        previous = apply_conf(spark, case_01_skewed_join.BROKEN_CONF)
        try:
            broken = case_01_skewed_join.build_broken(
                spark, rows=ROWS
            ).collect()
        finally:
            restore_conf(spark, previous)

        previous = apply_conf(spark, case_01_skewed_join.FIXED_CONF)
        try:
            fixed = case_01_skewed_join.build_fixed(spark, rows=ROWS).collect()
        finally:
            restore_conf(spark, previous)

        assert sorted(map(tuple, broken)) == sorted(map(tuple, fixed))

    def test_hot_key_really_dominates(self, spark):
        """Guard the fixture itself: without skew the case proves nothing."""
        events = fixtures.impression_events(spark, rows=10_000, skew_ratio=0.8)
        counts = (
            events.groupBy("user_id").count()
            .orderBy(F.col("count").desc())
            .first()
        )
        assert counts["user_id"] == fixtures.HOT_USER_ID
        assert counts["count"] > 10_000 * 0.7


class TestCase02DriverCollect:
    """The bug is invisible in the plan; assert on what reaches the driver."""

    def test_broken_returns_every_row_to_the_driver(self, spark):
        df = case_02_driver_collect_oom.build_broken(spark, rows=ROWS)
        assert case_02_driver_collect_oom.total_events_broken(df) == ROWS

    def test_fixed_returns_one_row_regardless_of_input(self, spark):
        small = case_02_driver_collect_oom.build_fixed(spark, rows=100)
        large = case_02_driver_collect_oom.build_fixed(spark, rows=ROWS)
        assert small.count() == 1
        assert large.count() == 1

    def test_both_compute_the_same_total(self, spark):
        df = case_02_driver_collect_oom.build_broken(spark, rows=ROWS)
        assert case_02_driver_collect_oom.total_events_fixed(df) == ROWS


class TestCase03PartitionPruning:
    """The UDF must be shown to cost a partition dimension."""

    @pytest.fixture(autouse=True)
    def string_partition_values(self, spark):
        """Read partition values as strings, as the real jobs do.

        utils/session.py disables partition column type inference; without it
        `date` reads back as a DateType and this case measures a type
        mismatch rather than pruning.
        """
        previous = apply_conf(spark, case_03_partition_pruning.CASE_CONF)
        yield
        restore_conf(spark, previous)

    @pytest.fixture
    def table_path(self, spark, tmp_path_factory):
        path = str(tmp_path_factory.mktemp("impressions") / "table")
        return fixtures.write_impression_table(
            spark, path, rows_per_partition=100
        )

    def test_udf_predicate_is_absent_from_partition_filters(
        self, spark, table_path
    ):
        plan = capture_plan(
            case_03_partition_pruning.build_broken(spark, table_path)
        )
        filters = " ".join(partition_filters(plan))
        # The hour predicate still prunes; the UDF'd date predicate does not.
        assert case_03_partition_pruning.TARGET_DATE not in filters

    def test_direct_predicate_prunes_on_date(self, spark, table_path):
        plan = capture_plan(
            case_03_partition_pruning.build_fixed(spark, table_path)
        )
        filters = " ".join(partition_filters(plan))
        assert case_03_partition_pruning.TARGET_DATE in filters

    def test_udf_adds_a_python_worker_above_the_scan(self, spark, table_path):
        broken = capture_plan(
            case_03_partition_pruning.build_broken(spark, table_path)
        )
        fixed = capture_plan(
            case_03_partition_pruning.build_fixed(spark, table_path)
        )
        assert python_eval_nodes(broken) == ["BatchEvalPython"]
        assert python_eval_nodes(fixed) == []

    def test_casts_do_not_break_pruning(self, spark, table_path):
        """The claim that casts block pruning is false in Spark 3.5."""
        plan = capture_plan(
            spark.read.parquet(table_path).filter(
                F.col("hour").cast("int")
                == case_03_partition_pruning.TARGET_HOUR
            )
        )
        assert any("cast(hour" in item for item in partition_filters(plan))

    def test_results_are_identical(self, spark, table_path):
        broken = case_03_partition_pruning.build_broken(spark, table_path)
        fixed = case_03_partition_pruning.build_fixed(spark, table_path)
        assert broken.count() == fixed.count()
        assert broken.exceptAll(fixed).isEmpty()


class TestCase04SmallFiles:
    """The claim: fewer files, identical directory layout."""

    def test_repartition_collapses_files_per_directory(self, spark, tmp_path):
        write_and_count = case_04_shuffle_small_files._write_and_count
        broken_files, broken_dirs = write_and_count(
            case_04_shuffle_small_files.build_broken(spark, rows=ROWS),
            str(tmp_path / "broken"),
        )
        fixed_files, fixed_dirs = write_and_count(
            case_04_shuffle_small_files.build_fixed(spark, rows=ROWS),
            str(tmp_path / "fixed"),
        )

        assert broken_dirs == fixed_dirs, "layout must be unchanged"
        assert fixed_files < broken_files
        assert fixed_files == fixed_dirs, "one file per partition directory"

    def test_fix_changes_partitioning_scheme_not_shuffle_count(self, spark):
        broken = capture_plan(
            case_04_shuffle_small_files.build_broken(spark, rows=ROWS)
        )
        fixed = capture_plan(
            case_04_shuffle_small_files.build_fixed(spark, rows=ROWS)
        )

        assert "RoundRobinPartitioning" in " ".join(
            exchange_partitionings(broken)
        )
        assert "hashpartitioning" in " ".join(exchange_partitionings(fixed))
        # Spark collapses the redundant repartition, so no shuffle is added.
        assert exchange_count(fixed) == exchange_count(broken)

    def test_row_count_is_preserved(self, spark, tmp_path):
        """Compaction must not lose rows.

        Compared against the frame's own count rather than ROWS: the fixture
        divides rows evenly across date/hour combos, so the total is rounded
        down to a multiple of the combo count.
        """
        path = str(tmp_path / "roundtrip")
        df = case_04_shuffle_small_files.build_fixed(spark, rows=ROWS)
        expected = df.count()
        case_04_shuffle_small_files._write_and_count(df, path)
        assert spark.read.parquet(path).count() == expected


class TestCase05AmbiguousColumn:
    """The broken path must actually raise, and the fix must actually work."""

    def test_unaliased_self_join_select_raises(self, spark):
        from pyspark.errors.exceptions.captured import AnalysisException

        with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
            case_05_ambiguous_column.trigger_broken(spark, rows=ROWS)

    def test_aliased_join_resolves(self, spark):
        df = case_05_ambiguous_column.build_fixed(spark, rows=ROWS)
        assert df.columns == [
            "impression_id", "page_type", "first_second", "last_second",
        ]
        assert df.count() > 0

    def test_fixed_columns_are_unique(self, spark):
        df = case_05_ambiguous_column.build_fixed(spark, rows=ROWS)
        assert len(df.columns) == len(set(df.columns))

    def test_failure_is_at_analysis_time(self, spark):
        """No job runs — the error arrives before any data is read."""
        joined = case_05_ambiguous_column.build_broken(spark, rows=ROWS)
        with pytest.raises(Exception):
            # select() alone raises; no action needed.
            joined.select("page_type")


class TestCase06UdfTaskFailure:
    def test_broken_udf_raises_python_exception(self, spark):
        from pyspark.errors.exceptions.captured import PythonException

        with pytest.raises(PythonException, match="ZeroDivisionError"):
            case_06_udf_task_failure.build_broken(spark, rows=ROWS).collect()

    def test_fixed_udf_completes_and_nulls_degenerate_rows(self, spark):
        df = case_06_udf_task_failure.build_fixed(spark, rows=ROWS)
        assert df.count() == ROWS
        assert df.filter(F.col("events_per_minute").isNull()).count() > 0

    def test_native_version_needs_no_python_worker(self, spark):
        udf_plan = capture_plan(
            case_06_udf_task_failure.build_fixed(spark, rows=ROWS)
        )
        native_plan = capture_plan(
            case_06_udf_task_failure.build_fixed_native(spark, rows=ROWS)
        )
        assert python_eval_nodes(udf_plan) == ["BatchEvalPython"]
        assert python_eval_nodes(native_plan) == []

    def test_udf_and_native_agree(self, spark):
        udf_df = case_06_udf_task_failure.build_fixed(spark, rows=ROWS)
        native_df = case_06_udf_task_failure.build_fixed_native(
            spark, rows=ROWS
        )
        assert udf_df.exceptAll(native_df).isEmpty()

    def test_cause_extraction_drops_the_java_frames(self, spark):
        try:
            case_06_udf_task_failure.build_broken(spark, rows=ROWS).collect()
        except Exception as error:
            cause = case_06_udf_task_failure.extract_python_cause(error)
        else:
            pytest.fail("expected the broken UDF to raise")

        assert "ZeroDivisionError" in cause
        assert "org.apache.spark" not in cause
        assert not cause.startswith("at ")


class TestCase07PandasUdf:
    """The plan operator is the claim; the timing is only indicative."""

    def test_python_udf_uses_the_row_at_a_time_operator(self, spark):
        plan = capture_plan(
            case_07_python_udf_to_pandas_udf.build_broken(spark, rows=ROWS)
        )
        assert python_eval_nodes(plan) == ["BatchEvalPython"]

    def test_pandas_udf_uses_the_arrow_operator(self, spark):
        plan = capture_plan(
            case_07_python_udf_to_pandas_udf.build_fixed(spark, rows=ROWS)
        )
        assert python_eval_nodes(plan) == ["ArrowEvalPython"]
        assert count_operator(plan, "BatchEvalPython") == 0

    def test_native_version_uses_no_python_worker(self, spark):
        plan = capture_plan(
            case_07_python_udf_to_pandas_udf.build_native(spark, rows=ROWS)
        )
        assert python_eval_nodes(plan) == []

    def test_all_three_implementations_agree(self, spark):
        """The whole point: same answer, different cost."""
        python_df = case_07_python_udf_to_pandas_udf.build_broken(
            spark, rows=ROWS
        ).select("user_id", "second", "min", "engagement_rate")
        pandas_df = case_07_python_udf_to_pandas_udf.build_fixed(
            spark, rows=ROWS
        ).select("user_id", "second", "min", "engagement_rate")
        native_df = case_07_python_udf_to_pandas_udf.build_native(
            spark, rows=ROWS
        ).select("user_id", "second", "min", "engagement_rate")

        def rounded(df):
            return df.withColumn(
                "engagement_rate", F.round("engagement_rate", 9)
            )

        assert rounded(python_df).exceptAll(rounded(pandas_df)).isEmpty()
        assert rounded(python_df).exceptAll(rounded(native_df)).isEmpty()

    def test_pandas_udf_handles_the_zero_denominator(self, spark):
        df = case_07_python_udf_to_pandas_udf.build_fixed(spark, rows=ROWS)
        zeros = df.filter(F.col("min") == 0)
        assert zeros.count() > 0
        assert zeros.filter(F.col("engagement_rate") != 0.0).count() == 0
