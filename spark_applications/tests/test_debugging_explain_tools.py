"""Tests for the plan parsers.

These run against captured plan text, so they need no SparkSession and are
fast. The fixtures below are real Spark 3.5 output, trimmed.
"""

import pytest

from spark_applications.debugging.explain_tools import (
    aqe_shuffle_reads,
    count_operator,
    diff_plans,
    exchange_count,
    exchange_partitionings,
    is_final_plan,
    join_strategies,
    partition_filters,
    pushed_filters,
    python_eval_nodes,
    skew_join_applied,
    summarize_plan,
)

# Real `formatted` output. Note the whole-stage-codegen "* " prefix and the
# numbered detail blocks that repeat every node name.
FORMATTED_PLAN = """== Physical Plan ==
* HashAggregate (19)
+- Exchange (18)
   +- * HashAggregate (17)
      +- * Project (16)
         +- * SortMergeJoin Inner (15)
            :- * Sort (6)
            :  +- Exchange (5)
            :     +- * Project (4)
            :        +- * Filter (3)
            :           +- * Range (1)
            +- * Sort (14)
               +- Exchange (13)
                  +- Union (12)


(1) Range [codegen id : 1]
Output [1]: [id#0L]
Arguments: Range (0, 5000, step=1, splits=Some(8))

(5) Exchange
Input [2]: [user_id#4, page_type#6]
Arguments: hashpartitioning(user_id#4, 8), ENSURE_REQUIREMENTS, [plan_id=42]

(15) SortMergeJoin [codegen id : 5]
Left keys [1]: [user_id#4]
Right keys [1]: [user_id#33]
Join type: Inner
"""

# Real post-AQE executed plan.
FINAL_PLAN = """AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(5) HashAggregate(keys=[id#0L], functions=[count(1)])
   +- *(5) SortMergeJoin [id#0L], [id#4L], Inner
      :- *(3) Sort [id#0L ASC NULLS FIRST], false, 0
      :  +- AQEShuffleRead coalesced
      :     +- ShuffleQueryStage 0
      :        +- Exchange hashpartitioning(id#0L, 8), ENSURE_REQUIREMENTS, [plan_id=160]
      +- *(4) Sort [id#4L ASC NULLS FIRST], false, 0
         +- AQEShuffleRead coalesced and skewed
            +- ShuffleQueryStage 1
               +- Exchange hashpartitioning(id#4L, 8), ENSURE_REQUIREMENTS, [plan_id=170]
+- == Initial Plan ==
   HashAggregate(keys=[id#0L], functions=[count(1)])
"""

SCAN_PLAN = """== Physical Plan ==
* Project (4)
+- * Filter (3)
   +- * ColumnarToRow (2)
      +- Scan parquet  (1)


(1) Scan parquet
Location: InMemoryFileIndex(1 paths)[file:/tmp/impressions]
PartitionFilters: [isnotnull(date#655), (date#655 = 2026-01-01), (cast(hour#656 as int) = 10)]
PushedFilters: [IsNotNull(user_id), EqualTo(user_id,user_00000000)]
ReadSchema: struct<user_id:string,impression_id:string>
"""

BATCH_UDF_PLAN = """== Physical Plan ==
* Project (4)
+- * Filter (3)
   +- BatchEvalPython [<lambda>(date#655)#666], [pythonUDF0#667]
      +- * ColumnarToRow (2)
         +- Scan parquet  (1)
"""

ARROW_UDF_PLAN = """== Physical Plan ==
* Project (3)
+- ArrowEvalPython [engagement_rate(second#12, min#13)#20], [pythonUDF0#21], 200
   +- * Range (1)
"""

ROUND_ROBIN_PLAN = """== Physical Plan ==
Exchange RoundRobinPartitioning(16), REPARTITION_BY_NUM, [plan_id=12]
+- * Project (2)
   +- * Range (1)
"""


class TestTreeParsing:
    def test_counts_each_node_once_despite_duplicate_detail_blocks(self):
        # SortMergeJoin appears in the tree AND in detail block (15).
        assert count_operator(FORMATTED_PLAN, "SortMergeJoin") == 1

    def test_finds_codegen_prefixed_operators(self):
        # "* SortMergeJoin" in formatted mode, "*(5) SortMergeJoin" in simple.
        assert count_operator(FORMATTED_PLAN, "SortMergeJoin") == 1
        assert count_operator(FINAL_PLAN, "SortMergeJoin") == 1

    def test_broadcast_exchange_is_not_a_shuffle(self):
        plan = "* Project (2)\n+- BroadcastExchange (1)\n"
        assert exchange_count(plan) == 0

    def test_exchange_count(self):
        assert exchange_count(FORMATTED_PLAN) == 3

    def test_detail_rows_are_not_counted_as_operators(self):
        # "Left keys [1]: [user_id#4]" must not register as an operator.
        assert count_operator(FORMATTED_PLAN, "Left") == 0


class TestJoinStrategies:
    def test_identifies_sort_merge_join(self):
        assert join_strategies(FORMATTED_PLAN) == ["SortMergeJoin"]

    def test_empty_when_no_join(self):
        assert join_strategies(SCAN_PLAN) == []

    def test_broadcast_hash_join(self):
        plan = "* Project (2)\n+- * BroadcastHashJoin Inner BuildRight (1)\n"
        assert join_strategies(plan) == ["BroadcastHashJoin"]


class TestAqe:
    def test_initial_plan_is_not_final(self):
        assert is_final_plan(FORMATTED_PLAN) is False

    def test_executed_plan_is_final(self):
        assert is_final_plan(FINAL_PLAN) is True

    def test_reads_aqe_shuffle_modifiers(self):
        assert aqe_shuffle_reads(FINAL_PLAN) == [
            "coalesced", "coalesced and skewed",
        ]

    def test_detects_skew_split(self):
        assert skew_join_applied(FINAL_PLAN) is True

    def test_no_skew_split_when_only_coalesced(self):
        plan = FINAL_PLAN.replace("coalesced and skewed", "coalesced")
        assert skew_join_applied(plan) is False


class TestFilters:
    def test_partition_filters_split_on_top_level_commas_only(self):
        # "(cast(hour#656 as int) = 10)" contains commas-free nesting but the
        # brackets must not terminate the list early.
        assert partition_filters(SCAN_PLAN) == [
            "isnotnull(date#655)",
            "(date#655 = 2026-01-01)",
            "(cast(hour#656 as int) = 10)",
        ]

    def test_pushed_filters(self):
        assert pushed_filters(SCAN_PLAN) == [
            "IsNotNull(user_id)",
            "EqualTo(user_id,user_00000000)",
        ]

    def test_absent_labels_yield_empty_list(self):
        assert partition_filters(FORMATTED_PLAN) == []
        assert pushed_filters(FORMATTED_PLAN) == []


class TestPythonEval:
    def test_batch_eval_python_is_the_slow_path(self):
        assert python_eval_nodes(BATCH_UDF_PLAN) == ["BatchEvalPython"]

    def test_arrow_eval_python_is_the_fast_path(self):
        assert python_eval_nodes(ARROW_UDF_PLAN) == ["ArrowEvalPython"]

    def test_none_when_no_udf(self):
        assert python_eval_nodes(SCAN_PLAN) == []


class TestExchangePartitionings:
    def test_round_robin(self):
        assert exchange_partitionings(ROUND_ROBIN_PLAN) == [
            "RoundRobinPartitioning(16)"
        ]

    def test_hash_partitioning(self):
        schemes = exchange_partitionings(FORMATTED_PLAN)
        assert schemes == ["hashpartitioning(user_id#4, 8)"]


class TestDiff:
    def test_diff_reports_changed_lines(self):
        diff = diff_plans("SortMergeJoin\nExchange", "BroadcastHashJoin")
        assert "-SortMergeJoin" in diff
        assert "+BroadcastHashJoin" in diff

    def test_identical_plans_produce_empty_diff(self):
        assert diff_plans(SCAN_PLAN, SCAN_PLAN) == ""


class TestSummary:
    def test_summary_collects_the_headline_numbers(self):
        summary = summarize_plan(FORMATTED_PLAN)
        assert summary.joins == ["SortMergeJoin"]
        assert summary.exchanges == 3
        assert summary.is_final is False

    def test_render_marks_a_full_scan(self):
        assert "full scan" in summarize_plan(FORMATTED_PLAN).render()


def test_capture_plan_rejects_unknown_mode(spark):
    df = spark.range(1)
    with pytest.raises(ValueError, match="mode must be one of"):
        from spark_applications.debugging.explain_tools import capture_plan

        capture_plan(df, mode="nonsense")
