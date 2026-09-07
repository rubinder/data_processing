"""Capture and inspect Spark query plans.

Debugging a Spark job almost always comes down to reading the physical plan
and answering three questions: *what did the optimiser actually decide*, *how
much data movement did it commit to*, and *how much of the work got pushed
down to the scan*. These helpers make that readable in code and assertable in
tests.

Two traps this module exists to work around:

1. **``df.explain()`` prints; it does not return.** Anything that wants to
   assert on a plan has to capture it. ``capture_plan`` returns the string.

2. **``explain()`` shows the *initial* plan, not what ran.** With Adaptive
   Query Execution on (this repo enables it in ``utils/session.py``) the
   optimiser rewrites the plan *during* execution — coalescing shuffle
   partitions, splitting skewed joins, switching a sort-merge join to a
   broadcast. None of that is in ``explain()`` output, which is why people
   conclude "AQE isn't working" when it is. ``capture_final_plan`` runs the
   query and returns the post-AQE plan, which reports ``isFinalPlan=true``
   and carries both a ``== Final Plan ==`` and an ``== Initial Plan ==``
   section.

   Worse, the final plan is only attached to the *exact DataFrame object* you
   executed. ``df.count()`` and ``df.write.format("noop").save()`` both build
   a **new** query execution, so ``df``'s own plan stays ``isFinalPlan=false``
   and looks like AQE did nothing. ``capture_final_plan`` uses ``collect()``,
   which executes ``df``'s own query execution. See
   ``case_01_skewed_join.py`` for what this looks like in practice.

The parsing helpers take plan *text*, not DataFrames, so they unit-test
against captured fixtures with no SparkSession at all
(``tests/test_debugging_explain_tools.py``).
"""

import difflib
import io
import re
from contextlib import redirect_stdout
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

# Modes accepted by DataFrame.explain / PythonSQLUtils.explainString.
EXPLAIN_MODES = ("simple", "extended", "codegen", "cost", "formatted")

# Physical join operators, worst-to-best in the sense that matters when a job
# is slow: a cartesian product is a bug, a sort-merge join means a full
# shuffle of both sides, a broadcast join means only the small side moves.
JOIN_OPERATORS = (
    "CartesianProduct",
    "BroadcastNestedLoopJoin",
    "SortMergeJoin",
    "ShuffledHashJoin",
    "BroadcastHashJoin",
)

# Nodes that hand rows to a Python worker. BatchEvalPython pickles row-at-a-
# time; ArrowEvalPython ships Arrow batches. Seeing the former where you
# expected the latter is the whole of case_07.
PYTHON_EVAL_OPERATORS = (
    "BatchEvalPython",
    "ArrowEvalPython",
    "MapInPandas",
    "FlatMapGroupsInPandas",
    "AggregateInPandas",
    "WindowInPandas",
)


def capture_plan(df: DataFrame, mode: str = "formatted") -> str:
    """Return ``df``'s initial physical plan as a string.

    This is the pre-execution plan. Under AQE it is *not* what runs — use
    :func:`capture_final_plan` for that.
    """
    if mode not in EXPLAIN_MODES:
        raise ValueError(
            f"mode must be one of {EXPLAIN_MODES}, got {mode!r}"
        )
    try:
        jvm = df.sparkSession._jvm
        return str(
            jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)
        )
    except Exception:
        # Private JVM entry points move between Spark versions; falling back
        # to capturing what explain() prints keeps this working regardless.
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            df.explain(mode=mode)
        return buffer.getvalue()


def capture_final_plan(df: DataFrame) -> str:
    """Execute ``df`` and return the plan that actually ran (post-AQE).

    Uses ``collect()`` deliberately: it executes *this* DataFrame's query
    execution, so the AQE rewrites land on the plan we then read back.
    ``count()`` would build a different query execution and leave this plan
    marked ``isFinalPlan=false``.

    Only call this on DataFrames small enough to land in the driver — that is
    the point of :mod:`case_02_driver_collect_oom`. For a large result, apply
    a ``limit`` first or execute it with a ``noop`` write and read the plan
    off the DataFrame you wrote.
    """
    df.collect()
    return str(df._jdf.queryExecution().executedPlan().toString())


def is_final_plan(plan: str) -> bool:
    """True if ``plan`` is a post-AQE plan rather than the initial guess."""
    return "isFinalPlan=true" in plan


def _tree_lines(plan: str) -> list[str]:
    """Return only the operator-tree lines of a plan.

    ``formatted`` mode prints every node twice: once in the tree at the top
    (``+- Exchange (3)``) and once as a numbered detail block below
    (``(3) Exchange``). Counting raw substring hits therefore double-counts.
    Tree lines are the ones whose first non-whitespace content is the
    operator itself, optionally behind the connectors Spark draws the tree
    with: ``+-``, ``:-``, ``:``, and the whole-stage-codegen marker, which is
    ``*(5)`` in ``simple``/``extended`` mode but a bare ``*`` in ``formatted``
    mode. Both spellings have to be allowed or every codegen'd operator
    (which is most of them, including the joins) is missed.
    """
    pattern = re.compile(r"^[\s:+\-|]*(?:\*(?:\(\d+\))?\s*)*(?=[A-Za-z])")
    lines = []
    for line in plan.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        # Skip the numbered detail headers of formatted mode: "(3) Exchange".
        if re.match(r"^\(\d+\)\s", stripped):
            continue
        # Skip the key/value detail rows: "Arguments: ...", "Output [1]: ...".
        if re.match(r"^[A-Za-z ]+(\s\[\d+\])?:", stripped):
            continue
        if pattern.match(line):
            lines.append(line)
    return lines


def count_operator(plan: str, operator: str) -> int:
    """Count times ``operator`` appears in the plan's operator tree."""
    pattern = re.compile(rf"\b{re.escape(operator)}\b")
    return sum(1 for line in _tree_lines(plan) if pattern.search(line))


def join_strategies(plan: str) -> list[str]:
    """Return the join operators present, most-expensive first.

    An empty list where you expected a join usually means the join was
    optimised away — or that you are reading a plan fragment.
    """
    return [
        operator
        for operator in JOIN_OPERATORS
        if count_operator(plan, operator) > 0
    ]


def exchange_count(plan: str) -> int:
    """Count ``Exchange`` nodes — each is a full shuffle across the network.

    The single most useful number in a plan. Two joins on the same key should
    not produce four exchanges; if they do, the keys disagree somewhere.
    """
    return count_operator(plan, "Exchange")


def exchange_partitionings(plan: str) -> list[str]:
    """Return the partitioning scheme of each shuffle in the plan.

    Counting exchanges tells you how much data moves; this tells you *how* it
    is divided when it lands, which is what decides skew and output file
    layout. ``RoundRobinPartitioning(16)`` spreads rows evenly with no regard
    for their keys; ``hashpartitioning(page_type, date, hour, 8)`` co-locates
    every row sharing those keys in one task.
    """
    schemes = []
    for match in re.finditer(
        r"\b(hashpartitioning|RoundRobinPartitioning|rangepartitioning|"
        r"SinglePartition)\b(\([^)]*\))?",
        plan,
    ):
        scheme = match.group(1) + (match.group(2) or "")
        if scheme not in schemes:
            schemes.append(scheme)
    return schemes


def aqe_shuffle_reads(plan: str) -> list[str]:
    """Return the modifier on each ``AQEShuffleRead`` node.

    Values are things like ``coalesced``, ``local``, ``skewed``. Present only
    in a final plan (see :func:`capture_final_plan`) — their absence in an
    ``explain()`` dump says nothing about whether AQE ran.
    """
    return [
        match.group(1).strip()
        for match in re.finditer(r"AQEShuffleRead\s*([^\n+]*)", plan)
    ]


def skew_join_applied(plan: str) -> bool:
    """True if AQE split a skewed partition in this (final) plan."""
    return any("skew" in read.lower() for read in aqe_shuffle_reads(plan))


def python_eval_nodes(plan: str) -> list[str]:
    """Return the Python-worker evaluation nodes present in the plan.

    ``BatchEvalPython`` means a plain Python UDF: every row is pickled to a
    Python process and back. ``ArrowEvalPython`` means a pandas UDF: rows move
    in Arrow batches and the function is called once per batch.
    """
    return [
        operator
        for operator in PYTHON_EVAL_OPERATORS
        if count_operator(plan, operator) > 0
    ]


def _balanced_bracket_list(plan: str, label: str) -> list[str]:
    """Extract ``label: [a, b, c]`` from a plan, splitting on top-level commas.

    Hand-rolled rather than a regex because filter expressions nest brackets
    and parentheses: ``[isnotnull(hour#3), (cast(hour#3 as int) = 10)]``.
    """
    marker = f"{label}:"
    start = plan.find(marker)
    if start == -1:
        return []
    open_bracket = plan.find("[", start)
    if open_bracket == -1:
        return []

    depth = 0
    for index in range(open_bracket, len(plan)):
        char = plan[index]
        if char in "[(":
            depth += 1
        elif char in "])":
            depth -= 1
            if depth == 0:
                inner = plan[open_bracket + 1:index]
                return _split_top_level(inner)
    return []


def _split_top_level(text: str) -> list[str]:
    """Split on commas that are not inside brackets or parentheses."""
    parts, depth, current = [], 0, []
    for char in text:
        if char in "[(":
            depth += 1
        elif char in "])":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return [part for part in parts if part]


def partition_filters(plan: str) -> list[str]:
    """Return the scan's ``PartitionFilters``.

    Non-empty means Spark will skip partition directories without opening
    them. Empty on a partitioned table means a full scan — see case_03.
    """
    return _balanced_bracket_list(plan, "PartitionFilters")


def pushed_filters(plan: str) -> list[str]:
    """Return the scan's ``PushedFilters`` (evaluated by the file reader)."""
    return _balanced_bracket_list(plan, "PushedFilters")


def scan_locations(plan: str) -> list[str]:
    """Return the file-scan ``Location`` lines, truncated by Spark itself."""
    return [
        match.group(1).strip()
        for match in re.finditer(r"Location:\s*(.+)", plan)
    ]


def scan_metrics(df: DataFrame) -> list[dict]:
    """Execute ``df`` and return each file scan's runtime metrics.

    Gives the numbers a plan cannot: ``numFiles`` and ``numPartitions``
    actually opened, ``filesSize`` bytes read, ``numOutputRows``. This is the
    hard evidence that partition pruning did or did not happen — the plan
    shows Spark's *intent*, these show the result.

    Reaches into private JVM plan internals, so it is written to degrade
    rather than fail: returns ``[]`` if the plan shape or metric API differs.
    Never assert on it without also asserting on the plan text.
    """
    try:
        df.collect()
        root = df._jdf.queryExecution().executedPlan()
    except Exception:
        return []

    collected: list[dict] = []

    def walk(node) -> None:
        try:
            name = str(node.nodeName()).strip()
            if "Scan" in name:
                values = {}
                iterator = node.metrics().iterator()
                while iterator.hasNext():
                    entry = iterator.next()
                    values[str(entry._1())] = entry._2().value()
                if values:
                    collected.append({"node": name, **values})
            children = node.children()
            for index in range(children.length()):
                walk(children.apply(index))
        except Exception:
            return

    walk(root)
    return collected


def diff_plans(before: str, after: str, label_before: str = "broken",
               label_after: str = "fixed", context: int = 2) -> str:
    """Unified diff between two plans, for showing what a fix changed."""
    lines = difflib.unified_diff(
        before.splitlines(),
        after.splitlines(),
        fromfile=label_before,
        tofile=label_after,
        lineterm="",
        n=context,
    )
    return "\n".join(lines)


@dataclass
class PlanSummary:
    """The numbers worth looking at before reading a plan in full."""

    joins: list[str] = field(default_factory=list)
    exchanges: int = 0
    aqe_reads: list[str] = field(default_factory=list)
    python_eval: list[str] = field(default_factory=list)
    partition_filters: list[str] = field(default_factory=list)
    pushed_filters: list[str] = field(default_factory=list)
    is_final: bool = False

    def render(self) -> str:
        """Format as an aligned block for terminal output."""
        rows = [
            ("post-AQE final plan",
             "yes" if self.is_final else "no (initial)"),
            ("join strategy", ", ".join(self.joins) or "-"),
            ("shuffles (Exchange)", str(self.exchanges)),
            ("AQE shuffle reads", ", ".join(self.aqe_reads) or "-"),
            ("python eval", ", ".join(self.python_eval) or "-"),
            ("partition filters",
             ", ".join(self.partition_filters)
             or "- (none: full scan)"),
            ("pushed filters", ", ".join(self.pushed_filters) or "-"),
        ]
        width = max(len(name) for name, _ in rows)
        return "\n".join(
            f"  {name:<{width}} : {value}" for name, value in rows
        )


def summarize_plan(plan: str) -> PlanSummary:
    """Reduce a plan to the numbers that usually identify the problem."""
    return PlanSummary(
        joins=join_strategies(plan),
        exchanges=exchange_count(plan),
        aqe_reads=aqe_shuffle_reads(plan),
        python_eval=python_eval_nodes(plan),
        partition_filters=partition_filters(plan),
        pushed_filters=pushed_filters(plan),
        is_final=is_final_plan(plan),
    )
