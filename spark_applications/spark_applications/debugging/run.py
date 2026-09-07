"""CLI for the debugging cases.

    uv run python -m spark_applications.debugging.run --list
    uv run python -m spark_applications.debugging.run --case 3
    uv run python -m spark_applications.debugging.run --case 3 --diff
    uv run python -m spark_applications.debugging.run --all
"""

import argparse
import importlib

from spark_applications.debugging.session import get_debug_session

# Ordered so a full run reads as a progression: the four pathologies that
# only show up as slowness first, then the two that raise, then the one that
# is purely a cost.
CASES = {
    "1": ("case_01_skewed_join", "Skewed join — one task never finishes"),
    "2": ("case_02_driver_collect_oom",
          "Driver OOM — work that never left the driver"),
    "3": ("case_03_partition_pruning", "Partition pruning lost to a UDF"),
    "4": ("case_04_shuffle_small_files",
          "Small-files explosion on a partitioned write"),
    "5": ("case_05_ambiguous_column", "AMBIGUOUS_REFERENCE on a self-join"),
    "6": ("case_06_udf_task_failure",
          "PythonException — the real cause in a Java trace"),
    "7": ("case_07_python_udf_to_pandas_udf", "Python UDF -> pandas UDF"),
}


def load_case(number: str):
    """Import a case module by its number."""
    if number not in CASES:
        raise SystemExit(
            f"unknown case {number!r}; choose from {', '.join(CASES)}"
        )
    module_name, _ = CASES[number]
    return importlib.import_module(
        f"spark_applications.debugging.{module_name}"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Spark debugging case",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--case", help="case number to run (1-7)")
    group.add_argument(
        "--all", action="store_true", help="run every case in order"
    )
    group.add_argument(
        "--list", action="store_true", help="list the cases and exit"
    )
    parser.add_argument(
        "--diff",
        action="store_true",
        help="also print a unified diff of the broken and fixed plans",
    )
    parser.add_argument(
        "--plans",
        action="store_true",
        help="print the full plan text rather than the summary",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help=(
            "row count for the generated fixtures (cases 1, 2, 4-7). The "
            "laptop defaults keep partitions under AQE's 256MB skew "
            "threshold; on a cluster use 1e8+ so the runtime effects are "
            "measurable. See debugging/CLUSTER_RUN.md."
        ),
    )
    return parser.parse_args(argv)


def diagnose_with_rows(module, spark, rows: int | None):
    """Call ``module.diagnose`` passing ``rows`` only where it is accepted.

    Case 03 is driven by a table on disk rather than a row count.
    """
    import inspect

    if rows is not None and "rows" in inspect.signature(
        module.diagnose
    ).parameters:
        return module.diagnose(spark, rows=rows)
    return module.diagnose(spark)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.list:
        print("Spark debugging cases:\n")
        for number, (module_name, title) in CASES.items():
            print(f"  {number}  {title}")
            print(f"     spark_applications/debugging/{module_name}.py")
        print("\nWritten up in spark_applications/DEBUGGING.md")
        return

    numbers = list(CASES) if args.all else [args.case]

    # One session for the whole run. Each case sets and restores the configs
    # it needs, so they do not leak into one another.
    spark = get_debug_session("SparkDebuggingCases")
    try:
        for number in numbers:
            module = load_case(number)
            diagnosis = diagnose_with_rows(module, spark, args.rows)
            print(diagnosis.render())

            if args.plans:
                print("--- broken plan ---")
                print(diagnosis.broken_plan)
                print("--- fixed plan ---")
                print(diagnosis.fixed_plan)

            if args.diff and diagnosis.broken_plan and diagnosis.fixed_plan:
                print("--- plan diff ---")
                print(diagnosis.plan_diff())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
