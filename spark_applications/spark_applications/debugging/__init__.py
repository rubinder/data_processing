"""Worked Spark debugging cases: reproduce, read the plan, resolve.

Each ``case_NN_*`` module is a self-contained investigation of one failure
mode over the impression data model this repo already moves. They share one
shape — ``build_broken`` / ``build_fixed`` / ``diagnose`` — so they read
as a series:

    01  skewed join            one task never finishes
    02  driver collect / OOM   the driver dies, not an executor
    03  partition pruning      a filtered read scans the whole table
    04  shuffle + small files  a fast job writes 20,000 tiny files
    05  ambiguous column       AnalysisException after a self-join
    06  UDF task failure       the real cause is buried in a Java stack trace
    07  python UDF -> pandas   BatchEvalPython vs ArrowEvalPython

Written up in ``spark_applications/DEBUGGING.md``. Run them with::

    uv run python -m spark_applications.debugging.run --list
    uv run python -m spark_applications.debugging.run --case 1
"""
