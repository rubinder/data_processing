# Debugging Spark Jobs

Seven worked cases over this repo's impression data. Each one reproduces a
real failure on local Spark, shows the plan evidence that identifies it, and
applies a resolution. They are runnable and tested, so they stay true:

```bash
uv run python -m spark_applications.debugging.run --list
uv run python -m spark_applications.debugging.run --case 3
uv run python -m spark_applications.debugging.run --case 3 --diff
uv run python -m spark_applications.debugging.run --all

uv run pytest tests/test_debugging_explain_tools.py   # plan parsers, no Spark
uv run pytest tests/test_debugging_cases.py           # real Spark
```

Code in `spark_applications/debugging/`. Every number quoted below is
produced by the case that claims it.

---

## Reading a plan

Before the cases, the mechanics — most of the difficulty is knowing where to
look and what the output does not tell you.

### `explain()` prints; it does not return

`df.explain()` writes to stdout and returns `None`, so nothing can assert on
it. `explain_tools.capture_plan(df)` returns the string instead.

Modes: `simple` (the tree), `formatted` (tree plus a numbered detail block
per node — best for reading filters and join keys), `cost` (adds estimated
statistics), `extended` (all four logical stages).

### `explain()` shows a plan that may never run

This is the trap that wastes the most time. With Adaptive Query Execution on
— `utils/session.py` enables it — Spark rewrites the plan *during* execution:
coalescing shuffle partitions, splitting skewed joins, switching a sort-merge
join to a broadcast. **None of that appears in `explain()` output**, which is
why AQE so often looks like it is doing nothing.

To see what actually ran, read the executed plan after an action. It reports
`isFinalPlan=true` and carries both a `== Final Plan ==` and an
`== Initial Plan ==` section:

```python
from spark_applications.debugging.explain_tools import capture_plan, capture_final_plan

capture_plan(df)        # the initial guess
capture_final_plan(df)  # what ran, including AQE rewrites
```

There is a second wrinkle. The final plan attaches only to the **exact
DataFrame object you executed**. `df.count()` and
`df.write.format("noop").save()` each build a *new* query execution, so `df`'s
own plan stays `isFinalPlan=false` and still looks like AQE did nothing.
`capture_final_plan` uses `collect()` for this reason. (Verified in case 01:
`explain()` reports "initial plan only", the executed plan reports
`isFinalPlan=true`.)

### What to look at first

| Signal | Meaning |
| --- | --- |
| `Exchange` count | Full shuffles across the network. The single most useful number. |
| `BroadcastHashJoin` | Only the small side moves. Cheapest join. |
| `SortMergeJoin` | Both sides shuffled on the join key — where skew bites. |
| `CartesianProduct` | Almost always a bug: a missing or non-equi join condition. |
| `PartitionFilters` | Non-empty means directories skipped without being opened. |
| `PushedFilters` | Row groups skipped *inside* files that were opened. |
| `BatchEvalPython` | A plain Python UDF: row-at-a-time pickling. |
| `ArrowEvalPython` | A pandas UDF: Arrow batches. |
| `AQEShuffleRead` | AQE acted. Only ever present in a *final* plan. |
| `#123` suffixes | Expression IDs — the only reliable way to tell two same-named columns apart. |

`explain_tools.summarize_plan(plan).render()` prints all of these at once.

---

## Case 01 — Skewed join, one task never finishes

**Symptom.** Stage sits at 199/200 tasks. The straggler's shuffle-read is
~200x the median, then `ExecutorLostFailure ... Container killed by YARN for
exceeding memory limits`. More executor memory buys one more retry.

**Evidence.**

| | broken | fixed |
| --- | --- | --- |
| Join operator | `SortMergeJoin` | `BroadcastHashJoin` |
| `Exchange` nodes | 3 | 1 |

**Cause.** 80% of rows carry one `user_id`. The join shuffles on `user_id`, so
hash partitioning puts all of them in one partition. Partitioning by a key can
never be more granular than that key's distribution — parallelism and memory
cannot help.

**Resolution.** Broadcast the small side. The user dimension is a few thousand
rows; broadcasting removes the shuffle on the join key entirely, making the
skew irrelevant rather than merely survivable.

When Spark declines to broadcast a genuinely small side, it is usually because
it cannot *size* it: no statistics, or the side is itself the output of a join
or aggregation. Fix the estimate (`ANALYZE TABLE`, cache, checkpoint) or force
it with a `broadcast()` hint.

**Notes.**

- **Order of attack: broadcast, then AQE, then salting.** Salting doubles the
  code and explodes the small side by the salt factor. Earn it.
- **AQE is not a skew strategy on its own.** `adaptive.skewJoin` triggers only
  above *both* `skewedPartitionFactor` (5x median) and
  `skewedPartitionThresholdInBytes` (256MB). Measured in this case: AQE
  reported `AQEShuffleRead coalesced` three times and **did not split** the
  skewed partition, because the partitions sit under 256MB. That is the normal
  case on mid-sized jobs.
- Broadcast moves the OOM risk to the driver — see case 02.
- If both sides really are large, `salted_join.py` has the pattern.
- **Check for a degenerate key first.** A null or sentinel (`''`, `unknown`,
  `-1`) in the join column produces the same pileup and is a data bug.

**Verification.** `TestCase01SkewedJoin` — asserts the strategy change, the
shuffle reduction, that both paths return identical rows, and that the fixture
is genuinely skewed.

---

## Case 02 — Driver OOM, work that never left the driver

**Symptom.** No executor errors. The driver log ends with
`OutOfMemoryError: Java heap space` at `Dataset.collectFromPlan`, or
`Total size of serialized results ... is bigger than spark.driver.maxResultSize`.
The breaking point tracks input volume, not cluster size.

**Evidence.**

| | broken | fixed |
| --- | --- | --- |
| Rows returned to the driver | 2,000 (grows with input) | 1 (constant) |
| At 5bn input rows | 5,000,000,000 | 1 |
| Plan difference | **none** | — |

**Cause.** `collectFromPlan` is the entire diagnosis: every row went into one
JVM heap. Via `collect()`, `toPandas()`, a Python `for` loop over
`df.collect()`, or `len(df.collect())` as a row count.

**Resolution.** Keep the computation in Spark.

```python
# BEFORE — every row through the driver's heap
rows = df.collect()
total = sum(row["event_count"] for row in rows)

# AFTER — runs distributed, one row comes back
total = df.agg(F.sum("event_count")).first()[0]
```

**Notes.**

- **This case has no plan evidence, and that is the lesson.** The broken plan
  is a perfectly healthy scan; `explain()` cannot see a driver-side action.
  Some Spark bugs are only visible in the stack trace.
- **Read which JVM died before tuning anything.** Driver OOM
  (`collectFromPlan`, `maxResultSize`) = a driver-side action. Executor OOM
  (`ExecutorLostFailure`) = skew or partition sizing, i.e. case 01.
- `maxResultSize` is a guard rail, not a limit to raise. Raising it converts a
  clean error into a heap OOM.
- `toPandas()` is `collect()` with roughly double the peak memory. Arrow
  reduces conversion cost, not the fact that everything lands in the driver.
- `show()` and `take(n)` push a limit into the plan and are safe. `collect()`
  does not.
- A too-large broadcast causes driver OOM too. If this appeared right after
  someone "fixed" case 01 with a `broadcast()` hint, that hint is the cause.
- This repo shipped a version of it: `DECISIONS.md` #2, where `api_pull`
  gzip-decompressed the whole file on the driver.

**Verification.** `TestCase02DriverCollect`.

---

## Case 03 — Partition pruning lost to a UDF

**Symptom.** A job that asks for one hour reads three days. No error; scanned
bytes equal the whole table, and runtime grows with table age rather than with
the slice requested.

**Evidence.**

| | broken | fixed |
| --- | --- | --- |
| `PartitionFilters` | `isnotnull(hour)`, `(cast(hour as int) = 10)` | + `(date = 2026-01-01)` |
| Python eval above scan | `BatchEvalPython` | none |
| Scan actually read | **72 files / 9 partitions** | **24 files / 3 partitions** |
| Rows returned | 300 | 300 |

**Cause.** The date predicate goes through a Python UDF. Spark must prove a
filter is safe to evaluate against partition *metadata* before it can skip
directories; a Python UDF is opaque, so the predicate is demoted to a row-level
filter applied after everything is read.

**Resolution.** Express the predicate in built-in column functions. If the
transformation genuinely needs a UDF, apply it *after* filtering on the raw
partition columns.

**Notes.**

- **`PartitionFilters` was not empty in the broken case.** The plain `hour`
  predicate still pruned, so the list looks populated and healthy while one of
  two partition dimensions was silently lost. Check that *every* partition
  column you filtered on appears there — partial pruning is how this hides.
- **Casts do not break pruning.** Verified in Spark 3.5:
  `(cast(hour#656 as int) = 10)` appears *inside* `PartitionFilters`, and so do
  `to_date()` and `substring()`. The widespread advice to contort filters to
  avoid casts is cargo cult. `aggregation.py`'s `hour.cast("int")` is fine.
  UDFs are what actually break it.
- `PartitionFilters` and `PushedFilters` are different: the first skips
  directories without opening a file, the second skips row groups inside files
  already opened. Only the first avoids I/O entirely. A non-partition column
  can only ever produce the second.
- Filtering on a correlated non-partition column prunes nothing, however
  obvious the relationship is to a human.
- Highest-leverage thing to check on a cloud warehouse: S3/Athena/BigQuery bill
  scanned bytes, so lost pruning is a line item, not just latency.

**Verification.** `TestCase03PartitionPruning` — including
`test_casts_do_not_break_pruning`, which pins the myth-correction.

---

## Case 04 — Small-files explosion on a partitioned write

**Symptom.** The write is fast. Downstream, file *listing* takes longer than
computation, the table has thousands of ~8KB files, and S3 cost shifts to
LIST/GET requests.

**Evidence.**

| | broken | fixed |
| --- | --- | --- |
| Data files written | **432** | **27** |
| Partition directories | 27 | 27 |
| Files per directory | 16.0 | 1.0 |
| Shuffle scheme | `RoundRobinPartitioning(16)` | `hashpartitioning(page_type, date, hour, 8)` |
| `Exchange` nodes | 1 | 1 |

**Cause.** Arithmetic: `output files = shuffle partitions x partition-key
combos`. 16 tasks x 27 directories = 432 files, exactly as observed. Every task
holds a few rows for most directories and writes its own file into each.
Hour-grain partitioning multiplies directories by 24 while dividing data per
directory by 24.

**Resolution.** Repartition by the same columns you partition the write by, so
one task produces each directory. This is what `utils/storage.py::_compact`
does (`DECISIONS.md` #2).

```python
(df.repartition("page_type", "date", "hour")
   .write.partitionBy("page_type", "date", "hour").parquet(path))
```

**Notes.**

- **The fix is usually free.** Measured here: `Exchange` count is *unchanged*
  (1 → 1). Spark collapses the redundant repartition, so you are changing an
  existing shuffle's partitioning scheme from round-robin to hash, not adding a
  shuffle. It genuinely adds one only when the write follows a pipeline with no
  shuffle at all.
- Target 128MB–1GB files. Below ~10MB per-file overhead dominates.
- `repartition(cols)` controls how many **tasks** hold the data; `partitionBy`
  controls the **directory** layout. Collapsing the file count requires them to
  agree.
- `coalesce(n)` avoids a shuffle but does not redistribute, so it can leave
  partitions uneven and reduces the parallelism of the computation feeding it.
- AQE's `coalescePartitions` acts on the shuffle, not the `partitionBy`
  fan-out. It does not solve this.
- If the fix makes the write slow, the repartition created skew — case 01 in
  another hat.
- **Reconsider the grain before tuning.** Hour partitioning is worth it only if
  queries filter by hour *and* each hour holds enough data.

**Verification.** `TestCase04SmallFiles`.

---

## Case 05 — `AMBIGUOUS_REFERENCE` on a self-join

The first case that actually raises. It fails at analysis time, before a row is
read.

**Symptom.**

```
pyspark.errors.exceptions.captured.AnalysisException:
[AMBIGUOUS_REFERENCE] Reference `page_type` is ambiguous,
could be: [`page_type`, `page_type`].
```

The "could be" list names the same string twice and tells you nothing.

**Evidence.**

| | result |
| --- | --- |
| Unaliased | ``could be: [`page_type`, `page_type`]`` |
| Aliased | ``could be: [`first`.`page_type`, `last`.`page_type`]`` |
| Fixed | 4 unambiguous columns |

**Cause.** A self-join puts two columns named `page_type` in the result. They
are distinct attributes internally (the `#651` expression IDs), but
`select("page_type")` resolves by *name* and matches both. Spark refuses to
guess — correctly, since picking the wrong side would silently produce wrong
numbers.

Not specific to self-joins: any join where both sides share a non-key column
name does it. Note `on="col"` collapses the join *key* to one column, which is
why the error usually points at some other column while the key looks innocent.

**Resolution.** Alias each side before the join and qualify every reference.
Aliasing also upgrades the error message to name the actual sources.

**Notes.**

- **Alias before the join, not after.** Once the ambiguity exists in the joined
  frame, aliasing the result cannot separate the two attributes.
- Select down to the columns you need before joining — a self-join carrying
  only `impression_id` and `second` cannot collide on `page_type`. This also
  makes the join cheaper.
- Use expression IDs (`page_type#651` vs `page_type#672`) to tell them apart.
- **The scarier variant does not raise.** Carry both columns through, then
  `drop("page_type")`, and Spark drops **both**. Same root cause, silent data
  loss instead of an error.
- Do not "fix" it with a blanket `toDF(*names)` — that renames positionally and
  will swap two same-typed columns if join order changes.

**Verification.** `TestCase05AmbiguousColumn`.

---

## Case 06 — `PythonException`, finding the real cause in a Java trace

**Symptom.** ~40 lines of trace, almost all Scala:

```
org.apache.spark.SparkException: Job aborted due to stage failure:
Task 3 in stage 7.0 failed 4 times, most recent failure: ...
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/app/jobs/enrich.py", line 42, in events_per_minute
    return int(second / count)
ZeroDivisionError: division by zero
    at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(...)
    ... 28 more Scala frames ...
```

**Evidence.**

| | measured |
| --- | --- |
| Python-side traceback (default) | 19 lines, **0** Scala frames |
| Python-side traceback (`jvmStacktrace=true`) | 141 lines, **104** Scala frames |
| Lines that mattered | 2 |

**Cause.** Three things to read out of it:

1. **Which lines matter.** Everything from `at org.apache.spark...` down is
   Python-worker plumbing, identical for every UDF failure. The signal is the
   Python traceback above it, specifically its last two lines: the `file:line`
   and the exception type.
2. **Why retries do not help.** `failed 4 times` looks like flaky infra, but a
   deterministic data bug fails identically every attempt — the offending row
   hashes to the same partition each time. Four identical failures is evidence
   of a *data* bug.
3. **What it does not tell you: which row.** The UDF sees values with no row
   context.

**Resolution.** Handle the degenerate input explicitly — or better, delete the
UDF, since this logic is expressible in built-ins (`F.when(...).otherwise(...)`),
which are faster, cannot raise a `PythonException`, and are not opaque to the
optimiser (case 03).

**Notes.**

- **Read a Spark trace inside out**: find the innermost Python traceback, read
  its last two lines, ignore the Scala frames.
- **Where you read it changes what you see.** PySpark strips the Scala frames
  from the Python-side exception by default
  (`spark.sql.pyspark.jvmStacktrace.enabled=false`) — measured above as 19
  lines vs 141. An interactive session shows the short version; the
  driver/executor **log** shows the full one. Enable the config when chasing a
  failure inside a data source or Scala UDF, where the Python half is not the
  interesting part.
- **To find the offending row, put the input in the error:**
  `raise ValueError(f"bad count for second={second}: {count}")`. The message
  travels back in the `PythonException`.
- A UDF returning `None` needs a nullable return type, or the `None` becomes a
  confusing serialization error instead.
- `spark.python.worker.faulthandler.enabled=true` covers worker crashes that
  produce no Python exception at all (segfaults, OOM-killed workers).
- `quality.split_on_contract` is the batch-level version of the same
  principle: quarantine bad rows instead of letting one kill the job.

**Verification.** `TestCase06UdfTaskFailure`, including
`test_cause_extraction_drops_the_java_frames`.

---

## Case 07 — Python UDF → pandas UDF

Nothing is broken. Correct results, no errors, paying a serialization tax per
row.

**Symptom.** Stage CPU time disproportionate to input size; executor CPU near
100% while shuffle and I/O idle; most time inside a `BatchEvalPython` node;
dozens of `python3` processes beside the JVM. Rewriting the function body
changes nothing, because the time is not going into the function.

**Evidence.** (timings from one local `local[*]` run; they vary — the plan
operator is the stable signal)

| | python UDF | pandas UDF | built-ins |
| --- | --- | --- | --- |
| Plan operator | `BatchEvalPython` | `ArrowEvalPython` | none |
| 2,000,000 rows | 0.99s | 0.47s | 0.20s |
| Speedup | — | **2.1x** | 5.0x |
| Results identical | — | yes | yes |

**Cause.** A plain `@udf` runs one row at a time in a separate Python process.
Per row: pickle from the JVM, socket write, unpickle, call the function on one
value, pickle the result, send it back, unpickle. The body might be a
multiplication; the overhead around it is four serialization steps and a
process boundary.

A `@pandas_udf` moves data in Arrow batches — one transfer per few thousand
rows, no pickling, and the function is called once per batch with a
`pd.Series`, so the loop runs in NumPy's C code rather than the interpreter.

The plan names which one you have, making this a one-line audit: grep any plan
for `BatchEvalPython`.

**Resolution.**

```python
# BEFORE — BatchEvalPython, one row at a time
@udf(DoubleType())
def engagement_rate(second, minute):
    if minute == 0:
        return 0.0
    return float(second) / float(minute)

# AFTER — ArrowEvalPython, one call per Arrow batch
@pandas_udf(DoubleType())
def engagement_rate(second: pd.Series, minute: pd.Series) -> pd.Series:
    return (second / minute.replace(0, pd.NA)).fillna(0.0).astype("float64")
```

Take and return `pd.Series`; operate on whole Series; handle NULLs as pandas
`NA`.

**Notes.**

- **Ordering: built-ins > pandas UDF > Python UDF.** Measured above, built-ins
  beat the pandas UDF more than the pandas UDF beats the Python one. Check
  whether built-ins can express it before writing either.
- **A pandas UDF that calls `.apply()` is a Python UDF with extra steps.** The
  win is vectorisation, not the decorator.
- **The measured 2.1x is not the 10x+ often quoted, and the difference is
  instructive.** This UDF body is one division, so nearly all the saving is
  serialization rather than vectorised computation; and `local[*]` shares
  memory between JVM and workers, making the transfer this fix removes
  unusually cheap. Expect a wider gap with a heavier body and real network
  hops. **Treat the plan operator, not a laptop timing, as the signal.**
- Tune `spark.sql.execution.arrow.maxRecordsPerBatch` (default 10,000) if
  batches strain executor memory.
- Requires pandas and pyarrow on every **executor**, not just the driver — a
  mismatch surfaces as an `ImportError` inside a `PythonException` (case 06),
  not at submit time.
- **Arrow silently falls back** to the non-Arrow path on unsupported types
  (nested structs, UDTs, some decimals). Set
  `spark.sql.execution.arrow.pyspark.fallback.enabled=false` to make that
  raise rather than quietly cost you the speedup.
- The type hints are load-bearing: Spark picks the UDF variant from them.

**Verification.** `TestCase07PandasUdf` — asserts the operator change and that
all three implementations agree.

---

## Quick reference

| Symptom | Likely case | First thing to check |
| --- | --- | --- |
| One task of N never finishes | 01 | Join operator + key distribution |
| Executor OOM / `ExecutorLostFailure` | 01 | Skew on the shuffle key |
| Driver OOM / `maxResultSize` | 02 | `collect()` / `toPandas()` in the code |
| Scanned bytes >> slice requested | 03 | Every partition column in `PartitionFilters` |
| Downstream reads slow, thousands of files | 04 | `repartition` before `partitionBy` |
| `AMBIGUOUS_REFERENCE` | 05 | Alias both sides before the join |
| `PythonException`, failed 4 times | 06 | Innermost Python traceback line |
| Stage CPU-bound, no I/O | 07 | `BatchEvalPython` in the plan |

## Environment note

`pyspark` 3.5's pandas/pyarrow version checks import `distutils`, removed from
the stdlib in Python 3.12. `setuptools` is a declared dependency so the pandas
UDF path in case 07 works on 3.12+ interpreters as well as the project's
stated 3.10 baseline.
