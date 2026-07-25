# The context, and every optimization, explained

[README.md](README.md) reports *what* the tuning achieved. This document
explains *why* each change works, in enough depth that you could apply the
reasoning to a different schema rather than copying these particular
decisions.

It is written to be read top to bottom. The mechanism section is not optional
background — every optimization below is a direct consequence of it, and
skipping it makes the rest look like a list of tricks.

**Contents**

1. [The context: what this system does](#1-the-context-what-this-system-does)
2. [The workload: four queries, two audiences](#2-the-workload-four-queries-two-audiences)
3. [The mechanism: how ClickHouse actually reads](#3-the-mechanism-how-clickhouse-actually-reads)
4. [Optimization 1 — physical types, LowCardinality, codecs](#4-optimization-1--physical-types-lowcardinality-codecs)
5. [Optimization 2 — the sorting key](#5-optimization-2--the-sorting-key)
6. [Optimization 3 — partitioning](#6-optimization-3--partitioning)
7. [Optimization 4 — skipping indexes](#7-optimization-4--skipping-indexes)
8. [Optimization 5 — materialized views](#8-optimization-5--materialized-views)
9. [The alternative — projections](#9-the-alternative--projections)
10. [How to choose: a decision procedure](#10-how-to-choose-a-decision-procedure)
11. [Measuring honestly](#11-measuring-honestly)
12. [What generalizes and what does not](#12-what-generalizes-and-what-does-not)

---

## 1. The context: what this system does

A conversational-AI platform runs AI agents on behalf of many brands. A
customer of one of those brands opens a conversation — on chat, voice, email,
SMS, or an API — and the AI agent tries to resolve it. Sometimes it does.
Sometimes it hands off to a human.

Every step emits an event:

| `event_type` | Meaning |
| --- | --- |
| `conversation_started` | a customer opened a conversation |
| `message_sent` | the customer said something |
| `agent_response` | the agent replied — carries the model used, its latency, and token counts |
| `resolution` | the agent closed the conversation successfully |
| `escalation` | handed to a human, with a reason |

Three things about this domain drive every decision that follows.

**It is multi-tenant.** Every event carries an `account_id` — the brand. Brands
see their own data and nobody else's. This single fact determines the physical
layout of the table, because it means almost every query in the system carries
`WHERE account_id = ?`.

**Traffic is heavily skewed.** In this dataset the largest tenant holds about a
third of all events, and the smallest hold a rounding error. That is realistic
for B2B: a handful of enterprise logos dominate. It matters because a benchmark
run against an *average* tenant measures a query nobody complains about. All
the numbers here are measured against the busiest tenant, which is the one
whose dashboard is actually slow.

**Latency has a long tail.** About 92% of agent responses are fast, 7% are
slow, and 1% are pathological — a tool call, a model retry, a cold start.
That tail is the entire reason anyone looks at these dashboards, so the
aggregates must report p95 and p99, not averages. An average hides exactly the
thing being investigated.

The events land in Kafka, are ingested into ClickHouse, and are served to
dashboards through a FastAPI service with a target of **p95 under 100 ms**.

---

## 2. The workload: four queries, two audiences

Optimization is meaningless without knowing which queries must be fast. There
are two audiences and four representative queries, and — this is the key point
— **no single optimization helps all four**.

**The customer** opens their own dashboard:

- **Q1 — tenant dashboard.** One account, last 7 days, grouped by day:
  conversation volume, resolution rate, escalation rate, latency percentiles.
  This is the landing page. It loads on every session.
- **Q2 — conversation drill-down.** A support engineer pastes a conversation
  ID and wants every event on it, in order. One needle in tens of millions of
  rows.

**The platform's own on-call engineer** watches the fleet:

- **Q3 — platform-wide health.** *All* tenants, last 7 days. No account filter
  at all — which, as we will see, changes everything about which optimizations
  apply.
- **Q4 — slow-response triage.** Within one account and window, only responses
  over 5 seconds, grouped by model and channel. "Which model is blowing the
  SLO this week?"

Q1 and Q4 filter by tenant. Q3 does not. Q2 filters by neither tenant nor
time. Those differences are why the optimizations below each help exactly one
or two of them.

---

## 3. The mechanism: how ClickHouse actually reads

Everything below follows from four facts about the storage engine.

**1. Storage is columnar.** Each column lives in its own file. A query reads
only the columns it names. The tenant dashboard touches 4 of 18 columns, so it
reads roughly a fifth of the row's bytes — before any indexing at all.

**2. Rows are physically sorted by the sorting key.** `ORDER BY (a, b)` is not
a hint or an index built on the side; it is the order the rows are written to
disk in. This is the single most consequential line in a ClickHouse schema.

**3. The primary index is sparse, and indexes granules — not rows.** Rows are
grouped into **granules** of 8192 (the `index_granularity` setting). ClickHouse
stores one index entry — a **mark** — per granule, holding the sorting-key
values at its first row. A table of 12 million rows has ~1,500 marks, small
enough to sit in memory.

   To answer `WHERE account_id = 'acct_0000' AND event_ts >= X`, ClickHouse
   binary-searches the marks for the range of granules that could contain
   matches, and reads only those. Everything else is never touched. This is why
   the unit of work is the granule, and why the honest measure of an
   optimization is **granules read**, not milliseconds.

   A granule is the minimum read. Even a query matching one row reads all 8192
   rows of its granule. That is the floor.

**4. Data lives in immutable parts, merged in the background.** Each `INSERT`
creates a **part** — a self-contained directory with its own column files and
its own primary index. A background process merges parts into larger ones.
`PARTITION BY` splits parts by a key, so a part never spans two partitions.

   Two consequences that matter later: **every part is examined separately**
   (its own index, its own granule ranges), so part count is a per-query cost;
   and a partition can be dropped instantly as a directory, which is why
   retention should align to it.

With that model, each optimization below is a specific answer to "how do I
make the engine read fewer granules, or make each granule cheaper?"

---

## 4. Optimization 1 — physical types, LowCardinality, codecs

**Measured: 181 ms → 49 ms on Q1 (3.7×). Storage 1003 MB → 405 MB.
Granules read: unchanged.**

### The problem

The naive schema stores everything as `String`, including timestamps, and
buries the numeric measures in a JSON blob:

```sql
event_ts  String,   -- '2026-06-01T09:15:22.431'
payload   String    -- '{"latency_ms":812,"prompt_tokens":1904,...}'
```

This reads and writes correctly and nobody notices at small volume. Its cost
is invisible until the dataset outgrows the page cache.

### Why it is slow

Three separate taxes, all paid per row:

- **Parsing.** `parseDateTimeBestEffort` on a text timestamp is dramatically
  more expensive than comparing a 64-bit integer. In the naive query it is the
  single most expensive operation.
- **Bytes.** A UUID as text is 36 bytes plus a length prefix; as a `UUID` it
  is 16 fixed bytes. A timestamp as ISO text is ~23 bytes; as `DateTime64(3)`
  it is 8.
- **Read amplification from the JSON blob.** Columnar storage's core promise is
  that you only read the columns you need. A JSON blob defeats it: to read
  `latency_ms` you must read, decompress, and parse *every* measure for every
  row. The blob is a row-store hiding inside a column store.

### What was changed, and why each thing works

| Change | Mechanism |
| --- | --- |
| `DateTime64(3)` instead of ISO text | removes parsing from the hot path; comparison becomes integer comparison |
| `UUID` instead of `String` | 16 fixed bytes, no length prefix, no variable-width decoding |
| `LowCardinality(String)` on dimensions | stores a dictionary index per row instead of the string; `GROUP BY` compares small integers, and the dictionary is usually cache-resident |
| `CODEC(Delta, ZSTD)` on timestamps | event streams are near-monotonic, so successive deltas are tiny and compress to almost nothing |
| `CODEC(T64, ZSTD)` on numerics | T64 transposes the bit planes of a bounded integer range; latency and token counts never approach `UInt32`'s range, so the high bytes are all zeros and vanish |
| JSON blob unpacked into real columns | a query needing latency reads the latency column and nothing else |

### The result, and its limit

Bytes read on Q1 fell from **2012 MB to 168 MB** — a 12× reduction — for a
3.7× latency win. The gap between those two numbers is the point: this stage
makes each row *cheaper*, but the engine still visits all 12 million of them.
`ORDER BY` is still `tuple()`, so there is no index to skip anything with.

**This is the ceiling of "make it cheaper" without "read less."** Everything
after this is about reading less.

`LowCardinality` has a threshold, incidentally: it wins below roughly 10,000
distinct values and becomes a liability above it, where the dictionary stops
fitting comfortably and adds indirection for no benefit. `account_id`,
`channel`, `model`, and `intent` are well under. `user_id` and
`conversation_id` are not, and are left as plain types.

---

## 5. Optimization 2 — the sorting key

**Measured: 49 ms → 7.1 ms on Q1 (25× cumulative). Granules read: 246/246 →
2/245. Rows read: 12,000,000 → 90,112.**

This is the highest-leverage decision in a ClickHouse schema, and the numbers
show why: it is the first change that makes the engine *skip* data.

```sql
ORDER BY (account_id, event_ts)
```

### Why this order

**`account_id` leads** because essentially every customer-facing query is
scoped to one tenant. Putting it first makes one tenant's rows physically
contiguous, so a tenant-scoped query becomes a single mark range. It also
makes per-tenant deletes and GDPR erasure cheap, since the affected rows are
adjacent rather than smeared across the table.

**`event_ts` is second** because within a tenant, every dashboard asks for a
time window. Sorted by time inside a tenant, a 7-day window is one contiguous
run of granules — hence **2 granules out of 245**.

### Why `event_type` is deliberately excluded

This is the interesting part, and the place where the usual advice misleads.

The common rule of thumb is "order sorting-key columns by ascending
cardinality." `event_type` has five values, so that rule would put it first.
It would be a serious mistake.

Placing `event_type` before `event_ts` shatters each tenant's time ordering
into five interleaved runs — all the `agent_response` rows, then all the
`escalation` rows, and so on. A time-window query that does *not* filter on
`event_type` (which is most of them, including Q1) would then have to scan five
disjoint granule ranges instead of one.

**The real rule is: match the filter prefix of the query you must make fast.**
The primary index can only prune on a *prefix* of the sorting key. A column
that is not in the prefix used by your dominant query is not helping, and a
column placed ahead of a needed one actively hurts. Cardinality ordering is a
tiebreaker between columns that are *all* in the prefix — not the primary
criterion.

### Where it does not help

Look at Q3 (platform-wide, no account filter): the sorting key drops it only
from 246/246 granules to **201/245**. With no predicate on the leading column,
ClickHouse cannot binary-search; the time filter is on the *second* key column,
which prunes only weakly because every tenant's time range overlaps every
other's. Same table, same index, ~18% pruning instead of 99%.

And Q2, the conversation drill-down: **245/245 granules** — the sorting key
does nothing at all, because `conversation_id` is not in it.

One optimization, three completely different outcomes. That is the argument
for choosing per access pattern.

---

## 6. Optimization 3 — partitioning

**Measured: Q3 21.3 ms → 15.6 ms, granules 201/245 → 73/247. Q1: no change.**

`PARTITION BY toYYYYMM(event_ts)` splits the table into directories by month.
Partitioning is **not** a second index — it is a coarser mechanism that runs
*before* index analysis, discarding whole partitions whose key range cannot
match.

### What it is actually for

Its query benefit shows up precisely where the sorting key fails: **Q3, the
cross-tenant query.** With no tenant predicate the primary index is nearly
useless, but the partition key is on time, and the query filters on time. Rows
read fall from 4.1M to 2.5M.

Its *operational* benefit is larger than its query benefit, and is the real
reason to partition:

- **Retention becomes `DROP PARTITION`** — a metadata operation that unlinks a
  directory, instant and merge-free. The alternative, `DELETE`, is a mutation
  that rewrites every affected part.
- **Backfills and corrections are scoped.** Rebuilding one month's aggregates
  does not touch the rest.

### The mistake I made, and the measurement that caught it

I first chose **weekly** partitions, reasoning that a 7-day dashboard window
should map to one partition. That reasoning counted the benefit and ignored
the cost, and it was wrong.

Recall mechanism fact #4: every surviving part is examined separately. More
partitions means more parts means more per-part work — index lookup, granule
range computation, file handles — even when the number of rows read is
identical.

`benchmarks/bench_partitioning.py` builds the same 20M rows four ways and runs
the *same* tenant query over the *same* 147k rows:

| Partition key | Parts | p50 | vs unpartitioned |
| --- | ---: | ---: | ---: |
| none | 1 | 7.3 ms | — |
| monthly | 3 | 10.6 ms | 1.5× slower |
| weekly | 14 | 19.6 ms | 2.7× slower |
| daily | 91 | **79.8 ms** | **11× slower** |

That is roughly **0.8 ms of fixed cost per part**. And on the cross-tenant
query, finer partitioning did cut rows read (4.7M → 1.6M going from none to
weekly) but per-part overhead ate the entire gain — wall time stayed flat
around 25 ms, then went 3.5× worse at daily.

So the shipped schema partitions **monthly**: coarse enough to keep the part
count in single digits at 90-day retention, fine enough that expiry is still a
`DROP PARTITION`. The sorting key does the pruning; partitioning does
retention.

**`PARTITION BY account_id` was rejected outright.** With thousands of tenants
it creates thousands of tiny partitions, destroying merge efficiency and
guaranteeing the per-part cost above. Tenant isolation belongs in the sorting
key. This is a common and expensive mistake.

### The rule worth taking away

Partition granularity is a trade between pruning benefit and per-part cost, and
**both sides must be measured**. At this volume monthly wins. At billions of
events per day, each daily partition is enormous, the per-part overhead is
negligible against the work inside it, and daily becomes correct. Reaching for
daily by reflex — which is the folklore default — is how you get the 11× row
in that table.

---

## 7. Optimization 4 — skipping indexes

**Measured: Q2 10.8 ms → 3.7 ms (granules 247/247 → 4/247, a 62× reduction).
Q1, Q3, Q4: no change.**

### The problem this solves

Q2 is the query neither previous optimization touched. A support engineer has
one `conversation_id` — a UUID with tens of millions of distinct values — and
wants its events. The sorting key cannot help, and adding `conversation_id` to
the sorting key is not an option: it would destroy the tenant/time ordering
that Q1, Q3, and Q4 all depend on.

A table can only be sorted one way. Skipping indexes are how you get partial
help for a second access pattern without a second copy of the data.

### How a skipping index works

It is not a lookup structure like a B-tree. It stores a small **summary per
group of granules**, and at query time discards any granule range whose summary
proves no match can exist. It can only ever produce false positives (a granule
read unnecessarily), never false negatives.

```sql
INDEX idx_conversation conversation_id TYPE bloom_filter(0.01) GRANULARITY 1
```

- **`bloom_filter`** — a probabilistic set membership summary. Ask "could this
  granule contain this UUID?" and get back "definitely not" or "maybe."
- **`0.01`** — a 1% false positive rate. A false positive costs one wasted
  granule read; a smaller rate costs memory in every part, forever. 1% is the
  right trade for a needle lookup.
- **`GRANULARITY 1`** — one bloom filter per granule: the finest and most
  selective setting. Higher values summarize several granules together, which
  is cheaper to store but prunes more coarsely.

Why it works so well here: a conversation's events are all written within a few
seconds of each other, so they land in a handful of adjacent granules. The
filter rejects everything else — **4 granules out of 247**.

### The negative result: minmax on `latency_ms`

I also added a `minmax` index on `latency_ms`, intending it to accelerate Q4
("responses slower than 5 s"). It does nothing at all. Compare v3 and v4 in the
Q4 results: identical rows read (90,112), identical latency.

The reason is worth internalizing. A `minmax` index stores the smallest and
largest value per granule and skips a granule when the predicate falls outside
that range. Agent latency's slow tail is spread *uniformly* across the dataset
— roughly 1% of rows everywhere. With 8192 rows per granule, essentially every
granule contains some row above 5 s, so every granule's recorded maximum
exceeds the threshold, and nothing is ever skipped.

**A skipping index only pays when the indexed column correlates with physical
row order.** Uncorrelated, it is pure write-side cost: built on every insert
and rebuilt on every merge, for zero read benefit. It was removed from the
shipped schema and deliberately kept in the benchmark, so the measurement keeps
demonstrating that it does nothing.

The general lesson: an index you did not measure is a liability you have not
noticed. "It might help someday" is how ingest gets slowly more expensive.

---

## 8. Optimization 5 — materialized views

**Measured: Q1 5.4 ms → 2.4 ms (rows read: 90,112 → 128). Q3 14.9 ms → 2.0 ms
(rows read: 12,000,000 → 91).**

Everything so far makes the scan cheaper or smaller. This removes the scan.

### What a ClickHouse materialized view actually is

Not a cached query. Not a snapshot refreshed on a schedule. **It is an insert
trigger.** As each block of rows lands in the source table, the view's `SELECT`
runs over *that block alone*, and the result is written to a separate target
table. Cost is paid once, at write time, in the streaming path — never on a
dashboard load.

The target is an `AggregatingMergeTree`, which knows how to combine partial
aggregates from different insert blocks as it merges parts.

Two column styles, chosen per measure:

- **`SimpleAggregateFunction(sum, UInt64)`** for counts. Summation is
  associative, so no intermediate state is needed: the column holds a plain
  number and merging just adds. Cheaper to store and read.
- **`AggregateFunction(quantilesTDigest(...), UInt32)`** for latency.
  Quantiles are **not** associative — you cannot average two p95s and get the
  p95. So the t-digest *sketch itself* is stored and merged at read time with
  `quantilesTDigestMerge`. t-digest is approximate but most accurate in the
  tails, which is exactly where p95/p99 live. (Measured drift against
  `quantileExact`: within the 5% tolerance the tests enforce.)

### Two discoveries that changed the design

Both are cases where the "obvious" materialized view was *slower* than the
scan it was meant to replace.

**Discovery 1: the default `index_granularity` is wrong for small tables.**

The aggregate table holds ~18,000 rows. `index_granularity` defaults to 8192 —
a value tuned for tables of hundreds of millions of rows, where a sparse index
must stay small. Here, one granule was a *third of the entire table*, and every
row in it carries a fat t-digest sketch. Answering a 7-row question was pulling
**5,817 rows and ~1 MB** off disk.

Setting `index_granularity = 128` made granule reads precise: **5,817 rows →
128 rows**, and the query went from **8.7 ms to 2.9 ms** (p50, same 20M
dataset, only the view layer rebuilt).

The general point: `index_granularity` is a trade between index size and read
precision. Small, wide tables want a much finer setting than the default.

**Discovery 2: a view accelerates the grouping it was keyed for, and nothing
coarser.**

The tenant view is keyed `(account_id, day)`. It is tempting to answer the
platform-wide query from it by simply dropping the `account_id` filter. That
works — and it was measurably *slower than scanning 1.5 million raw rows*:
**14.7 ms from the view versus 12.1 ms from the raw table** (20M-row run).
The reason is that it must read and merge one t-digest sketch per
`(account, day)` pair — 200 accounts × 7 days = **1,400 sketch merges** — and
sketch merging is not free.

The fix was not a better index but a **second view keyed on `day` alone**
(`platform_daily`), reducing the same answer to 7 sketch merges: **14.7 ms →
2.3 ms** on that same 20M dataset, and **2.0 ms reading 91 rows** in the
shipped 12M benchmark.

So the shipped design has three views, one per dashboard that actually loads:
`conversation_daily` (tenant overview), `agent_hourly` (latency SLO by model
and channel), `platform_daily` (fleet-wide).

### The caveats you must design around

- **A view only sees rows inserted after it is created.** Existing data needs
  an explicit backfill. The benchmark derives its backfill from the view's own
  `SELECT` so the two cannot drift — a very common source of "the dashboard
  disagrees with the raw table."
- **A view does not see mutations.** `ALTER TABLE ... DELETE` on the source
  leaves the aggregate untouched. Corrections require rebuilding the affected
  partitions.
- **On a sharded cluster, attach the view to the *local* table on every node**,
  never to the `Distributed` table, or rows are aggregated twice.
- **Duplicate inserts are double-counted** even though `ReplacingMergeTree`
  will eventually dedupe the raw table. ClickHouse's identical-block
  deduplication covers the exact-replay case that at-least-once delivery
  actually produces; the backstop is that aggregates are rebuildable per
  partition.

### The property that matters most

The view reads **91 rows whether the raw table holds 12 million events or 12
billion**. Every other optimization reduces the scan by a *factor*; this one
makes the read cost independent of the data volume. That is what actually
holds a latency SLO as a business grows.

---

## 9. The alternative — projections

A projection is a second physical copy of the data *inside the same table*,
with its own sort order or its own pre-aggregation. The planner picks it
automatically — you keep querying the base table.

**Where it wins:** for the conversation drill-down it beats the bloom filter
outright — **2.0 ms reading 24,576 rows, versus 3.7 ms and 186,479 rows**. A
re-sorted copy locates the conversation exactly, where a bloom filter only
narrows probabilistically.

**Where it failed:** the aggregate projection was **never selected**, and
`EXPLAIN` says why — the query filters on `event_ts`, which the projection does
not materialize, so the planner cannot push the predicate into it. A projection
only helps queries matching its *shape*. That brittleness is a real argument for
materialized views whenever query shapes vary.

**Why materialized views shipped anyway**, despite the projection winning on
Q2: raw events expire at 90 days while aggregates are kept for two years, and
**a projection cannot outlive its parent part**. It therefore cannot have a
longer retention than the raw data. A view's target is a real table with its
own partitioning, TTL, and rebuild story.

Storage cost is also real: the projection table is 770 MB against 405 MB for
the plain one — a `_normal` projection duplicates every column it lists.

**Rule of thumb:** aggregate projections for "same table, different rollup,
same lifecycle"; materialized views when the rollup needs a *different
lifecycle* from the raw data.

---

## 10. How to choose: a decision procedure

Given a slow query, in order:

1. **Read the plan first.** `EXPLAIN indexes = 1` reports granules selected
   versus total. If it is already reading few granules, the problem is not
   pruning — it is the work per row, so look at types, codecs, and how many
   columns the query touches.
2. **Is the filter a prefix of the sorting key?** If yes and it is still slow,
   the sorting key is fine. If no, ask whether the dominant query's filter
   *should* be the sorting key. You get one sort order; spend it on the query
   you cannot afford to be slow.
3. **Is the filter on the partition key?** Partitioning helps queries that the
   sorting key cannot — typically cross-tenant or purely time-scoped ones. Do
   not add partitions for pruning alone without measuring per-part cost.
4. **Is it a high-cardinality needle?** That is a skipping index — but only if
   the column correlates with physical row order. Verify with `EXPLAIN` that
   granules actually drop; if they do not, remove the index.
5. **Is the query shape fixed and frequent?** Pre-aggregate it in a
   materialized view keyed *exactly* to its `GROUP BY`. Do not expect one view
   to serve a coarser grouping.
6. **Only then consider projections**, and only when the rollup can share the
   raw data's lifecycle.

Each step reduces a different cost. Applying all of them everywhere is how you
get a schema that is slow to write and no faster to read.

---

## 11. Measuring honestly

The benchmark methodology is part of the result, because a benchmark is
extraordinarily easy to rig by accident. Four rules, each of which caught a
real error:

**One variable per stage.** Stage *N* differs from *N−1* by exactly one
decision, and the data is generated once and copied, so no stage can win by
holding different rows.

**Verify results before reporting speed.** Every stage's output is compared
against the naive baseline. Counts must match exactly; percentiles within 5%
(t-digest sketches merged from many partial states are not bit-identical to a
single-pass computation). This caught a genuine bug: `ORDER BY slow_responses
DESC` with tied counts returns rows in scan-dependent order, so stages
disagreed on row *order* while agreeing on every value. The fix — a
deterministic tiebreaker — is also a real API bug fix, since unstable ordering
under ties breaks pagination.

**Interleave measurements.** Measuring one table to completion before the next
lets cache warmth accumulate against whichever runs last. An earlier revision
did exactly that and reported a phantom **2.6× regression for partitioning**
that vanished entirely once rounds were interleaved.

**Disable the query condition cache.** ClickHouse 25+ enables
`use_query_condition_cache` by default: it remembers which granules matched a
predicate, so re-running a query makes an *unindexed* table behave like an
indexed one. It was silently erasing most of the bloom filter's measured value
— showing a 3× difference in rows read instead of the true 62×. It was found
because a test asserting index pruning passed alone and failed in the suite;
an order-dependent test failure is worth chasing, not retrying.

Report **rows and bytes read** alongside milliseconds. Wall time tells you
something changed; the engine's own accounting tells you why, and transfers
across machines in a way that milliseconds do not.

---

## 12. What generalizes and what does not

**Generalizes:**

- The unit of work is the granule. Optimizing means reading fewer granules, or
  making each cheaper.
- The sorting key is the single highest-leverage decision, and it should match
  the filter prefix of the query you cannot afford to be slow.
- Pre-aggregation is the only technique that makes read cost *independent* of
  data volume. Everything else buys a factor.
- An index that does not measurably prune is a write-side cost with no
  benefit. Measure, then keep or delete.
- Partition granularity is a two-sided trade; measure both sides.
- Every optimization here helped exactly one or two of the four queries.
  Choose per access pattern.

**Does not generalize:**

- **The specific partition granularity.** Monthly is right at this volume and
  retention. At billions of events per day, daily is right. The *method* —
  measure pruning benefit against per-part cost — is what transfers.
- **The absolute latencies.** Measured with ClickHouse embedded in-process on
  an 8-core laptop. Server hardware, real page cache, and a distributed
  cluster all shift the numbers. Ratios and `rows_read` transfer; milliseconds
  do not.
- **`index_granularity = 128`.** Correct for a small, wide aggregate table.
  Applying it to a large fact table would bloat the primary index badly.
- **The minmax failure.** It failed because *this* column is uncorrelated with
  row order. A minmax index on a column that does correlate — a monotonically
  increasing ID, or a timestamp in a time-sorted table — is genuinely
  effective.

---

*Numbers cited here come from `benchmarks/results/` and are reproducible with
`./deploy.sh bench`, `./deploy.sh explain`, and
`python benchmarks/bench_partitioning.py`. See [README.md](README.md) for the
full result tables and the pipeline itself.*
