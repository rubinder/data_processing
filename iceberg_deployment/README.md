# Apache Iceberg — lakehouse tables on Spark 3.5

Iceberg table format over the repository's impression event model, demonstrating
the four things a table format buys you over a directory of Parquet files:
**safe schema evolution, partition evolution, time travel, and row-level
upserts** — plus the maintenance work that keeps a table healthy afterwards.

This closes an open item in [`../Tasks.md`](../Tasks.md): *"Adopt Delta/Iceberg
with column mapping and a deliberate schema-evolution policy for evolving
tables."*

Everything here is verified by **22 tests that run against real Iceberg tables**
— real Parquet, real snapshot metadata, no mocks and no Docker required.

```bash
./deploy.sh test     # 22 tests, local filesystem catalog
./deploy.sh demo     # the full walkthrough, printed as it happens
./deploy.sh up       # REST catalog + MinIO, the production-shaped deployment
```

---

## Why a table format at all

A Hive-style table is a directory of Parquet files plus a schema in a
metastore. Parquet records column *names* and *positions*; nothing binds a file
written last year to the schema registered today. Evolution therefore goes
wrong in ways that **do not raise an error**:

| Change | What actually happens on a Hive table |
| --- | --- |
| Rename a column | old files still say the old name → reads return NULL, silently |
| Drop, then re-add the same name | old files still hold the old column → its values reappear under the new meaning |
| Reorder columns | anything reading positionally maps the wrong values to the wrong columns |
| Change the partitioning | the partitioning *is* the directory layout, so it means rewriting the table |

Each produces plausible-looking wrong data, which is worse than a crash: nobody
notices until a number is questioned in a meeting.

**Iceberg gives every column a permanent field ID.** Data files record IDs, not
names; the schema maps IDs to current names. A rename changes the name for an
ID and nothing else. A drop retires the ID forever, so re-adding the same
*name* allocates a *new* ID and cannot resurrect old values. Reordering is
metadata. All of it is an atomic, metadata-only commit — no data rewritten,
instantly reversible via the snapshot it creates.

The tests assert exactly that, including on the field IDs themselves.

---

## What each module demonstrates

### `schema_evolution.py` — the reason this exists

The compatibility policy this module enforces. Safe, because they are
metadata-only and cannot invalidate an existing value:

```
ADD COLUMN (nullable, any position)    RENAME COLUMN
DROP COLUMN                            REORDER COLUMNS
WIDEN TYPE (int→bigint, float→double, decimal precision)
```

Deliberately rejected, because they cannot be applied to already-written files:

```
narrowing a type (bigint→int)          making a nullable column required
changing an unrelated type (string→int)
```

> **The rule:** a change that could invalidate an existing value is a new column
> plus a backfill, never an in-place alter.

Verified in `tests/test_schema_evolution.py` — including that a rename
**preserves the field ID**, that drop-then-re-add does **not** resurrect old
values, that narrowing is **rejected**, and that evolution rewrites **zero**
data files.

### Partition evolution — no Hive equivalent

Iceberg versions the partition spec and stamps each data file with the spec it
was written under, so `days(event_ts)` can become `hours(event_ts)` without
rewriting a byte. The planner produces a split plan and prunes each group by
its own spec. Observed in the demo:

```
3. Partition spec evolution — days() -> hours(), no rewrite
data files now span partition specs [0, 2]; total rows 387
```

The usual reason to do this is growth: daily partitions that were right at a
million events a day are too coarse at a hundred million.

### `time_travel.py` — snapshots, `AS OF`, rollback

Every write is an atomically committed snapshot. Readers pin one for the
duration of a query, so a long read never sees a half-finished write.

- **Time travel** makes an incident answerable after the fact ("what did the
  dashboard show before the backfill?") and makes a rollup reproducible: pin
  the snapshot ID and the aggregate is deterministic.
- **Rollback** undoes a bad write by moving the table pointer. Metadata only —
  the same cost on a 10 GB table as a 10 TB one.

Rollback *appends* a snapshot rather than erasing history, so the rollback
itself is auditable and reversible.

> The [ClickHouse module](../realtime_analytics) in this repo has neither. When
> an aggregate there drifts, the remedy is rebuilding partitions from raw data.
> That is a fine trade for a serving layer optimized for read latency — and it
> is precisely why a lakehouse table usually sits underneath one.

### `upserts.py` — MERGE INTO and row-level deletes

The repo's CDC path (Debezium → Kafka → Flink) is at-least-once: a crash
between processing and offset commit replays the tail of the stream. `MERGE
INTO` makes that harmless by converging rather than appending — verified by
applying the same batch twice and asserting the row count is unchanged.

Row-level `DELETE` is worth calling out separately: on plain partitioned
Parquet the smallest unit you can delete is a whole partition, so "erase this
user" becomes "rewrite every partition they appear in".

The table is `format-version = 2` with merge-on-read, so an update writes a
small delete file rather than rewriting whole data files. That makes per-batch
MERGE cheap; the cost moves to read time and is reclaimed by compaction.
Copy-on-write is the opposite trade — choose per table by write frequency.

### `maintenance.py` — the part that gets skipped

An unmaintained Iceberg table degrades three separate ways:

| Problem | Procedure | Why it matters |
| --- | --- | --- |
| Small files | `rewrite_data_files` | every file costs a manifest entry, an open, and a footer read at plan time |
| Accumulated delete files | `rewrite_data_files` | merge-on-read pushes cost to readers until compaction reclaims it |
| Unbounded snapshot history | `expire_snapshots` | retained snapshots retain their data files — storage grows forever |
| Files from failed writes | `remove_orphan_files` | nothing references them, so nothing else will clean them up |

Observed in the demo:

```
data files 52 -> 32, records preserved: 392 -> 392
snapshots retained: 3 (expiry removed 14 data files, 4 manifests)
```

> **The expiry window *is* the recovery window.** `expire_snapshots` deletes the
> data files that rolled-back snapshots referenced. Seven days of retention
> means seven days to notice a bad write.

---

## Catalogs: what the tests use vs what you would deploy

| | `hadoop` (filesystem) | `rest` (REST catalog + S3) |
| --- | --- | --- |
| Services needed | none | REST catalog + object store |
| Commit atomicity | relies on atomic rename | owned by the catalog |
| Safe on object storage | **no** | yes |
| Used by | the tests and `demo` | `docker-compose.yaml` |

The filesystem catalog is a development catalog: object stores do not guarantee
atomic rename, so concurrent commits can corrupt it. It is what makes the tests
runnable with no infrastructure; it is not what you would run in production.

Switch with one variable — the job bodies never know the difference:

```bash
./deploy.sh up
export ICEBERG_CATALOG_TYPE=rest
```

---

## Running it

```bash
./deploy.sh test     # 22 tests against real Iceberg tables (~2 min)
./deploy.sh demo     # the six-step walkthrough
./deploy.sh up       # REST catalog :8181 + MinIO console :9101
./deploy.sh down
```

Requires a JDK (17 or 11) and Python 3.10. The Iceberg runtime jar is resolved
by Spark on first run and cached in `~/.ivy2`.

One portability note worth knowing if you run PySpark from a virtualenv: Spark
launches Python workers with whatever `python3` is on `PATH`, which is usually
*not* the interpreter running the driver. The mismatch surfaces at the first
shuffle as `PYTHON_VERSION_MISMATCH`, long after the session started
successfully. `session.py` pins both to `sys.executable`.

---

## Layout

```
iceberg_deployment/
├── README.md
├── deploy.sh                     lifecycle, demo, tests
├── docker-compose.yaml           Iceberg REST catalog + MinIO
├── pyproject.toml
├── iceberg_deployment/
│   ├── session.py                catalog config: hadoop | rest
│   ├── impressions.py            the table, hidden partitioning, schema-aware seeding
│   ├── schema_evolution.py       the compatibility policy, and the mechanism
│   ├── time_travel.py            snapshots, AS OF, rollback, cherrypick
│   ├── upserts.py                MERGE INTO, row-level delete
│   ├── maintenance.py            compaction, expiry, orphan cleanup
│   └── demo.py                   the six-step walkthrough
└── tests/                        22 tests, real tables, no mocks
```

## Verified / not verified

**Verified on this machine:** all 22 tests and the full demo, against Spark
3.5.4, Iceberg 1.6.1, Java 17, using the filesystem catalog.

**Not verified:** the `docker-compose.yaml` REST-catalog-on-MinIO path. The
configuration is written and the code path is exercised by the same
`session.py` used everywhere else, but the containers have not been started
here. Treat that stack as a starting point rather than a tested deployment.
