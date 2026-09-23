# Governed agent access to gold — RBAC, blue-green promotion, a pgvector catalog and an MCP server

The layer that makes the gold datamart "AI-ready" in the agentic sense: an
agent can **find** what exists and **run** a pre-approved, parameterized query
against it. It cannot run SQL. Not "we told it not to" — the MCP server has no
tool that accepts SQL, and the PostgreSQL role it connects as can `SELECT`
nothing but `gold` and the catalog.

Built on the gold layer this repository already has: the four dbt analysis
models in [`../dbt_deployment`](../dbt_deployment/) (`funnel_analysis`,
`page_type_summary`, `user_engagement`, `hourly_traffic`) on PostgreSQL, now
running the `pgvector/pgvector:pg15` image.

```bash
../dbt_deployment/deploy.sh up && ../dbt_deployment/deploy.sh load-data --all && ../dbt_deployment/deploy.sh run
./deploy.sh demo     # apply roles + gold + catalog, promote, sync, search, run, prove denials, roll back
./deploy.sh test     # 26 unit tests always; 8 PostgreSQL tests when the database is reachable
./deploy.sh serve    # the MCP server on stdio, as mcp_reader
```

---

## Four pieces

### 1. RBAC — one least-privilege role per stage (`sql/001_roles.sql`)

| Role | Can | Cannot |
| --- | --- | --- |
| `ingestion` | read/write `raw` | see anything else |
| `transform` | read `raw`, own the dbt schemas, write the catalog | read or write `gold` |
| `promotion` | call `gold_ops.promote()` and `gold_ops.rollback()`, read the release log | read **any** data table, including gold |
| `mcp_reader` | `SELECT` on `gold.*` and `catalog.entries`, read the release log | `raw`, staging, the dbt candidate schema, DDL anywhere, promotion |

Passwords are development defaults; `./deploy.sh apply` rotates them from
`<ROLE>_PASSWORD` if set. Go-live is secrets management, network policy and
TLS on top of this, not instead of it.

### 2. Blue-green promotion into `gold` (`sql/002_gold_promotion.sql`)

Readers always query `gold.<table>`. `gold_ops.promote(source_schema, note)`
is one `SECURITY DEFINER` function, owned by the admin, that:

1. validates the source (must exist, must not be a system, gold or release schema),
2. writes the release row, creates `gold_release_<id>`, copies every base
   table from the source except dbt's `stg_`/`int_` intermediates, records
   row counts,
3. grants `mcp_reader` on the new schema,
4. **swaps in one transaction**: the previous `gold` is renamed to its
   release schema, the candidate is renamed to `gold`,
5. keeps the three newest inactive releases and drops older schemas, saying
   so in the log.

No reader ever sees a half-promoted gold; nothing is copied at swap time.
`gold_ops.rollback()` is the rename in reverse: the active release is marked
`rolled_back`, the most recent retained `superseded` release becomes `gold`
again. The `promotion` role can call both and read the log, and can do
nothing else — it cannot even `SELECT` from the table it just promoted
(`test_stage_roles_are_least_privilege`).

### 3. The semantic catalog (`sql/003_catalog.sql`, `catalog.py`)

One row per gold **table**, **column** and **query template**, embedded with
pgvector and searched by cosine distance over an HNSW index. Entries are
*generated*: tables and columns from the dbt `manifest.json` (descriptions)
and `catalog.json` (physical types), templates from `templates/*.yaml`. Each
entry carries a content hash; a sync embeds and writes only what changed and
deletes what is gone, so the catalog follows the warehouse without anyone
remembering to update it. The embedder is recorded per row and a change of
embedder re-embeds everything.

The default embedder is a dependency-free hashing embedder — deterministic,
offline, and *lexical* (a paraphrase that shares no words is far away).
`CATALOG_EMBEDDER=sentence-transformers` (`uv sync --extra semantic`) swaps in
`all-MiniLM-L6-v2` at the same 384 dimensions.

### 4. Query templates and the MCP server (`templates/`, `templates.py`, `server.py`)

A template is a YAML file: a searchable description, SQL with `%(param)s`
placeholders, typed parameters with defaults, choices and ranges, the columns
it returns, a row cap and a timeout. Loading **rejects** anything that could
not be run safely:

- more than one statement, or anything that is not `SELECT`/`WITH`;
- a placeholder that is not declared, or a parameter that is not used;
- a `FROM`/`JOIN` target that is not `gold.<table>` or a CTE the template defines;
- a bare `%` (write `%%`), an unknown parameter type, an optional parameter
  whose description does not say what NULL means.

Execution binds parameters server-side through psycopg, wraps the SQL in
`LIMIT max_rows + 1` (so truncation is reported, not guessed), and runs under
`SET LOCAL statement_timeout`. The lint is a courtesy to the author; the role
is the boundary — `test_governed_execution_binds_caps_and_times_out` builds a
template by hand that reads outside gold and shows the database stopping it.

The MCP server (`FastMCP`, stdio) exposes five read-only tools:

| Tool | What it does |
| --- | --- |
| `gold_search_catalog` | semantic search over tables, columns and templates |
| `gold_list_templates` | every runnable template with its parameters |
| `gold_describe_template` | typed parameters, returns, cap, timeout, and the SQL (read-only, for transparency) |
| `gold_describe_table` | columns from the catalog, which templates read it, the active release |
| `gold_run_template` | validated, bound, capped, timed execution as `mcp_reader` |

Five shipped templates: `funnel_by_page_type`, `page_type_summary`,
`top_engaged_users`, `hourly_traffic`, `daily_conversion_trend`. Adding one is
one YAML file and a catalog sync.

---

## The demo, as it ran (2026-09-22)

`./deploy.sh demo` against the dbt deployment's PostgreSQL, holding the dbt
output of an earlier load (3,001 raw impressions). The test suite had already
run against the same database, which is why the release numbers start in the
fifties and the catalog reports nothing changed: the sync is incremental and
the tests had left it complete.

```console
=== 1. apply roles, gold, promotion functions, catalog
applied 001_roles.sql, 002_gold_promotion.sql, 003_catalog.sql as dbt

=== 2. promote public_analytics -> gold as the promotion role
  release #58: funnel_analysis, hourly_traffic, page_type_summary, user_engagement
  row counts: {"hourly_traffic": 3, "funnel_analysis": 6, "user_engagement": 50, "page_type_summary": 3}

=== 3. sync the catalog from the dbt manifest and templates
catalog: 45 entries (0 new, 0 changed, 0 removed, 45 unchanged) with hash-v1-384

=== 4. what the agent sees: search, describe, run
  ? conversion rate by page type
      0.492  template funnel_by_page_type
      0.445  template daily_conversion_trend
      0.427  column   page_type
  ? who are the most engaged users
      0.380  template top_engaged_users
      0.244  column   most_engaged_page_type
      0.212  column   last_seen
  ? traffic by hour last week
      0.183  column   hour
      0.141  column   total_impressions
      0.141  column   unique_users
  run funnel_by_page_type(page_type=3): 2 rows in 1.14 ms, columns ['page_type', 'event_type', 'impressions_at_stage', 'total_impressions', 'pct_of_total', 'pct_from_previous_stage']
      [3, 'c', 200, 400, 50.0, None]
      [3, 'f', 200, 400, 50.0, 100.0]
  rejected {'page_type': 9}: template 'funnel_by_page_type': parameter 'page_type' must be one of [1, 2, 3], got 9
  rejected {'page_type': 'three'}: template 'funnel_by_page_type': parameter 'page_type' expects int, got 'three'
  rejected {'page_typo': 3}: template 'funnel_by_page_type': unknown parameter(s) ['page_typo']; accepts ['page_type']

=== 5. what the roles cannot do
  mcp_reader: denied   SELECT count(*) FROM raw.impressions  (permission denied for schema raw)
  mcp_reader: denied   SELECT count(*) FROM public_analytics.funnel_analysis  (permission denied for schema public_analytics)
  mcp_reader: denied   SELECT gold_ops.promote('public_analytics', 'x')  (permission denied for function promote)
  promotion: denied   SELECT count(*) FROM gold.funnel_analysis  (permission denied for schema gold)
  ingestion: denied   SELECT count(*) FROM gold.funnel_analysis  (permission denied for schema gold)

=== 6. promote again, then roll back
  release #59 active
  rolled back: release #58 active again
    #59 rolled_back retained=True second release
    #58 active      retained=True demo release
    #57 superseded  retained=True restore after tests
    #56 superseded  retained=True all templates
    #55 superseded  retained=False service
    ... (older releases, schemas dropped by retention)
  reader still sees gold.funnel_analysis: 6 rows
```

Reading it:

- **The promotion role promoted a schema it cannot read.** Step 2 runs as
  `promotion`; step 5 shows the same role denied `SELECT` on the table it just
  published. The function does the reading as its definer.
- **The reader can run a template and nothing else.** Three malformed calls
  are rejected by parameter validation before any SQL exists; three direct
  queries outside gold are rejected by PostgreSQL. Both messages name the
  problem.
- **Rollback is a rename.** Release 59 went active and was rolled back in the
  same second; the reader's next query saw release 58 again. Retention has
  dropped the schemas of releases older than the three newest inactive ones
  and the log says so (`retained=False`).
- **The hashing embedder is lexical, and it shows.** "conversion rate by page
  type" and "most engaged users" rank the right template first; "traffic by
  hour last week" ranks the `hour` column above the `hourly_traffic` template
  because the template's text says "by hour for a date range", not "last
  week". `CATALOG_EMBEDDER=sentence-transformers` is the fix; the shape of the
  catalog does not change.

The server was also driven over stdio by a raw JSON-RPC client:
`tools/list` returned the five tools, `gold_run_template` on
`page_type_summary` with no parameters returned three rows, and the same tool
with `page_type: 9` returned a JSON error naming the allowed values. That
probe found the one bug the tests had not: an optional parameter arriving as
NULL left PostgreSQL unable to type it. The two templates with optional
parameters now cast their placeholders, and
`test_every_shipped_template_runs_with_only_its_required_parameters` holds
every template to that.

---

## Layout

```
sql/001_roles.sql            stage roles and grants (idempotent)
sql/002_gold_promotion.sql   gold, gold_ops.releases, promote(), rollback()
sql/003_catalog.sql          pgvector extension, catalog.entries, HNSW index
templates/*.yaml             the five approved queries
mcp_deployment/
  config.py      connections (admin + stage roles), paths, source schema
  db.py          psycopg helpers: apply sql/, rotate passwords
  embeddings.py  HashEmbedder (default), SentenceTransformerEmbedder (optional)
  catalog.py     entries from dbt artifacts + templates; rank(); sync(); search()
  templates.py   Template/Param, the lint, bind(), execute()
  promotion.py   promote / rollback / releases as the promotion role (+ CLI)
  service.py     GoldService: what the tools do, with injectable search/execute
  server.py      the FastMCP server; five tools, no SQL tool
  demo.py        apply -> promote -> sync -> search -> run -> deny -> roll back
tests/
  test_templates.py, test_catalog.py, test_service.py   no database needed
  test_postgres.py                                       marked `postgres`; skips if unreachable
```

## Connecting an MCP client

Any MCP client that speaks stdio. For Claude Code, from this directory:

```bash
claude mcp add gold -- uv run python -m mcp_deployment.server
```

Then ask a question. The agent's first call is `gold_search_catalog`, its last
is `gold_run_template`; there is nothing else it can do here.

## Scale, honestly

This is toy-sized: four gold tables, ~50 catalog entries, five templates. The
shapes that matter at scale are already in place — the catalog is a table with
an HNSW index (pgvector handles millions of rows), sync is incremental by hash,
templates are files under review, and the server is stateless per call — but
nothing here has been measured under load.
