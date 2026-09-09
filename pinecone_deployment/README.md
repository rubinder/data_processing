# Pinecone Deployment

Similar-conversation retrieval for the AI agent platform whose events
`realtime_analytics/` streams: given a new conversation, find the nearest
past conversations *for the same tenant*, and suggest the resolution that
worked for them. The interesting decisions are Pinecone's, not the API's:
how tenants are isolated, whether the sparse side earns its keep, what a
model upgrade does to an index, and what any of it costs. Each is a measured
section below, negative results included.

Everything runs against **Pinecone Local** (the emulator Pinecone ships as a
Docker image) with a dependency-free embedder, so the tests and the local
benchmark need no account. Set `PINECONE_API_KEY` and the same code targets a
serverless index; `EMBEDDER=pinecone` switches to Pinecone-hosted embedding
models.

```
pinecone_deployment/
├── pinecone_deployment/
│   ├── conversations.py   # deterministic synthetic corpus with a labelled intent per conversation
│   ├── embeddings.py      # hash (stdlib) | sentence-transformers | pinecone inference
│   ├── sparse.py          # hashed BM25 + Pinecone's convex hybrid weighting
│   ├── store.py           # index lifecycle, batched idempotent upserts, tenant isolation, search
│   ├── rerank.py          # Pinecone Inference reranker + a lexical stand-in
│   ├── evaluate.py        # recall@k vs exact kNN, intent precision@k, latency, freshness
│   ├── migrate.py         # embedding-model migration: shadow index, dual write, cutover
│   └── api.py             # FastAPI: /similar-conversations, /suggest-resolution
├── benchmarks/run_local.py
├── tests/                 # 20 tests against Pinecone Local
├── docker-compose.yaml    # pinecone-local + the API
└── deploy.sh              # up | down | test | bench | load | api
```

## Quick start

```bash
./deploy.sh up local          # Pinecone Local on :5080 (one port per index, 5081+), API on :8090
./deploy.sh test              # 20 tests
./deploy.sh load              # 2,000 synthetic conversations, 20 tenants, into index "conversations"
curl -s localhost:8090/similar-conversations -H 'content-type: application/json' \
  -d '{"account_id":"acct_0003","text":"I was charged twice this month","top_k":3}' | python3 -m json.tool
./deploy.sh bench             # the tables below
./deploy.sh down local
```

## Design decisions

**Deterministic ids.** A vector's id is `account_id:conversation_id`, so
re-embedding a conversation is an in-place upsert. Same idempotency rule as
the Spark partitions; without it every retry duplicates a row that then
outvotes its neighbours.

**Tenant isolation is a parameter.** `isolation="namespace"` gives every
account its own namespace: a query physically cannot see another tenant, and
offboarding is one `delete(delete_all=True, namespace=...)`.
`isolation="filter"` keeps one namespace and applies `account_id` as a
metadata filter: one hot namespace, cross-tenant analytics possible, and
offboarding is list-by-prefix then delete-by-id because serverless has no
delete-by-filter. The store implements both so they can be measured rather
than argued.

**Metadata is a schema.** Pinecone indexes every metadata field by default
and bills for it; `Conversation.metadata()` is a small, typed set (tenant,
intent, channel, locale, sentiment, escalation, resolution text) and nothing
else. The transcript is not stored in metadata; the resolution is, because
the reranker and the API need it.

**The index knows its model.** `ensure_index` tags the index with the
embedding model (cloud) *and* writes a metadata-only record in a reserved
`__meta__` namespace (works everywhere, including the emulator, which does
not return tags). An embedder that disagrees with the recorded model is
refused. Querying model-A vectors with a model-B query returns confident
nonsense, and nothing else in the stack would notice.

**Hybrid needs `dotproduct`.** Sparse-dense queries are only valid on a
dotproduct index; the store refuses to attach a sparse encoder to a cosine
index. Weighting follows Pinecone's documented convex scaling (`alpha` on the
dense side, `1 - alpha` on the sparse values).

**Model migration is a procedure, not an update.** `migrate.py`: shadow
index named after the new model, backfill from the source of truth (the
conversations, never the old vectors), dual-write new arrivals, compare on
labelled queries, flip the `ActiveIndex` pointer the API reads, retire the
old index. Pinecone has no index aliases, so the pointer is ours. The test
runs the whole sequence.

## Measured (Pinecone Local, 2026-09-08)

2,000 conversations across 20 tenants, 200 held-out queries, k=10, hashing
embedder (256 dims). Pinecone Local is exact and in-memory: recall@k is 1.0
by construction and latencies are a floor. What transfers is the *relative*
behaviour and the precision numbers.

### Ingestion

| index | vectors | seconds | vectors/s | batch |
| --- | --- | --- | --- | --- |
| namespace / dense | 2,000 | 0.79 | 2,527 | 100 |
| filter / dense | 2,000 | 0.79 | 2,530 | 100 |
| namespace / hybrid | 2,000 | 1.00 | 2,000 | 100 |

Batch size (namespace / dense, 1,000 vectors): 10 → 1,790/s, 50 → 2,370/s,
100 → 2,438/s, 200 → 2,447/s. Past 100 the client is the bottleneck, not the
request count. On serverless each batch is a write-unit charge, so 100 to 200
is the sweet spot for both throughput and cost; the 2 MB request cap is the
ceiling.

### Retrieval

| strategy | recall@k vs exact | intent precision@k | p50 ms | p95 ms |
| --- | --- | --- | --- | --- |
| namespace / dense | 1.000 | 0.299 | 2.4 | 3.6 |
| filter / dense | 1.000 | 0.299 | 5.2 | 6.8 |
| namespace / dense + lexical rerank | 0.861 | 0.303 | 3.5 | 4.1 |
| namespace / hybrid, alpha 0.3 to 1.0 | 1.000 | 0.299 | 2.0 to 2.4 | 2.5 to 3.4 |
| namespace / hybrid, alpha 0.0 (sparse only) | 0.103 | 0.088 | 2.3 | 2.8 |

Reading it:

- **Namespace isolation is 2x faster than filtering here** (2.4 vs 5.2 ms
  p50) with identical results. The filter path searches one 2,000-vector
  namespace and discards 95% of candidates; the namespace path searches the
  tenant's ~100. On serverless the same shape holds and the filter path also
  reads more units. Namespaces win whenever tenants are the dominant filter;
  filtering wins when you need cross-tenant queries or have more tenants than
  the namespace count you want to manage.
- **Intent precision 0.30 at k=10 is the hashing embedder's ceiling**, 3.6x
  the 1/12 random baseline and no more: two phrasings of the same intent that
  share no words are far apart in a bag-of-words space. This is the number a
  semantic model moves; see the next section.
- **The lexical reranker did not help** (0.303 vs 0.299) and its reordering
  drops recall-vs-exact to 0.86 while adding 1 ms. A reranker is only worth
  its latency when it is a stronger model than the retriever; a token-overlap
  reranker over a token-hash retriever is not. Pinecone's hosted
  `bge-reranker-v2-m3` is the real test and is cloud-only.
- **Pinecone Local does not score sparse vectors.** It accepts
  `sparse_values` on upsert and `sparse_vector` on query, but every alpha from
  0.3 to 1.0 returned exactly the dense ranking, and alpha 0 (sparse only)
  returned score 0.0 for every hit. Hybrid ranking is therefore a serverless
  measurement; the local test asserts only that the request path works. The
  sparse encoder itself (hashed BM25) is tested directly: the order reference
  in a transcript hashes to a shared term id with a high IDF.

### Freshness and offboarding

Upsert-to-fetch-visible: p50 1.5 ms, max 2.2 ms over 20 samples. On
serverless this is the eventual-consistency window and is measured the same
way (`store.wait_visible`), not assumed. Tenant delete: 1 ms by namespace,
5 ms by list-and-delete-ids at this size; the filter path scales with the
tenant's vector count, the namespace path does not.

### A real embedding model

Same benchmark with `EMBEDDER=sentence-transformers` (`all-MiniLM-L6-v2`,
384 dims, on the laptop's CPU), 1,000 conversations, 10 tenants, 100 queries:

| strategy | recall@k vs exact | intent precision@k | p50 ms | p95 ms |
| --- | --- | --- | --- | --- |
| namespace / dense | 1.000 | **0.553** | 4.2 | 5.8 |
| filter / dense | 1.000 | 0.553 | 10.2 | 14.1 |
| namespace / dense + lexical rerank | 0.869 | 0.551 | 5.3 | 10.6 |
| namespace / hybrid, alpha 0.3 to 1.0 | 1.000 | 0.553 | 3.8 to 4.1 | 5.7 to 5.9 |

- **Intent precision 0.30 → 0.55**, 6.6x the random baseline, from changing
  nothing but the embedder. That is the whole argument for a semantic model
  over lexical features, and also its limit here: the remaining errors are
  intents whose template banks share vocabulary (`order_status`,
  `refund_request` and `shipping_delay` all talk about orders arriving), so
  the number is a property of the corpus as much as the model. A richer,
  LLM-written corpus is the follow-up in Tasks.md.
- **Ingestion is now embedder-bound**: 247 vectors/s for the first index
  (model warm-up included), ~700/s after, versus 2,500/s with hashing.
  Batch size stops mattering past 50 because the time is in `encode`, not in
  the upsert request. On the hosted embedder this becomes a request-count and
  billing question instead of a CPU one.
- **The query-side latency doubled** (2.4 → 4.2 ms p50) because each query
  now runs the model once before the index call. The filter-vs-namespace gap
  (2.4x) and the rerank result are unchanged: they are index effects, not
  embedder effects.
- The model cache defaults to `~/.cache/huggingface`; set `HF_HOME` if that
  path is not writable.

## Cloud-only parts (need `PINECONE_API_KEY`)

Written and unit-tested with fakes, not run against the service in this repo:

- `PineconeInferenceEmbedder`: hosted `multilingual-e5-large` /
  `llama-text-embed-v2`, with the passage/query `input_type` distinction
  e5-style models need, batched at the 96-input API limit.
- `PineconeReranker`: hosted `bge-reranker-v2-m3` / `cohere-rerank-3.5`.
- Hybrid ranking, approximate recall (serverless is not exact), read/write
  unit accounting, and the eventual-consistency window at scale.

Run `PINECONE_API_KEY=... PYTHONPATH=. uv run python benchmarks/run_local.py`
for the serverless numbers; the script is identical. Cost model for the
sizing conversation: serverless bills storage per GB-month, write units per
upsert batch (size-dependent), and read units per query (scales with the
namespace's size and the `top_k`), which is the quantitative reason namespace
isolation beats filtering for tenant-scoped search.

## Operations

- **Tenant offboarding**: `store.delete_account(id)`; namespace mode is one
  call, filter mode lists ids by prefix (`account_id:`) and deletes in
  batches of 1,000.
- **Backups**: serverless supports index backups and restore to a new index;
  the migration pointer makes the restored index the live one without an API
  change.
- **Freshness**: `describe_index_stats` plus `wait_visible` after critical
  upserts, since serverless acknowledges before the vector is queryable.
- **SDK pin**: `pinecone>=7,<8`. The 10.x client sends the 2025-10 control
  plane body and Pinecone Local rejects it (`missing field dimension`).
