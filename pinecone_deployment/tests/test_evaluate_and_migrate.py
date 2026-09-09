import time

from pinecone_deployment.conversations import generate
from pinecone_deployment.embeddings import HashEmbedder
from pinecone_deployment.evaluate import (
    evaluate,
    exact_top_k,
    measure_freshness,
)
from pinecone_deployment.migrate import (
    ActiveIndex,
    DualWriter,
    cut_over,
    shadow_index_name,
)
from pinecone_deployment.store import ConversationStore


def _settle(store, expected):
    for _ in range(100):
        if store.stats()["total_vectors"] >= expected:
            return
        time.sleep(0.05)


def test_recall_against_exact_is_perfect_on_local(store, corpus):
    store.upsert(corpus)
    _settle(store, len(corpus))
    held_out = generate(count=30, accounts=5, seed=99)
    # Queries must belong to tenants that exist in the corpus.
    accounts = {c.account_id for c in corpus}
    queries = [q for q in held_out if q.account_id in accounts][:20]
    ev = evaluate(store, corpus, queries, k=5)
    assert ev.queries == len(queries)
    assert ev.recall_at_k == 1.0  # Pinecone Local is exact
    assert 0.0 <= ev.intent_precision_at_k <= 1.0
    assert ev.p95_ms >= ev.p50_ms >= 0


def test_exact_top_k_cosine():
    import numpy as np

    ids = ["a", "b", "c"]
    m = np.array([[1, 0], [0, 1], [1, 1]], dtype=np.float32)
    assert exact_top_k([1.0, 0.0], ids, m, 2, "cosine") == ["a", "c"]


def test_freshness_measurement(store, corpus):
    result = measure_freshness(store, corpus[:3])
    assert result["samples"] == 3 and result["max_ms"] < 5000


def test_migration_shadow_dual_write_cutover(client, store, corpus, tmp_path):
    pointer = ActiveIndex(tmp_path / "active_index.json")
    pointer.write(
        store.index_name, store.embedder.model, store.embedder.dimension
    )
    store.upsert(corpus[:100])

    new_model = HashEmbedder(64)  # "new" embedding model: different dims
    new_model.model = "hash-v2-64"
    shadow = ConversationStore(
        client, new_model, shadow_index_name(store.index_name, new_model.model)
    )
    shadow.ensure_index()
    try:
        # backfill from the source of truth, then dual-write the new arrivals
        shadow.upsert(corpus[:100])
        live_n, shadow_n = DualWriter(store, shadow).upsert(corpus[100:120])
        assert (live_n, shadow_n) == (20, 20)
        _settle(store, 120)
        _settle(shadow, 120)
        assert (
            store.stats()["total_vectors"]
            == shadow.stats()["total_vectors"]
            == 120
        )

        queries = [
            c
            for c in generate(20, accounts=5, seed=5)
            if c.account_id in {x.account_id for x in corpus}
        ][:10]
        old = evaluate(store, corpus[:120], queries, k=5, label="old")
        new = evaluate(shadow, corpus[:120], queries, k=5, label="new")
        assert old.queries == new.queries == len(queries)

        active = cut_over(pointer, shadow)
        assert active == {
            "index": shadow.index_name,
            "model": "hash-v2-64",
            "dimension": 64,
        }
        assert len(shadow.index_name) <= 45
    finally:
        shadow.delete_index()
