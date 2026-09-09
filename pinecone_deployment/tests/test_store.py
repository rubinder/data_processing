"""Store behaviour against Pinecone Local."""

import time

import pytest

from pinecone_deployment.conversations import generate
from pinecone_deployment.embeddings import HashEmbedder
from pinecone_deployment.rerank import LexicalReranker
from pinecone_deployment.store import ConversationStore, is_local


def _settle(store, expected):
    for _ in range(100):
        if store.stats()["total_vectors"] >= expected:
            return
        time.sleep(0.05)


def test_upsert_is_idempotent_by_deterministic_id(store, corpus):
    assert store.upsert(corpus[:50]) == 50
    assert store.upsert(corpus[:50]) == 50
    _settle(store, 50)
    assert store.stats()["total_vectors"] == 50


def test_namespace_isolation_never_returns_another_tenant(store, corpus):
    store.upsert(corpus)
    _settle(store, len(corpus))
    account = corpus[0].account_id
    result = store.search(corpus[0].transcript, account, top_k=20)
    assert result.hits
    assert {h.metadata["account_id"] for h in result.hits} == {account}
    assert result.hits[0].id == corpus[0].vector_id  # itself, score ~1
    assert result.hits[0].score > 0.99
    # Namespaces: one per account.
    assert set(store.stats()["namespaces"]) == {c.account_id for c in corpus}


def test_filter_isolation_matches_namespace_results(filter_store, corpus):
    filter_store.upsert(corpus)
    _settle(filter_store, len(corpus))
    account = corpus[3].account_id
    result = filter_store.search(corpus[3].transcript, account, top_k=20)
    assert {h.metadata["account_id"] for h in result.hits} == {account}
    assert list(filter_store.stats()["namespaces"]) == ["conversations"]
    # Extra filters compose with the tenant filter.
    only_escalated = filter_store.search(
        corpus[3].transcript,
        account,
        top_k=50,
        filter={"escalated": {"$eq": True}},
    )
    assert all(h.metadata["escalated"] is True for h in only_escalated.hits)


def test_hybrid_query_executes_locally_but_sparse_is_not_scored(
    hybrid_store, corpus
):
    """Pinecone Local accepts ``sparse_values`` on upsert and ``sparse_vector``
    on query but does not score them (measured: a sparse-only query, alpha=0,
    returns score 0.0 for every hit; the probe's dense+sparse score equalled
    the dense score alone). Hybrid *ranking* is therefore a cloud-only
    measurement; locally we can only assert the request path works."""
    hybrid_store.upsert(corpus)
    _settle(hybrid_store, len(corpus))
    target = next(c for c in corpus if "#" in c.transcript)
    ref = next(
        tok for tok in target.transcript.split() if tok.startswith("#")
    ).rstrip(",.?")
    result = hybrid_store.search(
        ref, target.account_id, top_k=3, strategy="hybrid", alpha=0.0
    )
    assert result.strategy == "hybrid" and len(result.hits) == 3
    if is_local(hybrid_store.client):
        assert all(
            h.score == 0.0 for h in result.hits
        )  # the emulator's limitation
    else:
        assert (
            result.hits[0].id == target.vector_id
        )  # the service scores sparse
    with pytest.raises(ValueError):
        hybrid_store.search(
            ref, target.account_id, strategy="hybrid", alpha=1.5
        )


def test_hashed_bm25_matches_exact_tokens_without_pinecone(corpus):
    """The sparse side itself, independent of the index."""
    from pinecone_deployment.sparse import HashedBm25, hybrid_scale

    enc = HashedBm25().fit([c.transcript for c in corpus])
    target = next(c for c in corpus if "#" in c.transcript)
    ref = next(
        tok for tok in target.transcript.split() if tok.startswith("#")
    ).rstrip(",.?")
    doc, query = enc.encode_document(target.transcript), enc.encode_query(ref)
    shared = set(doc["indices"]) & set(query["indices"])
    assert shared, "the order reference must hash to a shared term id"
    # A rare term carries a high IDF; a word present in most docs a low one.
    common = enc.encode_query("thanks")["values"][0]
    assert query["values"][0] > common
    dense, sparse = hybrid_scale([1.0, 1.0], query, alpha=0.25)
    assert dense == [0.25, 0.25] and sparse["values"][0] == pytest.approx(
        0.75 * query["values"][0]
    )


def test_hybrid_requires_dotproduct(client):
    with pytest.raises(ValueError, match="dotproduct"):
        ConversationStore(
            client,
            HashEmbedder(64),
            "x",
            metric="cosine",
            sparse_encoder=__import__(
                "pinecone_deployment.sparse", fromlist=["HashedBm25"]
            ).HashedBm25(),
        )


def test_rerank_keeps_top_k_and_uses_resolution_text(store, corpus):
    store.upsert(corpus)
    _settle(store, len(corpus))
    q = corpus[5]
    result = store.search(
        q.transcript, q.account_id, top_k=3, reranker=LexicalReranker()
    )
    assert result.reranked and len(result.hits) == 3


def test_delete_account_removes_only_that_tenant(store, corpus):
    store.upsert(corpus)
    _settle(store, len(corpus))
    victim = corpus[0].account_id
    before = store.stats()["namespaces"]
    store.delete_account(victim)
    time.sleep(0.3)
    after = store.stats()["namespaces"]
    assert after.get(victim, 0) == 0
    for account, count in before.items():
        if account != victim:
            assert after[account] == count


def test_delete_account_with_filter_isolation(filter_store, corpus):
    filter_store.upsert(corpus)
    _settle(filter_store, len(corpus))
    victim = corpus[0].account_id
    expected_left = sum(1 for c in corpus if c.account_id != victim)
    filter_store.delete_account(victim)
    time.sleep(0.3)
    assert filter_store.stats()["total_vectors"] == expected_left


def test_wait_visible_measures_freshness(store, corpus):
    conv = corpus[0]
    store.upsert([conv])
    assert store.wait_visible(conv) < 5.0


def test_ensure_index_refuses_a_different_model(client, store):
    other = ConversationStore(client, HashEmbedder(128), store.index_name)
    other.embedder.model = "some-other-model"
    with pytest.raises(RuntimeError, match="do not mix models"):
        other.ensure_index()


def test_ensure_index_refuses_a_dimension_mismatch(client, store):
    other = ConversationStore(client, HashEmbedder(64), store.index_name)
    other.embedder.model = store.embedder.model
    with pytest.raises(RuntimeError, match="dimension"):
        other.ensure_index()


def test_generate_is_deterministic():
    a = generate(20, seed=3)
    b = generate(20, seed=3)
    assert [c.vector_id for c in a] == [c.vector_id for c in b]
    assert a[0].transcript == b[0].transcript
