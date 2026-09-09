import os
import time

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api(client, corpus, monkeypatch):
    import uuid
    from pinecone_deployment import api as api_module

    name = f"t-api-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv(
        "PINECONE_LOCAL_HOST",
        os.environ.get("PINECONE_LOCAL_HOST", "http://localhost:5080"),
    )
    monkeypatch.delenv("PINECONE_API_KEY", raising=False)
    monkeypatch.setenv("PINECONE_INDEX", name)
    monkeypatch.setenv("EMBEDDER", "hash")
    api_module.get_store.cache_clear()
    store = api_module.get_store()
    store.upsert(corpus)
    for _ in range(100):
        if store.stats()["total_vectors"] >= len(corpus):
            break
        time.sleep(0.05)
    yield TestClient(api_module.app), store, corpus
    store.delete_index()
    api_module.get_store.cache_clear()


def test_health_and_stats(api):
    tc, store, corpus = api
    assert tc.get("/health").json()["embedder"].startswith("hash")
    assert tc.get("/stats").json()["total_vectors"] == len(corpus)


def test_similar_conversations_scoped_to_account(api):
    tc, store, corpus = api
    q = corpus[2]
    r = tc.post(
        "/similar-conversations",
        json={"account_id": q.account_id, "text": q.transcript, "top_k": 5},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["strategy"] == "dense" and len(body["hits"]) == 5
    assert {h["account_id"] for h in body["hits"]} == {q.account_id}
    assert body["hits"][0]["id"] == q.vector_id
    r = tc.post(
        "/similar-conversations",
        json={
            "account_id": q.account_id,
            "text": q.transcript,
            "strategy": "hybrid",
        },
    )
    assert r.status_code == 400  # store is dense-only


def test_suggest_resolution_uses_resolved_neighbours(api):
    tc, store, corpus = api
    q = next(c for c in corpus if not c.escalated)
    r = tc.post(
        "/suggest-resolution",
        json={"account_id": q.account_id, "text": q.transcript, "top_k": 5},
    )
    body = r.json()
    assert body["suggestion"] and 0 < body["confidence"] <= 1
    assert all(n["intent"] for n in body["neighbours"])
