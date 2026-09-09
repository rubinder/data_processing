"""Fixtures against Pinecone Local (docker compose up pinecone-local)."""

import os
import uuid

import pytest

from pinecone_deployment.conversations import generate
from pinecone_deployment.embeddings import HashEmbedder
from pinecone_deployment.sparse import HashedBm25
from pinecone_deployment.store import ConversationStore, connect

LOCAL_HOST = os.environ.get("PINECONE_LOCAL_HOST", "http://localhost:5080")


@pytest.fixture(scope="session")
def client():
    pc = connect(local_host=LOCAL_HOST)
    try:
        pc.list_indexes()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Pinecone Local not reachable at {LOCAL_HOST}: {exc}")
    return pc


@pytest.fixture(scope="session")
def corpus():
    return generate(count=300, accounts=5, seed=11)


def _store(client, **kwargs):
    name = f"t-{uuid.uuid4().hex[:10]}"
    store = ConversationStore(
        client, kwargs.pop("embedder", HashEmbedder(128)), name, **kwargs
    )
    store.ensure_index()
    return store


@pytest.fixture
def store(client):
    s = _store(client)
    yield s
    s.delete_index()


@pytest.fixture
def filter_store(client):
    s = _store(client, isolation="filter")
    yield s
    s.delete_index()


@pytest.fixture
def hybrid_store(client):
    s = _store(client, metric="dotproduct", sparse_encoder=HashedBm25())
    yield s
    s.delete_index()
