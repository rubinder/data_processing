"""FastAPI service over the conversation index.

    GET  /health
    POST /similar-conversations   {"account_id", "text", "top_k",
                                   "strategy", "alpha", "rerank"}
    POST /suggest-resolution      {"account_id", "text", "top_k"}
    GET  /stats

Configuration by environment: ``PINECONE_API_KEY`` (cloud) or
``PINECONE_LOCAL_HOST`` (emulator), ``PINECONE_INDEX``, ``EMBEDDER``
(``hash`` | ``sentence-transformers`` | ``pinecone``), ``ISOLATION``
(``namespace`` | ``filter``), ``ACTIVE_INDEX_FILE`` (migration pointer,
overrides ``PINECONE_INDEX`` when present).
"""

import os
from collections import Counter
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pinecone_deployment.embeddings import make_embedder
from pinecone_deployment.migrate import ActiveIndex
from pinecone_deployment.rerank import LexicalReranker, PineconeReranker
from pinecone_deployment.sparse import HashedBm25
from pinecone_deployment.store import ConversationStore, connect, is_local

app = FastAPI(title="Conversation similarity search (Pinecone)")


@lru_cache(maxsize=1)
def get_store() -> ConversationStore:
    client = connect()
    embedder_name = os.environ.get("EMBEDDER", "hash")
    embedder = (
        make_embedder(embedder_name, client=client)
        if embedder_name == "pinecone"
        else make_embedder(embedder_name)
    )
    index_name = os.environ.get("PINECONE_INDEX", "conversations")
    pointer_file = os.environ.get("ACTIVE_INDEX_FILE")
    if pointer_file and (active := ActiveIndex(Path(pointer_file)).read()):
        index_name = active["index"]
    hybrid = os.environ.get("HYBRID", "false").lower() == "true"
    store = ConversationStore(
        client,
        embedder,
        index_name,
        isolation=os.environ.get("ISOLATION", "namespace"),
        metric="dotproduct" if hybrid else "cosine",
        sparse_encoder=HashedBm25() if hybrid else None,
    )
    store.ensure_index()
    return store


def get_reranker(store: ConversationStore):
    if is_local(store.client) or not os.environ.get("PINECONE_API_KEY"):
        return LexicalReranker()
    return PineconeReranker(store.client)


class SimilarRequest(BaseModel):
    account_id: str
    text: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=50)
    strategy: str = "dense"
    alpha: float = Field(default=0.5, ge=0.0, le=1.0)
    rerank: bool = False


class SuggestRequest(BaseModel):
    account_id: str
    text: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=20)


@app.get("/health")
def health():
    store = get_store()
    return {
        "status": "ok",
        "index": store.index_name,
        "embedder": store.embedder.model,
        "isolation": store.isolation,
        "local": is_local(store.client),
    }


@app.get("/stats")
def stats():
    return get_store().stats()


@app.post("/similar-conversations")
def similar(req: SimilarRequest):
    store = get_store()
    try:
        result = store.search(
            req.text,
            req.account_id,
            top_k=req.top_k,
            strategy=req.strategy,
            alpha=req.alpha,
            reranker=get_reranker(store) if req.rerank else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "strategy": result.strategy,
        "reranked": result.reranked,
        "latency_ms": result.latency_ms,
        "hits": [
            {"id": h.id, "score": round(h.score, 4), **h.metadata}
            for h in result.hits
        ],
    }


@app.post("/suggest-resolution")
def suggest(req: SuggestRequest):
    """Majority resolution among the nearest resolved conversations, with the
    neighbours it came from so an agent can judge the suggestion."""
    store = get_store()
    result = store.search(
        req.text,
        req.account_id,
        top_k=req.top_k,
        filter={"escalated": {"$eq": False}},
    )
    if not result.hits:
        return {"suggestion": None, "confidence": 0.0, "neighbours": []}
    votes = Counter(h.metadata.get("intent") for h in result.hits)
    intent, count = votes.most_common(1)[0]
    winner = next(h for h in result.hits if h.metadata.get("intent") == intent)
    return {
        "suggestion": winner.metadata.get("resolution"),
        "intent": intent,
        "confidence": round(count / len(result.hits), 3),
        "latency_ms": result.latency_ms,
        "neighbours": [
            {
                "id": h.id,
                "intent": h.metadata.get("intent"),
                "score": round(h.score, 4),
            }
            for h in result.hits
        ],
    }
