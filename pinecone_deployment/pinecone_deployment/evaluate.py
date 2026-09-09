"""Measurements: recall against exact search, intent precision, latency,
freshness. Everything the README's tables come from.

- **recall@k vs exact kNN.** Compute the exact top-k by cosine (or dot)
  over the same vectors with numpy; recall is the overlap with what the index
  returned. Pinecone Local is exact, so this is 1.0 there and the harness
  exists for the serverless run, where the index is approximate.
- **intent precision@k.** The corpus is labelled (each conversation has an
  intent). For a query drawn from one intent, the share of the top-k with
  the same intent. This is the number that separates lexical hashing from a
  semantic model, and dense from hybrid.
- **latency** p50 / p95 of ``store.search`` from the client's point of view.
- **freshness** upsert-to-visible seconds via ``store.wait_visible``.
"""

import statistics
import time
from dataclasses import dataclass

import numpy as np

from pinecone_deployment.conversations import Conversation
from pinecone_deployment.store import ConversationStore


@dataclass
class Evaluation:
    label: str
    queries: int
    recall_at_k: float
    intent_precision_at_k: float
    p50_ms: float
    p95_ms: float

    def row(self) -> str:
        return (
            f"| {self.label} | {self.queries} | {self.recall_at_k:.3f} | "
            f"{self.intent_precision_at_k:.3f} | {self.p50_ms:.1f} | "
            f"{self.p95_ms:.1f} |"
        )


HEADER = (
    "| strategy | queries | recall@k vs exact | intent precision@k "
    "| p50 ms | p95 ms |\n"
    "| --- | --- | --- | --- | --- | --- |"
)


def exact_top_k(
    query_vec: list[float],
    ids: list[str],
    matrix: np.ndarray,
    k: int,
    metric: str,
    tie_tolerance: float = 1e-6,
) -> list[str]:
    """Ids of every vector scoring at least the k-th best score.

    Tie-aware on purpose: the synthetic corpus has identical transcripts, so
    several vectors can share the k-th score and any of them is a correct
    answer. Without this a perfectly exact index reports recall 0.99.
    """
    q = np.asarray(query_vec, dtype=np.float32)
    if metric == "cosine":
        norms = np.linalg.norm(matrix, axis=1) * (np.linalg.norm(q) or 1.0)
        scores = matrix @ q / np.where(norms == 0, 1.0, norms)
    else:
        scores = matrix @ q
    if len(scores) == 0:
        return []
    order = np.argsort(-scores)
    kth = scores[order[min(k, len(order)) - 1]]
    return [ids[i] for i in order if scores[i] >= kth - tie_tolerance]


def evaluate(
    store: ConversationStore,
    corpus: list[Conversation],
    queries: list[Conversation],
    k: int = 10,
    strategy: str = "dense",
    alpha: float = 0.5,
    reranker=None,
    label: str | None = None,
) -> Evaluation:
    """Run each query (a held-out conversation) against its own tenant."""
    # Exact baseline over the tenant's vectors, dense side only.
    vectors = store.embedder.embed_documents([c.transcript for c in corpus])
    by_account: dict[str, tuple[list[str], np.ndarray]] = {}
    for account in {c.account_id for c in corpus}:
        members = [
            (c.vector_id, v)
            for c, v in zip(corpus, vectors)
            if c.account_id == account
        ]
        by_account[account] = (
            [m[0] for m in members],
            np.asarray([m[1] for m in members], dtype=np.float32),
        )

    recalls, precisions, latencies = [], [], []
    for query in queries:
        ids, matrix = by_account[query.account_id]
        exact = set(
            exact_top_k(
                store.embedder.embed_query(query.transcript),
                ids,
                matrix,
                k,
                store.metric,
            )
        )
        result = store.search(
            query.transcript,
            query.account_id,
            top_k=k,
            strategy=strategy,
            alpha=alpha,
            reranker=reranker,
        )
        got = [h.id for h in result.hits]
        # Share of returned hits that belong to the exact (tie-aware) top-k.
        recalls.append(len(exact & set(got)) / (len(got) or 1))
        same_intent = sum(
            1 for h in result.hits if h.metadata.get("intent") == query.intent
        )
        precisions.append(same_intent / (len(result.hits) or 1))
        latencies.append(result.latency_ms)

    latencies.sort()
    return Evaluation(
        label=label or (strategy + (" + rerank" if reranker else "")),
        queries=len(queries),
        recall_at_k=statistics.mean(recalls) if recalls else 0.0,
        intent_precision_at_k=(
            statistics.mean(precisions) if precisions else 0.0
        ),
        p50_ms=latencies[len(latencies) // 2] if latencies else 0.0,
        p95_ms=(
            latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
            if latencies
            else 0.0
        ),
    )


def measure_freshness(
    store: ConversationStore, samples: list[Conversation]
) -> dict:
    """Upsert each sample alone and time until it is fetchable."""
    waits = []
    for conv in samples:
        store.upsert([conv])
        waits.append(store.wait_visible(conv))
    waits.sort()
    return {
        "samples": len(waits),
        "p50_ms": round(1000 * waits[len(waits) // 2], 1),
        "max_ms": round(1000 * waits[-1], 1),
    }


def time_upserts(store: ConversationStore, corpus: list[Conversation]) -> dict:
    start = time.monotonic()
    written = store.upsert(corpus)
    seconds = time.monotonic() - start
    return {
        "vectors": written,
        "seconds": round(seconds, 2),
        "vectors_per_second": round(written / seconds, 1) if seconds else None,
        "batch_size": store.batch_size,
    }
