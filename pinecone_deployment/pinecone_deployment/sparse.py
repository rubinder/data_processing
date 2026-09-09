"""Sparse (lexical) vectors for hybrid search.

Pinecone hybrid search is one query carrying a dense vector and a sparse
vector against a ``dotproduct`` index. The sparse side is where exact terms
live: an order number, a product code, a word the embedding model has never
seen. Two encoders:

- ``HashedBm25``: stdlib BM25 with feature-hashed term ids. Fit on the corpus
  for document frequencies; deterministic; no download. Used by the tests
  and the local benchmark.
- ``pinecone-text``'s ``BM25Encoder`` (optional extra ``hybrid``) is the
  production-grade equivalent with a real tokenizer; same interface.

``hybrid_scale`` implements Pinecone's documented convex combination: scale
the dense vector by ``alpha`` and the sparse values by ``1 - alpha`` before
querying, so ``alpha=1`` is pure dense and ``alpha=0`` pure lexical.
"""

import hashlib
import math
from collections import Counter

from pinecone_deployment.embeddings import tokenize


class HashedBm25:
    def __init__(self, buckets: int = 2**20, k1: float = 1.2, b: float = 0.75):
        self.buckets = buckets
        self.k1 = k1
        self.b = b
        self.doc_freq: Counter = Counter()
        self.n_docs = 0
        self.avg_len = 1.0

    def _term_id(self, token: str) -> int:
        digest = hashlib.blake2b(token.encode(), digest_size=8).digest()
        return int.from_bytes(digest[:4], "little") % self.buckets

    def fit(self, texts: list[str]) -> "HashedBm25":
        lengths = []
        for text in texts:
            tokens = set(tokenize(text))
            lengths.append(len(tokenize(text)))
            for tok in tokens:
                self.doc_freq[self._term_id(tok)] += 1
        self.n_docs = len(texts)
        self.avg_len = (sum(lengths) / len(lengths)) if lengths else 1.0
        return self

    def _idf(self, term_id: int) -> float:
        df = self.doc_freq.get(term_id, 0)
        return math.log(1 + (self.n_docs - df + 0.5) / (df + 0.5))

    def encode_document(self, text: str) -> dict:
        tokens = tokenize(text)
        tf = Counter(self._term_id(t) for t in tokens)
        length = len(tokens) or 1
        indices, values = [], []
        for term_id, freq in sorted(tf.items()):
            denom = freq + self.k1 * (
                1 - self.b + self.b * length / self.avg_len
            )
            score = self._idf(term_id) * freq * (self.k1 + 1) / denom
            if score > 0:
                indices.append(term_id)
                values.append(float(score))
        return {"indices": indices, "values": values}

    def encode_query(self, text: str) -> dict:
        tf = Counter(self._term_id(t) for t in tokenize(text))
        indices, values = [], []
        for term_id, freq in sorted(tf.items()):
            indices.append(term_id)
            values.append(float(self._idf(term_id) * freq))
        return {"indices": indices, "values": values}


def hybrid_scale(
    dense: list[float], sparse: dict, alpha: float
) -> tuple[list[float], dict]:
    """Convex weighting of the two sides (0 <= alpha <= 1)."""
    if not 0.0 <= alpha <= 1.0:
        raise ValueError("alpha must be in [0, 1]")
    scaled_dense = [v * alpha for v in dense]
    scaled_sparse = {
        "indices": list(sparse["indices"]),
        "values": [v * (1 - alpha) for v in sparse["values"]],
    }
    return scaled_dense, scaled_sparse
