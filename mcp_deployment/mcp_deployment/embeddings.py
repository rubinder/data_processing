"""Text -> vector, behind one small interface.

The default is a feature-hashing embedder: deterministic, dependency-free,
and *lexical* -- two phrasings of the same question that share no words are
far apart. That is enough to make ranking observable and testable offline.
``sentence-transformers`` is the optional real thing (``all-MiniLM-L6-v2``,
384 dims, the same dimension so the catalog table does not change).
"""
from __future__ import annotations

import hashlib
import math
import os
import re
from typing import Protocol

from mcp_deployment.config import EMBEDDING_DIMENSION

TOKEN = re.compile(r"[a-z0-9_']+")


class Embedder(Protocol):
    model: str
    dimension: int

    def embed(self, texts: list[str]) -> list[list[float]]: ...


def tokenize(text: str) -> list[str]:
    tokens = TOKEN.findall(text.lower())
    # snake_case identifiers also contribute their parts, so "funnel_depth"
    # matches a question about "funnel depth".
    parts = [p for t in tokens if "_" in t for p in t.split("_") if p]
    return tokens + parts


class HashEmbedder:
    """Feature hashing over unigrams and bigrams, L2-normalised."""

    def __init__(self, dimension: int = EMBEDDING_DIMENSION):
        self.dimension = dimension
        self.model = f"hash-v1-{dimension}"

    def _vector(self, text: str) -> list[float]:
        vec = [0.0] * self.dimension
        tokens = tokenize(text)
        grams = tokens + [f"{a}_{b}" for a, b in zip(tokens, tokens[1:])]
        for gram in grams:
            digest = hashlib.blake2b(gram.encode(), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "little") % self.dimension
            vec[bucket] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]


class SentenceTransformerEmbedder:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer  # optional extra

        self._model = SentenceTransformer(model_name)
        self.model = f"sentence-transformers/{model_name}"
        self.dimension = int(self._model.get_sentence_embedding_dimension())
        if self.dimension != EMBEDDING_DIMENSION:
            raise ValueError(f"{model_name} produces {self.dimension} dims; the catalog "
                             f"column is vector({EMBEDDING_DIMENSION})")

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [list(map(float, v)) for v in
                self._model.encode(texts, normalize_embeddings=True)]


def get_embedder(name: str | None = None) -> Embedder:
    choice = (name or os.environ.get("CATALOG_EMBEDDER") or "hash").lower()
    if choice == "hash":
        return HashEmbedder()
    if choice in ("sentence-transformers", "minilm"):
        return SentenceTransformerEmbedder()
    raise ValueError(f"unknown embedder: {choice!r} (expected 'hash' or 'sentence-transformers')")


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def vector_literal(vec: list[float]) -> str:
    """pgvector's text input form."""
    return "[" + ",".join(f"{v:.8f}" for v in vec) + "]"
