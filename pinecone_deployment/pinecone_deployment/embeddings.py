"""Embedders: text -> dense vector, behind one interface.

Three implementations, chosen by name so the same index code serves all:

- ``hash``: feature-hashed bag of words with L2 normalisation, stdlib only.
  Deterministic, dependency-free, and good enough to make nearest-neighbour
  behaviour observable in tests and against Pinecone Local. It is a *lexical*
  embedding: two phrasings of the same intent that share no words are far
  apart. That weakness is deliberate; the evaluation shows it.
- ``sentence-transformers``: a real semantic model on your own machine
  (``all-MiniLM-L6-v2``, 384 dims). Optional extra; pulls torch.
- ``pinecone``: Pinecone Inference (hosted models such as
  ``multilingual-e5-large`` / ``llama-text-embed-v2``): no model on the
  client, one API for embed and rerank, but every embed is a billed request
  and needs an API key. The ``input_type`` distinction (passage vs query)
  matters for e5-style models and is honoured here.

Every embedder exposes ``dimension`` and ``model`` so an index can be created
with the right size and tagged with the model that produced its vectors,
which is what the migration flow (``migrate.py``) keys on.
"""

import hashlib
import math
import re
from typing import Iterable, Protocol

TOKEN = re.compile(r"[a-z0-9']+")


class Embedder(Protocol):
    model: str
    dimension: int

    def embed_documents(self, texts: list[str]) -> list[list[float]]: ...

    def embed_query(self, text: str) -> list[float]: ...


def tokenize(text: str) -> list[str]:
    return TOKEN.findall(text.lower())


class HashEmbedder:
    """Feature hashing over unigrams + bigrams, L2-normalised."""

    def __init__(self, dimension: int = 256):
        self.dimension = dimension
        self.model = f"hash-v1-{dimension}"

    def _vector(self, text: str) -> list[float]:
        vec = [0.0] * self.dimension
        tokens = tokenize(text)
        grams = tokens + [f"{a}_{b}" for a, b in zip(tokens, tokens[1:])]
        for gram in grams:
            digest = hashlib.blake2b(gram.encode(), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "little") % self.dimension
            sign = 1.0 if digest[4] & 1 else -1.0
            vec[bucket] += sign
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._vector(text)


class SentenceTransformerEmbedder:
    """Local semantic embeddings (optional extra ``local-embeddings``)."""

    def __init__(self, model: str = "all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(model)
        self.model = model
        self.dimension = self._model.get_sentence_embedding_dimension()

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._model.encode(
            texts, normalize_embeddings=True, batch_size=64
        ).tolist()

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]


class PineconeInferenceEmbedder:
    """Pinecone-hosted embedding models via ``pc.inference.embed``.

    Batches of up to 96 inputs per call (the API limit for e5), passage vs
    query ``input_type`` so asymmetric models embed each side correctly.
    """

    DIMENSIONS = {"multilingual-e5-large": 1024, "llama-text-embed-v2": 1024}

    def __init__(
        self,
        client,
        model: str = "multilingual-e5-large",
        batch_size: int = 96,
    ):
        self._client = client
        self.model = model
        self.dimension = self.DIMENSIONS[model]
        self.batch_size = batch_size

    def _embed(
        self, texts: Iterable[str], input_type: str
    ) -> list[list[float]]:
        texts = list(texts)
        out: list[list[float]] = []
        for start in range(0, len(texts), self.batch_size):
            response = self._client.inference.embed(
                model=self.model,
                inputs=texts[start : start + self.batch_size],  # noqa: E203
                parameters={"input_type": input_type, "truncate": "END"},
            )
            out.extend(list(item["values"]) for item in response)
        return out

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._embed(texts, "passage")

    def embed_query(self, text: str) -> list[float]:
        return self._embed([text], "query")[0]


def make_embedder(name: str, client=None, **kwargs) -> Embedder:
    if name == "hash":
        return HashEmbedder(**kwargs)
    if name == "sentence-transformers":
        return SentenceTransformerEmbedder(**kwargs)
    if name == "pinecone":
        if client is None:
            raise ValueError("the pinecone embedder needs a Pinecone client")
        return PineconeInferenceEmbedder(client, **kwargs)
    raise ValueError(f"unknown embedder {name!r}")
