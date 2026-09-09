"""Rerankers: re-score a candidate list with a stronger model.

Retrieval gets ``rerank_candidates`` results cheaply from the index; a
reranker then reads the actual text of each candidate against the query and
reorders them. Precision goes up, latency goes up by one more model call.

- ``PineconeReranker`` uses Pinecone Inference (``bge-reranker-v2-m3`` or
  ``cohere-rerank-3.5``): hosted, billed per request, needs an API key. The
  document text comes from the ``resolution`` / transcript metadata field.
- ``LexicalReranker`` is the dependency-free stand-in: token overlap between
  the query and the candidate's text. It exists so the rerank code path runs
  locally and in tests; its quality is not the point.
"""

from pinecone_deployment.embeddings import tokenize
from pinecone_deployment.store import SearchHit


class LexicalReranker:
    def __init__(self, text_field: str = "resolution"):
        self.text_field = text_field
        self.model = "lexical-overlap"

    def rerank(
        self, query: str, hits: list[SearchHit], top_k: int
    ) -> list[SearchHit]:
        q = set(tokenize(query))
        rescored = []
        for hit in hits:
            doc = set(tokenize(str(hit.metadata.get(self.text_field, ""))))
            overlap = len(q & doc) / (len(q) or 1)
            rescored.append(
                SearchHit(
                    hit.id,
                    round(0.5 * hit.score + 0.5 * overlap, 6),
                    hit.metadata,
                )
            )
        rescored.sort(key=lambda h: h.score, reverse=True)
        return rescored[:top_k]


class PineconeReranker:
    def __init__(
        self,
        client,
        model: str = "bge-reranker-v2-m3",
        text_field: str = "resolution",
    ):
        self._client = client
        self.model = model
        self.text_field = text_field

    def rerank(
        self, query: str, hits: list[SearchHit], top_k: int
    ) -> list[SearchHit]:
        documents = [
            {"id": h.id, "text": str(h.metadata.get(self.text_field, ""))}
            for h in hits
        ]
        response = self._client.inference.rerank(
            model=self.model,
            query=query,
            documents=documents,
            top_n=top_k,
            return_documents=False,
        )
        by_id = {h.id: h for h in hits}
        return [
            SearchHit(
                documents[r.index]["id"],
                float(r.score),
                by_id[documents[r.index]["id"]].metadata,
            )
            for r in response.data
        ]
