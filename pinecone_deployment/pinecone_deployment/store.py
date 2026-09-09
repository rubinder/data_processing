"""Index lifecycle, ingestion and retrieval against Pinecone (local or cloud).

Design decisions this module encodes, each measured in the README:

- **Deterministic ids** (``account:conversation``) so re-embedding a
  conversation is an in-place upsert, not a duplicate: the same idempotency
  rule the Spark jobs use for partitions.
- **Tenant isolation strategy is a parameter**, not a foregone conclusion.
  ``namespace`` puts each account in its own namespace (hard isolation,
  queries never see other tenants, per-tenant delete is one call);
  ``filter`` keeps one namespace and applies ``account_id`` as a metadata
  filter (one hot namespace, simpler ops, cross-tenant analytics possible).
- **Batched upserts** of ``batch_size`` vectors with exponential-backoff
  retries; Pinecone's request cap is 2MB / batch and write units are billed
  per batch on serverless, so batch size is a cost knob as well as a
  throughput one.
- **Dense, hybrid and reranked retrieval** through one ``search`` method so
  the evaluation can compare them like for like.
- **Model tag on the index** (``tags={"model": ...}``) so nothing ever
  queries an index with vectors from a different embedder than the one that
  embeds the query, which is the silent failure mode of model upgrades.
"""

import os
import time
from dataclasses import dataclass, field

from pinecone import Pinecone, ServerlessSpec

from pinecone_deployment.conversations import Conversation
from pinecone_deployment.embeddings import Embedder
from pinecone_deployment.sparse import HashedBm25, hybrid_scale

LOCAL_API_KEY = "pclocal"


def connect(
    api_key: str | None = None,
    local_host: str | None = None,
) -> Pinecone:
    """Pinecone client for the cloud (API key) or Pinecone Local (host).

    Precedence: explicit arguments, then ``PINECONE_API_KEY`` (cloud), then
    ``PINECONE_LOCAL_HOST`` (default ``http://localhost:5080``).
    """
    api_key = api_key or os.environ.get("PINECONE_API_KEY")
    if api_key and not local_host:
        return Pinecone(api_key=api_key)
    host = local_host or os.environ.get(
        "PINECONE_LOCAL_HOST", "http://localhost:5080"
    )
    return Pinecone(api_key=LOCAL_API_KEY, host=host)


def is_local(client: Pinecone) -> bool:
    return client.config.api_key == LOCAL_API_KEY


@dataclass
class SearchHit:
    id: str
    score: float
    metadata: dict = field(default_factory=dict)


@dataclass
class SearchResult:
    hits: list[SearchHit]
    strategy: str
    latency_ms: float
    reranked: bool = False


class ConversationStore:
    def __init__(
        self,
        client: Pinecone,
        embedder: Embedder,
        index_name: str,
        isolation: str = "namespace",
        metric: str = "cosine",
        sparse_encoder: HashedBm25 | None = None,
        cloud: str = "aws",
        region: str = "us-east-1",
        batch_size: int = 100,
    ):
        if isolation not in ("namespace", "filter"):
            raise ValueError("isolation must be 'namespace' or 'filter'")
        if sparse_encoder is not None and metric != "dotproduct":
            raise ValueError(
                "hybrid (sparse-dense) indexes must use the dotproduct metric"
            )
        self.client = client
        self.embedder = embedder
        self.index_name = index_name
        self.isolation = isolation
        self.metric = metric
        self.sparse_encoder = sparse_encoder
        self.cloud = cloud
        self.region = region
        self.batch_size = batch_size
        self._index = None

    # -- lifecycle -----------------------------------------------------------

    META_NAMESPACE = "__meta__"
    META_ID = "__index_meta__"

    def ensure_index(self) -> None:
        """Create the index if missing; refuse one built by another model.

        The producing model is recorded twice: as an index tag (cloud) and
        as a metadata-only record in a reserved namespace (works everywhere,
        including Pinecone Local, which does not return tags). Either one
        disagreeing with the embedder is fatal: a query embedded by model B
        against model-A vectors returns confident nonsense.
        """
        created = False
        if not self.client.has_index(self.index_name):
            self.client.create_index(
                name=self.index_name,
                dimension=self.embedder.dimension,
                metric=self.metric,
                spec=ServerlessSpec(cloud=self.cloud, region=self.region),
                deletion_protection="disabled",
                tags={
                    "model": self.embedder.model,
                    "isolation": self.isolation,
                },
            )
            self._wait_ready()
            created = True
        description = self.client.describe_index(self.index_name)
        if description.dimension != self.embedder.dimension:
            raise RuntimeError(
                f"index dimension {description.dimension} != embedder "
                f"dimension {self.embedder.dimension}"
            )
        tagged = (getattr(description, "tags", None) or {}).get("model")
        recorded = tagged or self._read_meta().get("model")
        if created and not recorded:
            self._write_meta()
        elif recorded and recorded != self.embedder.model:
            raise RuntimeError(
                f"index {self.index_name} holds {recorded} vectors; "
                "embedder is "
                f"{self.embedder.model}. Use migrate.py, do not mix models."
            )
        elif not recorded:
            self._write_meta()

    def _write_meta(self) -> None:
        # Unit vector on axis 0 as the placeholder; only metadata matters.
        values = [1.0] + [0.0] * (self.embedder.dimension - 1)
        self.index.upsert(
            vectors=[
                {
                    "id": self.META_ID,
                    "values": values,
                    "metadata": {
                        "model": self.embedder.model,
                        "dimension": self.embedder.dimension,
                        "isolation": self.isolation,
                    },
                }
            ],
            namespace=self.META_NAMESPACE,
        )
        for _ in range(200):
            if self._read_meta():
                return
            time.sleep(0.02)

    def _read_meta(self) -> dict:
        try:
            fetched = self.index.fetch(
                ids=[self.META_ID], namespace=self.META_NAMESPACE
            )
        except Exception:  # noqa: BLE001 - index not ready yet
            return {}
        record = fetched.vectors.get(self.META_ID)
        return dict(record.metadata or {}) if record else {}

    def _wait_ready(self, timeout: float = 120.0) -> None:
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            status = self.client.describe_index(self.index_name).status
            if getattr(status, "ready", False) or (
                isinstance(status, dict) and status.get("ready")
            ):
                return
            time.sleep(0.5)

    @property
    def index(self):
        if self._index is None:
            description = self.client.describe_index(self.index_name)
            host = description.host
            if is_local(self.client) and not host.startswith("http"):
                host = f"http://{host}"
            self._index = self.client.Index(host=host)
        return self._index

    def delete_index(self) -> None:
        if self.client.has_index(self.index_name):
            self.client.delete_index(self.index_name)
        self._index = None

    # -- placement -----------------------------------------------------------

    def _namespace(self, account_id: str | None) -> str:
        if self.isolation == "namespace":
            if not account_id:
                raise ValueError("namespace isolation needs an account_id")
            return account_id
        return "conversations"

    def _filter(
        self, account_id: str | None, extra: dict | None
    ) -> dict | None:
        clauses = []
        if self.isolation == "filter" and account_id:
            clauses.append({"account_id": {"$eq": account_id}})
        if extra:
            clauses.append(extra)
        if not clauses:
            return None
        return clauses[0] if len(clauses) == 1 else {"$and": clauses}

    # -- ingestion -----------------------------------------------------------

    def upsert(
        self, conversations: list[Conversation], max_attempts: int = 5
    ) -> int:
        """Embed and upsert in batches; returns vectors written."""
        if self.sparse_encoder is not None and self.sparse_encoder.n_docs == 0:
            self.sparse_encoder.fit([c.transcript for c in conversations])
        written = 0
        by_namespace: dict[str, list[Conversation]] = {}
        for conv in conversations:
            by_namespace.setdefault(
                self._namespace(conv.account_id), []
            ).append(conv)
        for namespace, group in by_namespace.items():
            for start in range(0, len(group), self.batch_size):
                batch = group[start : start + self.batch_size]  # noqa: E203
                dense = self.embedder.embed_documents(
                    [c.transcript for c in batch]
                )
                vectors = []
                for conv, values in zip(batch, dense):
                    record = {
                        "id": conv.vector_id,
                        "values": values,
                        "metadata": conv.metadata(),
                    }
                    if self.sparse_encoder is not None:
                        record["sparse_values"] = (
                            self.sparse_encoder.encode_document(
                                conv.transcript
                            )
                        )
                    vectors.append(record)
                self._upsert_with_retry(vectors, namespace, max_attempts)
                written += len(vectors)
        return written

    def _upsert_with_retry(self, vectors, namespace, max_attempts):
        delay = 0.5
        for attempt in range(1, max_attempts + 1):
            try:
                self.index.upsert(vectors=vectors, namespace=namespace)
                return
            except (
                Exception
            ):  # noqa: BLE001 - transient 429/5xx from the service
                if attempt == max_attempts:
                    raise
                time.sleep(delay)
                delay *= 2

    def wait_visible(
        self, conversation: Conversation, timeout: float = 30.0
    ) -> float:
        """Poll fetch until the vector is readable; return seconds waited.

        Serverless indexes are eventually consistent: an upsert acknowledges
        before the vector is queryable. Freshness is a measured number here,
        not an assumption.
        """
        start = time.monotonic()
        namespace = self._namespace(conversation.account_id)
        while time.monotonic() - start < timeout:
            fetched = self.index.fetch(
                ids=[conversation.vector_id], namespace=namespace
            )
            if fetched.vectors:
                return time.monotonic() - start
            time.sleep(0.02)
        raise TimeoutError(
            f"{conversation.vector_id} not visible after {timeout}s"
        )

    def delete_account(self, account_id: str) -> None:
        """Tenant offboarding. One call per namespace, or a metadata delete
        (serverless: delete-by-filter is not supported, so list ids by
        prefix then delete by id)."""
        if self.isolation == "namespace":
            self.index.delete(delete_all=True, namespace=account_id)
            return
        namespace = self._namespace(None)
        ids = [
            vid
            for page in self.index.list(
                prefix=f"{account_id}:", namespace=namespace
            )
            for vid in page
        ]
        for start in range(0, len(ids), 1000):
            self.index.delete(
                ids=ids[start : start + 1000],  # noqa: E203
                namespace=namespace,
            )

    def stats(self) -> dict:
        """Vector counts excluding the reserved metadata namespace."""
        s = self.index.describe_index_stats()
        namespaces = {
            k: v.vector_count
            for k, v in (s.namespaces or {}).items()
            if k != self.META_NAMESPACE
        }
        return {
            "total_vectors": sum(namespaces.values()),
            "namespaces": namespaces,
            "dimension": s.dimension,
        }

    # -- retrieval -----------------------------------------------------------

    def search(
        self,
        text: str,
        account_id: str | None,
        top_k: int = 10,
        strategy: str = "dense",
        alpha: float = 0.5,
        filter: dict | None = None,
        reranker=None,
        rerank_candidates: int | None = None,
    ) -> SearchResult:
        """Nearest conversations for ``text`` within the tenant.

        ``strategy``: ``dense`` (embedding only), ``hybrid`` (dense + BM25
        sparse, weighted by ``alpha``). ``reranker`` re-scores the top
        ``rerank_candidates`` (default 3 x top_k) and returns the best top_k.
        """
        if strategy not in ("dense", "hybrid"):
            raise ValueError("strategy must be 'dense' or 'hybrid'")
        if strategy == "hybrid" and self.sparse_encoder is None:
            raise ValueError(
                "hybrid search needs a sparse_encoder on the store"
            )
        dense = self.embedder.embed_query(text)
        kwargs = {
            "top_k": rerank_candidates or (3 * top_k if reranker else top_k),
            "namespace": self._namespace(account_id),
            "include_metadata": True,
        }
        flt = self._filter(account_id, filter)
        if flt:
            kwargs["filter"] = flt
        if strategy == "hybrid":
            sparse = self.sparse_encoder.encode_query(text)
            dense, sparse = hybrid_scale(dense, sparse, alpha)
            kwargs["sparse_vector"] = sparse
        start = time.monotonic()
        response = self.index.query(vector=dense, **kwargs)
        hits = [
            SearchHit(m.id, float(m.score), dict(m.metadata or {}))
            for m in response.matches
        ]
        reranked = False
        if reranker is not None and hits:
            hits = reranker.rerank(text, hits, top_k)
            reranked = True
        latency_ms = (time.monotonic() - start) * 1000
        return SearchResult(
            hits[:top_k], strategy, round(latency_ms, 2), reranked
        )
