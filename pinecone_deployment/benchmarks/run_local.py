"""Local benchmark against Pinecone Local: the README's measured tables.

    uv run python benchmarks/run_local.py \
        --count 2000 --accounts 20 --queries 200

Compares tenant isolation strategies (namespace vs filter), retrieval
strategies (dense, hybrid at several alphas, dense + rerank), upsert
throughput by batch size, and freshness. Pinecone Local is exact and
in-memory, so recall@k is 1.0 by construction and the latencies are a floor,
not a forecast; the *relative* numbers (filter vs namespace, hybrid vs dense,
rerank overhead) and the intent-precision numbers are what transfer. Run the
same script with PINECONE_API_KEY set for the serverless numbers.
"""

import argparse
import time
import uuid

from pinecone_deployment.conversations import generate
from pinecone_deployment.embeddings import make_embedder
from pinecone_deployment.evaluate import (
    HEADER,
    evaluate,
    measure_freshness,
    time_upserts,
)
from pinecone_deployment.rerank import LexicalReranker
from pinecone_deployment.sparse import HashedBm25
from pinecone_deployment.store import ConversationStore, connect, is_local


def settle(store, expected):
    for _ in range(600):
        if store.stats()["total_vectors"] >= expected:
            return
        time.sleep(0.1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--accounts", type=int, default=20)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--embedder", default="hash")
    args = parser.parse_args(argv)

    client = connect()
    embedder = (
        make_embedder(args.embedder, client=client)
        if args.embedder == "pinecone"
        else make_embedder(args.embedder)
    )
    corpus = generate(args.count, args.accounts, seed=42)
    accounts = {c.account_id for c in corpus}
    queries = [
        q
        for q in generate(args.queries * 2, args.accounts, seed=4242)
        if q.account_id in accounts
    ][: args.queries]
    run_id = uuid.uuid4().hex[:6]
    print(
        f"Pinecone {'Local' if is_local(client) else 'serverless'} — "
        f"{len(corpus):,} conversations, {len(accounts)} accounts, "
        f"{len(queries)} queries, k={args.k}, "
        f"embedder={embedder.model}\n"
    )

    stores = {
        "namespace / dense": ConversationStore(
            client, embedder, f"bench-ns-{run_id}", isolation="namespace"
        ),
        "filter / dense": ConversationStore(
            client, embedder, f"bench-flt-{run_id}", isolation="filter"
        ),
        "namespace / hybrid": ConversationStore(
            client,
            embedder,
            f"bench-hyb-{run_id}",
            isolation="namespace",
            metric="dotproduct",
            sparse_encoder=HashedBm25(),
        ),
    }
    try:
        print(
            "## Ingestion\n\n"
            "| index | vectors | seconds | vectors/s | batch |\n"
            "| --- | --- | --- | --- | --- |"
        )
        for label, store in stores.items():
            store.ensure_index()
            t = time_upserts(store, corpus)
            settle(store, len(corpus))
            print(
                f"| {label} | {t['vectors']:,} | {t['seconds']} | "
                f"{t['vectors_per_second']} | {t['batch_size']} |"
            )

        # Batch-size sweep on a scratch index (serverless bills write
        # units per batch).
        print(
            "\n## Upsert batch size (namespace / dense)\n\n"
            "| batch | vectors/s |\n| --- | --- |"
        )
        for batch in (10, 50, 100, 200):
            scratch = ConversationStore(
                client, embedder, f"bench-b{batch}-{run_id}", batch_size=batch
            )
            scratch.ensure_index()
            try:
                rate = time_upserts(scratch, corpus[:1000])
                print(f"| {batch} | {rate['vectors_per_second']} |")
            finally:
                scratch.delete_index()

        print(f"\n## Retrieval (k={args.k})\n\n" + HEADER)
        rows = [
            evaluate(
                stores["namespace / dense"],
                corpus,
                queries,
                k=args.k,
                label="namespace / dense",
            ),
            evaluate(
                stores["filter / dense"],
                corpus,
                queries,
                k=args.k,
                label="filter / dense",
            ),
            evaluate(
                stores["namespace / dense"],
                corpus,
                queries,
                k=args.k,
                reranker=LexicalReranker(),
                label="namespace / dense + lexical rerank",
            ),
        ]
        for alpha in (0.0, 0.3, 0.5, 0.7, 1.0):
            rows.append(
                evaluate(
                    stores["namespace / hybrid"],
                    corpus,
                    queries,
                    k=args.k,
                    strategy="hybrid",
                    alpha=alpha,
                    label=f"namespace / hybrid alpha={alpha}",
                )
            )
        for r in rows:
            print(r.row())

        print("\n## Freshness (upsert -> fetch visible)\n")
        fresh = measure_freshness(
            stores["namespace / dense"], generate(20, args.accounts, seed=777)
        )
        print(
            f"samples={fresh['samples']} p50={fresh['p50_ms']} ms "
            f"max={fresh['max_ms']} ms"
        )

        print("\n## Tenant offboarding\n")
        victim = sorted(accounts)[0]
        for label in ("namespace / dense", "filter / dense"):
            t0 = time.monotonic()
            stores[label].delete_account(victim)
            print(
                f"{label}: delete_account({victim}) in "
                f"{1000 * (time.monotonic() - t0):.0f} ms"
            )
    finally:
        for store in stores.values():
            store.delete_index()


if __name__ == "__main__":
    main()
