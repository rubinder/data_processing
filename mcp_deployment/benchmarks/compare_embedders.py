"""Hashing embedder vs sentence-transformers on a fixed, labelled question set.

In-memory ranking over the real catalog entries (dbt artifacts + templates),
so no database is needed and the result is a property of the embedder, not
of pgvector. Reports hit@1, hit@3 and MRR overall and on the paraphrase
subset, plus per-query embed latency, and writes a markdown table.

    HF_HOME=.cache/hf uv run --extra test --extra semantic python benchmarks/compare_embedders.py
"""
from __future__ import annotations

import statistics
import sys
import time
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_deployment import catalog, templates  # noqa: E402
from mcp_deployment.embeddings import HashEmbedder, SentenceTransformerEmbedder  # noqa: E402

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"


def evaluate(embedder, entries, questions) -> dict:
    vectors = embedder.embed([e.content for e in entries])
    hits1 = hits3 = kind_hits1 = 0
    rr = []
    misses = []
    latencies = []
    para = {"n": 0, "hit1": 0, "hit3": 0, "rr": []}
    for item in questions:
        started = time.perf_counter()
        q = embedder.embed([item["q"]])[0]
        latencies.append((time.perf_counter() - started) * 1000)
        ranked = sorted(zip(entries, (catalog.cosine(q, v) for v in vectors)),
                        key=lambda x: -x[1])
        ids = [e.entry_id for e, _ in ranked]
        rank = ids.index(item["expect"]) + 1
        # The call an agent actually makes: search within one kind. Most
        # unfiltered misses are a column of the right table outranking the
        # table or template entry, which this removes.
        expect_kind = item["expect"].split(":")[0]
        within_kind = [i for i in ids if i.startswith(expect_kind + ":")]
        kind_hits1 += within_kind[0] == item["expect"]
        hit1 = rank == 1
        hit3 = rank <= 3
        hits1 += hit1
        hits3 += hit3
        rr.append(1 / rank)
        if item.get("paraphrase"):
            para["n"] += 1
            para["hit1"] += hit1
            para["hit3"] += hit3
            para["rr"].append(1 / rank)
        if not hit1:
            misses.append((item["q"], item["expect"], rank, ids[0]))
    n = len(questions)
    return {
        "model": embedder.model, "n": n,
        "hit1": hits1 / n, "hit3": hits3 / n, "mrr": statistics.mean(rr),
        "kind_hit1": kind_hits1 / n,
        "para_n": para["n"], "para_hit1": para["hit1"] / para["n"],
        "para_hit3": para["hit3"] / para["n"], "para_mrr": statistics.mean(para["rr"]),
        "embed_p50_ms": statistics.median(latencies),
        "embed_max_ms": max(latencies), "misses": misses,
    }


def main() -> int:
    questions = yaml.safe_load((HERE / "questions.yaml").read_text())
    entries = catalog.entries_from_dbt() + catalog.entries_from_templates(
        templates.load_templates())
    results = [evaluate(HashEmbedder(), entries, questions)]
    try:
        results.append(evaluate(SentenceTransformerEmbedder(), entries, questions))
    except ImportError:
        print("sentence-transformers not installed: uv sync --extra semantic", file=sys.stderr)

    lines = [f"# Embedder comparison — {len(entries)} catalog entries, {len(questions)} "
             f"questions ({results[0]['para_n']} paraphrases)", "",
             "| Embedder | hit@1 | hit@3 | MRR | hit@1 within kind | paraphrase hit@1 | "
             "paraphrase hit@3 | paraphrase MRR | query embed p50 | max |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for r in results:
        lines.append(f"| `{r['model']}` | {r['hit1']:.0%} | {r['hit3']:.0%} | {r['mrr']:.2f} "
                     f"| {r['kind_hit1']:.0%} "
                     f"| {r['para_hit1']:.0%} | {r['para_hit3']:.0%} | {r['para_mrr']:.2f} "
                     f"| {r['embed_p50_ms']:.1f} ms | {r['embed_max_ms']:.1f} ms |")
    for r in results:
        lines += ["", f"## Misses at rank 1 — `{r['model']}`", ""]
        if not r["misses"]:
            lines.append("none")
        for q, expect, rank, top in r["misses"]:
            lines.append(f"- \"{q}\" → expected `{expect}` at rank {rank}; ranked `{top}` first")
    out = RESULTS / "embedders.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nwritten to {out.relative_to(HERE.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
