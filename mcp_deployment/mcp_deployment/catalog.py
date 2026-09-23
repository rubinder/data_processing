"""The semantic catalog: what exists in gold, and what an agent may ask for.

Entries are generated, not hand-written: gold tables and their columns come
from the dbt manifest (descriptions) and catalog (physical types), templates
from ``templates/*.yaml``. Each entry carries a content hash, so a sync
embeds and writes only what changed and deletes what is gone -- the catalog
follows the warehouse without anyone remembering to update it.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

from mcp_deployment import config
from mcp_deployment.embeddings import Embedder, cosine, vector_literal
from mcp_deployment.templates import Template

EXCLUDED_PREFIXES = ("stg_", "int_")


@dataclass(frozen=True)
class Entry:
    entry_id: str
    kind: str          # table | column | template
    name: str
    table_name: str | None
    description: str
    content: str
    ordinal: int = 0

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class SyncResult:
    inserted: int
    updated: int
    deleted: int
    unchanged: int
    embedder: str


def _gold_models(manifest: dict, source_schema: str) -> list[dict]:
    return sorted(
        (n for n in manifest["nodes"].values()
         if n.get("resource_type") == "model" and n.get("schema") == source_schema
         and not n["name"].startswith(EXCLUDED_PREFIXES)),
        key=lambda n: n["name"])


def entries_from_dbt(manifest_path: Path | None = None, catalog_path: Path | None = None,
                     source_schema: str = config.SOURCE_SCHEMA) -> list[Entry]:
    """Table and column entries for every dbt model that is promoted to gold."""
    target = config.DBT_TARGET_DIR
    manifest = json.loads(Path(manifest_path or target / "manifest.json").read_text())
    catalog_file = Path(catalog_path or target / "catalog.json")
    physical = json.loads(catalog_file.read_text())["nodes"] if catalog_file.exists() else {}

    entries: list[Entry] = []
    for node in _gold_models(manifest, source_schema):
        name = node["name"]
        phys = physical.get(node["unique_id"], {}).get("columns", {})
        declared = node.get("columns", {})
        column_names = list(phys) or list(declared)
        description = " ".join((node.get("description") or "").split()) or f"gold table {name}"
        col_summary = ", ".join(
            f"{c} ({phys.get(c, {}).get('type') or declared.get(c, {}).get('data_type') or '?'})"
            for c in column_names)
        entries.append(Entry(
            entry_id=f"table:{name}", kind="table", name=name, table_name=name,
            description=description,
            content=f"gold table {name}: {description} Columns: {col_summary}."))
        for col in column_names:
            col_type = phys.get(col, {}).get("type") or declared.get(col, {}).get("data_type") or ""
            col_desc = " ".join((declared.get(col, {}).get("description") or "").split())
            entries.append(Entry(
                entry_id=f"column:{name}.{col}", kind="column", name=col, table_name=name,
                description=col_desc or f"{col} of {name}",
                content=f"column {name}.{col} ({col_type}): {col_desc or ''} "
                        f"in gold table {name}, {description}",
                ordinal=int(phys.get(col, {}).get("index") or column_names.index(col) + 1)))
    return entries


def entries_from_templates(templates: dict[str, Template]) -> list[Entry]:
    entries = []
    for t in sorted(templates.values(), key=lambda x: x.name):
        params = "; ".join(f"{p.name} ({p.type}{'' if p.required else ', optional'}): "
                           f"{p.description}" for p in t.params) or "none"
        entries.append(Entry(
            entry_id=f"template:{t.name}", kind="template", name=t.name, table_name=None,
            description=t.description,
            content=f"query template {t.name}: {t.description} Parameters: {params}. "
                    f"Returns: {', '.join(t.returns) or 'see description'}. "
                    f"Tags: {', '.join(t.tags)}."))
    return entries


def rank(entries: list[Entry], query: str, embedder: Embedder,
         kinds: tuple[str, ...] | None = None, limit: int = 10) -> list[tuple[Entry, float]]:
    """In-memory ranking by cosine similarity. The database does the same
    thing with pgvector; this is the reference the tests hold it to."""
    pool = [e for e in entries if not kinds or e.kind in kinds]
    if not pool:
        return []
    vectors = embedder.embed([e.content for e in pool])
    q = embedder.embed([query])[0]
    scored = sorted(zip(pool, (cosine(q, v) for v in vectors)), key=lambda x: -x[1])
    return scored[:limit]


def sync(conn, entries: list[Entry], embedder: Embedder) -> SyncResult:
    """Upsert changed entries, delete vanished ones. Embeds only what changed,
    unless the embedder itself changed, in which case everything is redone."""
    # Everything below runs in one transaction and is committed before
    # returning: a connection that already has an implicit transaction open
    # would otherwise leave the sync invisible to every other session.
    existing = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT entry_id, content_hash, embedder FROM catalog.entries").fetchall()}
    wanted = {e.entry_id: e for e in entries}
    to_embed = [e for e in entries
                if existing.get(e.entry_id) != (e.content_hash, embedder.model)]
    inserted = sum(1 for e in to_embed if e.entry_id not in existing)
    updated = len(to_embed) - inserted
    vectors = embedder.embed([e.content for e in to_embed]) if to_embed else []
    with conn.transaction():
        for entry, vec in zip(to_embed, vectors):
            conn.execute(
                """INSERT INTO catalog.entries
                       (entry_id, kind, name, table_name, description, content,
                        content_hash, embedder, embedding, ordinal, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s, now())
                   ON CONFLICT (entry_id) DO UPDATE SET
                       kind = EXCLUDED.kind, name = EXCLUDED.name,
                       table_name = EXCLUDED.table_name, description = EXCLUDED.description,
                       content = EXCLUDED.content, content_hash = EXCLUDED.content_hash,
                       embedder = EXCLUDED.embedder, embedding = EXCLUDED.embedding,
                       ordinal = EXCLUDED.ordinal, updated_at = now()""",
                (entry.entry_id, entry.kind, entry.name, entry.table_name, entry.description,
                 entry.content, entry.content_hash, embedder.model, vector_literal(vec),
                 entry.ordinal))
        gone = [i for i in existing if i not in wanted]
        if gone:
            conn.execute("DELETE FROM catalog.entries WHERE entry_id = ANY(%s)", (gone,))
    conn.commit()
    if to_embed or gone:
        # Fresh statistics, or the planner keeps sequential-scanning a table
        # it still believes is forty rows (measured: 22 ms at 10k rows).
        conn.execute("ANALYZE catalog.entries")
        conn.commit()
    return SyncResult(inserted, updated, len(gone), len(entries) - len(to_embed), embedder.model)


def search(conn, query: str, embedder: Embedder, kinds: tuple[str, ...] | None = None,
           limit: int = 10) -> list[dict]:
    """Nearest entries by cosine distance, through pgvector."""
    import psycopg

    q = vector_literal(embedder.embed([query])[0])
    kind_filter = "AND kind = ANY(%(kinds)s)" if kinds else ""
    # pgvector >= 0.8: keep walking the HNSW graph until LIMIT rows pass the
    # kind filter, instead of stopping at ef_search candidates and returning
    # fewer. Older pgvector has no such setting; the query still works.
    try:
        conn.execute("SELECT set_config('hnsw.iterative_scan', 'relaxed_order', false)")
    except psycopg.errors.UndefinedObject:
        conn.rollback()
    rows = conn.execute(
        f"""SELECT entry_id, kind, name, table_name, description,
                   1 - (embedding <=> %(q)s::vector) AS score
            FROM catalog.entries
            WHERE embedder = %(model)s {kind_filter}
            ORDER BY embedding <=> %(q)s::vector
            LIMIT %(limit)s""",
        {"q": q, "model": embedder.model, "kinds": list(kinds or ()), "limit": int(limit)},
    ).fetchall()
    return [{"entry_id": r[0], "kind": r[1], "name": r[2], "table_name": r[3],
             "description": r[4], "score": round(float(r[5]), 4)} for r in rows]


def columns_of(conn, table_name: str) -> list[dict]:
    rows = conn.execute(
        "SELECT name, description FROM catalog.entries WHERE kind = 'column' "
        "AND table_name = %s ORDER BY ordinal, name", (table_name,)).fetchall()
    return [{"name": r[0], "description": r[1]} for r in rows]
