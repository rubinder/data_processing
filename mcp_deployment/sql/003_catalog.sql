-- The semantic catalog: one row per gold table, column and query template,
-- embedded with pgvector so an agent can find what to ask for by meaning.
-- Written by the transform stage (after dbt), read by the agent.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.entries (
    entry_id      text PRIMARY KEY,
    kind          text NOT NULL CHECK (kind IN ('table', 'column', 'template')),
    name          text NOT NULL,
    table_name    text,
    description   text NOT NULL,
    content       text NOT NULL,
    content_hash  text NOT NULL,
    embedder      text NOT NULL,
    embedding     vector(384) NOT NULL,
    -- physical column position, so a table description lists columns in order
    ordinal       integer NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE catalog.entries ADD COLUMN IF NOT EXISTS ordinal integer NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS entries_embedding_hnsw
    ON catalog.entries USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS entries_kind ON catalog.entries (kind);

GRANT USAGE ON SCHEMA catalog TO mcp_reader, transform;
GRANT SELECT ON catalog.entries TO mcp_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON catalog.entries TO transform;
