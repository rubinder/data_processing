-- init_db.sql only runs when the PostgreSQL volume is created, so existing
-- deployments pick up new columns through these idempotent migrations.
-- Applied by `./deploy.sh migrate` (and automatically by `./deploy.sh up`).
ALTER TABLE raw.impressions
    ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now();
