-- Least-privilege roles, one per pipeline stage. Idempotent.
--
--   ingestion   writes the raw layer and nothing else
--   transform   reads raw, owns the dbt schemas, maintains the catalog
--   promotion   can call gold_ops.promote() / rollback() and read the log; cannot
--               read or write any data table directly
--   mcp_reader  what the agent connects as: SELECT on gold and the catalog only
--
-- Passwords here are development defaults; deploy.sh rotates them from the
-- environment (INGESTION_PASSWORD, TRANSFORM_PASSWORD, PROMOTION_PASSWORD,
-- MCP_READER_PASSWORD) after this file is applied. Go-live means secrets
-- management, network policy and TLS on top of this, not instead of it.

DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['ingestion', 'transform', 'promotion', 'mcp_reader'] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', role_name, role_name);
        END IF;
    END LOOP;
    EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ingestion, transform, promotion, mcp_reader',
                   current_database());
    -- transform creates the dbt schemas (public_staging, public_analytics)
    EXECUTE format('GRANT CREATE ON DATABASE %I TO transform', current_database());
END $$;

-- Nobody gets to dump tables into public by default.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- ingestion: the raw layer only.
CREATE SCHEMA IF NOT EXISTS raw;
GRANT USAGE ON SCHEMA raw TO ingestion;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO ingestion;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ingestion;

-- transform: read raw, own the dbt schemas.
GRANT USAGE ON SCHEMA raw TO transform;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO transform;
CREATE SCHEMA IF NOT EXISTS public_staging;
CREATE SCHEMA IF NOT EXISTS public_analytics;
GRANT ALL ON SCHEMA public_staging TO transform;
GRANT ALL ON SCHEMA public_analytics TO transform;
GRANT ALL ON ALL TABLES IN SCHEMA public_staging TO transform;
GRANT ALL ON ALL TABLES IN SCHEMA public_analytics TO transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_analytics GRANT ALL ON TABLES TO transform;
ALTER DEFAULT PRIVILEGES IN SCHEMA public_staging GRANT ALL ON TABLES TO transform;
-- promote() reads the candidate schema as its definer, so transform's own
-- grants are enough; nothing else needs SELECT on public_analytics.
