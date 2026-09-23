-- Blue-green promotion into `gold`, with a release log and a real rollback.
--
-- Readers always query `gold.<table>`. A promotion builds the next release in
-- its own schema, grants the reader role on it, then swaps schema names in
-- one transaction: the previous release is renamed away, the candidate is
-- renamed to `gold`. Nothing is copied at swap time and no reader ever sees
-- a half-promoted gold. Rollback is the same rename in reverse.
--
-- Both functions are SECURITY DEFINER and owned by the superuser that applies
-- this file. The promotion role holds EXECUTE on them and nothing else: it
-- cannot read a data table, cannot write one, cannot promote a schema it
-- names arbitrarily (the source is validated), and cannot skip the log.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_ops;

CREATE TABLE IF NOT EXISTS gold_ops.releases (
    release_id      serial PRIMARY KEY,
    promoted_at     timestamptz NOT NULL DEFAULT now(),
    promoted_by     text NOT NULL DEFAULT session_user,
    source_schema   text NOT NULL,
    tables          text[] NOT NULL DEFAULT '{}',
    row_counts      jsonb NOT NULL DEFAULT '{}'::jsonb,
    note            text,
    status          text NOT NULL CHECK (status IN ('active', 'superseded', 'rolled_back')),
    status_changed  timestamptz NOT NULL DEFAULT now(),
    schema_retained boolean NOT NULL DEFAULT true
);

CREATE OR REPLACE FUNCTION gold_ops.release_schema(rid integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT format('gold_release_%s', rid) $$;

-- Promote every base table in `source_schema` (except dbt's stg_/int_
-- intermediates) into a new gold release. Returns the release id.
CREATE OR REPLACE FUNCTION gold_ops.promote(source_schema text, note text DEFAULT NULL)
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    rid       integer;
    prev_id   integer;
    staging   text;
    tbl       record;
    n         bigint;
    tbls      text[] := '{}';
    counts    jsonb  := '{}'::jsonb;
BEGIN
    IF source_schema !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid source schema name: %', source_schema;
    END IF;
    IF source_schema IN ('gold', 'gold_ops', 'catalog', 'raw', 'pg_catalog', 'information_schema')
       OR source_schema LIKE 'gold\_release\_%' THEN
        RAISE EXCEPTION 'schema % is not a promotable candidate', source_schema;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = source_schema) THEN
        RAISE EXCEPTION 'source schema % does not exist', source_schema;
    END IF;

    INSERT INTO gold_ops.releases (source_schema, note, status)
    VALUES (source_schema, note, 'active') RETURNING release_id INTO rid;
    staging := gold_ops.release_schema(rid);
    EXECUTE format('CREATE SCHEMA %I', staging);

    FOR tbl IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = source_schema AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'stg\_%' AND table_name NOT LIKE 'int\_%'
        ORDER BY table_name
    LOOP
        EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I',
                       staging, tbl.table_name, source_schema, tbl.table_name);
        EXECUTE format('SELECT count(*) FROM %I.%I', staging, tbl.table_name) INTO n;
        tbls := tbls || tbl.table_name;
        counts := counts || jsonb_build_object(tbl.table_name, n);
    END LOOP;
    IF array_length(tbls, 1) IS NULL THEN
        RAISE EXCEPTION 'nothing to promote: % has no base tables', source_schema;
    END IF;

    -- The reader's grants live on the tables and the schema object, so they
    -- survive the rename below.
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO mcp_reader', staging);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO mcp_reader', staging);

    SELECT release_id INTO prev_id FROM gold_ops.releases
    WHERE status = 'active' AND release_id <> rid;
    IF prev_id IS NULL THEN
        -- First release: gold is the empty schema 002 created. DROP without
        -- CASCADE, so anything someone put there by hand fails the promotion
        -- instead of vanishing.
        EXECUTE 'DROP SCHEMA gold';
    ELSE
        EXECUTE format('ALTER SCHEMA gold RENAME TO %I', gold_ops.release_schema(prev_id));
        UPDATE gold_ops.releases SET status = 'superseded', status_changed = now()
        WHERE release_id = prev_id;
    END IF;
    EXECUTE format('ALTER SCHEMA %I RENAME TO gold', staging);
    UPDATE gold_ops.releases SET tables = tbls, row_counts = counts WHERE release_id = rid;

    -- Retention: keep the three newest inactive releases for rollback; drop
    -- the schemas of older ones and say so in the log.
    FOR tbl IN
        SELECT release_id FROM gold_ops.releases
        WHERE status <> 'active' AND schema_retained
        ORDER BY release_id DESC OFFSET 3
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', gold_ops.release_schema(tbl.release_id));
        UPDATE gold_ops.releases SET schema_retained = false WHERE release_id = tbl.release_id;
    END LOOP;
    RETURN rid;
END $$;

-- Swap the active release out for the most recent superseded one whose
-- schema is still retained. Returns the release id now active.
CREATE OR REPLACE FUNCTION gold_ops.rollback()
RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    cur_id  integer;
    prev_id integer;
BEGIN
    SELECT release_id INTO cur_id FROM gold_ops.releases WHERE status = 'active';
    IF cur_id IS NULL THEN
        RAISE EXCEPTION 'no active release to roll back';
    END IF;
    SELECT release_id INTO prev_id FROM gold_ops.releases
    WHERE status = 'superseded' AND schema_retained
    ORDER BY release_id DESC LIMIT 1;
    IF prev_id IS NULL THEN
        RAISE EXCEPTION 'no retained previous release to roll back to';
    END IF;
    EXECUTE format('ALTER SCHEMA gold RENAME TO %I', gold_ops.release_schema(cur_id));
    EXECUTE format('ALTER SCHEMA %I RENAME TO gold', gold_ops.release_schema(prev_id));
    UPDATE gold_ops.releases SET status = 'rolled_back', status_changed = now()
    WHERE release_id = cur_id;
    UPDATE gold_ops.releases SET status = 'active', status_changed = now()
    WHERE release_id = prev_id;
    RETURN prev_id;
END $$;

REVOKE ALL ON FUNCTION gold_ops.promote(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION gold_ops.rollback() FROM PUBLIC;
GRANT USAGE ON SCHEMA gold_ops TO promotion, mcp_reader;
GRANT EXECUTE ON FUNCTION gold_ops.promote(text, text) TO promotion;
GRANT EXECUTE ON FUNCTION gold_ops.rollback() TO promotion;
GRANT SELECT ON gold_ops.releases TO promotion, mcp_reader;
GRANT USAGE ON SCHEMA gold TO mcp_reader;
