-- Source-side schema evolution demo for impressions.events.
-- Run with: ./deploy.sh evolve   (or: ./deploy.sh exec-sql schema_changes.sql)
--
-- Each ALTER is followed by an INSERT because Debezium (pgoutput) only learns
-- about a new table shape from the *relation message* that precedes the next
-- change event, and only registers a new Avro schema version when it emits a
-- record with that new shape. An ALTER with no DML after it produces no new
-- version. See SCHEMA_EVOLUTION.md for what happens at each layer.

-- 1. ADD COLUMN (nullable, no default).
--    Debezium: optional Connect field -> Avro ["null","string"] default null.
--    Registry (BACKWARD): accepted, adding an optional field is allowed.
--    Flink: the job's DDL does not declare `referrer`, so the reader schema
--    ignores it; nothing changes downstream.
ALTER TABLE impressions.events ADD COLUMN referrer VARCHAR(200);

INSERT INTO impressions.events
    (user_id, impression_id, page_type, event_type, event_date, event_hour,
     event_minute, event_second, referrer)
VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890',
     '66666666-6666-6666-6666-666666666666', 1, 'a', '2025-06-03', 8, 5, 10,
     'https://example.com/landing');

-- 2. RENAME COLUMN. To Avro a rename is "drop event_minute + add
--    minute_of_hour". event_minute is NOT NULL with no DEFAULT; renamed as-is
--    the new field would be a *required* Avro int with no default, which
--    BACKWARD rejects (a reader with a required field the writer never wrote
--    cannot read old records) -> the registry answers 409 and the Debezium
--    task fails. Dropping NOT NULL first makes the renamed field optional
--    (["null","int"] default null), which BACKWARD accepts.
--    Flink: the job declares event_minute INT purely to show resolution:
--    from this version on it reads as NULL instead of failing.
ALTER TABLE impressions.events ALTER COLUMN event_minute DROP NOT NULL;
ALTER TABLE impressions.events RENAME COLUMN event_minute TO minute_of_hour;

INSERT INTO impressions.events
    (user_id, impression_id, page_type, event_type, event_date, event_hour,
     minute_of_hour, event_second, referrer)
VALUES
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901',
     '77777777-7777-7777-7777-777777777777', 2, 'a', '2025-06-03', 9, 15, 20,
     'https://example.com/search');

-- 3. DROP COLUMN.
--    Registry (BACKWARD): accepted, a reader may drop fields the writer has.
--    Flink: event_second (declared in the DDL) resolves to NULL; the
--    aggregate only needs page_type / impression_id / __op / __source_ts_ms
--    and keeps producing windows.
ALTER TABLE impressions.events DROP COLUMN event_second;

INSERT INTO impressions.events
    (user_id, impression_id, page_type, event_type, event_date, event_hour,
     minute_of_hour, referrer)
VALUES
    ('c3d4e5f6-a7b8-9012-cdef-123456789012',
     '88888888-8888-8888-8888-888888888888', 3, 'a', '2025-06-03', 10, 45,
     NULL);
