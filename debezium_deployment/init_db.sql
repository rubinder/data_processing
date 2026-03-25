-- Debezium CDC source database initialization
-- Creates tables that mirror the impression data theme of the project

CREATE SCHEMA IF NOT EXISTS impressions;

-- Impression events table - the main CDC source
CREATE TABLE impressions.events (
    event_id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    impression_id UUID NOT NULL,
    page_type SMALLINT NOT NULL CHECK (page_type IN (1, 2, 3)),
    event_type CHAR(1) NOT NULL CHECK (event_type IN ('a', 'b', 'c', 'd', 'e', 'f')),
    event_date DATE NOT NULL,
    event_hour SMALLINT NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    event_minute SMALLINT NOT NULL CHECK (event_minute BETWEEN 0 AND 59),
    event_second SMALLINT NOT NULL CHECK (event_second BETWEEN 0 AND 59),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- User dimension table - tracks user metadata
CREATE TABLE impressions.users (
    user_id UUID PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    signup_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pull status table - tracks data pull status (mirrors spark app behavior)
CREATE TABLE impressions.pull_status (
    pull_id BIGSERIAL PRIMARY KEY,
    page_type SMALLINT NOT NULL,
    pull_date DATE NOT NULL,
    pull_hour SMALLINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    row_count INTEGER,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (page_type, pull_date, pull_hour)
);

-- Create indexes for common query patterns
CREATE INDEX idx_events_user_id ON impressions.events (user_id);
CREATE INDEX idx_events_impression_id ON impressions.events (impression_id);
CREATE INDEX idx_events_date_hour ON impressions.events (event_date, event_hour);
CREATE INDEX idx_events_page_type ON impressions.events (page_type);

-- Grant replication permissions for Debezium
ALTER TABLE impressions.events REPLICA IDENTITY FULL;
ALTER TABLE impressions.users REPLICA IDENTITY FULL;
ALTER TABLE impressions.pull_status REPLICA IDENTITY FULL;

-- Seed some initial user data
INSERT INTO impressions.users (user_id, username, region, signup_date) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'alice', 'us-east', '2025-01-15'),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'bob', 'us-west', '2025-02-20'),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', 'carol', 'eu-west', '2025-03-10'),
    ('d4e5f6a7-b8c9-0123-defa-234567890123', 'dave', 'ap-south', '2025-04-05'),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', 'eve', 'us-east', '2025-05-12');

-- Seed some initial impression events
INSERT INTO impressions.events (user_id, impression_id, page_type, event_type, event_date, event_hour, event_minute, event_second) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', '11111111-1111-1111-1111-111111111111', 1, 'a', '2025-06-01', 10, 30, 15),
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', '11111111-1111-1111-1111-111111111111', 1, 'b', '2025-06-01', 10, 30, 18),
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', '11111111-1111-1111-1111-111111111111', 1, 'c', '2025-06-01', 10, 30, 22),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', '22222222-2222-2222-2222-222222222222', 2, 'a', '2025-06-01', 11, 15, 5),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', '22222222-2222-2222-2222-222222222222', 2, 'b', '2025-06-01', 11, 15, 8),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', '33333333-3333-3333-3333-333333333333', 3, 'a', '2025-06-01', 14, 0, 30),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', '33333333-3333-3333-3333-333333333333', 3, 'b', '2025-06-01', 14, 0, 33),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', '33333333-3333-3333-3333-333333333333', 3, 'c', '2025-06-01', 14, 0, 36),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', '33333333-3333-3333-3333-333333333333', 3, 'd', '2025-06-01', 14, 0, 40),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', '33333333-3333-3333-3333-333333333333', 3, 'e', '2025-06-01', 14, 0, 45);
