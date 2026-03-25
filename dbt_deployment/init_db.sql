CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.impressions (
    user_id VARCHAR(36),
    impression_id VARCHAR(36),
    page_type INTEGER,
    date VARCHAR(10),
    hour INTEGER,
    min INTEGER,
    second INTEGER,
    event_type VARCHAR(1)
);
