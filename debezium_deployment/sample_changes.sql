-- Sample DML to generate CDC events after the connector is registered.
-- Run with: ./deploy.sh exec-sql sample_changes.sql

-- Insert new users
INSERT INTO impressions.users (user_id, username, region, signup_date) VALUES
    ('f6a7b8c9-d0e1-2345-fab0-456789012345', 'frank', 'eu-central', '2025-06-15'),
    ('a7b8c9d0-e1f2-3456-ab01-567890123456', 'grace', 'ap-east', '2025-06-20');

-- Insert new impression events
INSERT INTO impressions.events (user_id, impression_id, page_type, event_type, event_date, event_hour, event_minute, event_second) VALUES
    ('d4e5f6a7-b8c9-0123-defa-234567890123', '44444444-4444-4444-4444-444444444444', 2, 'a', '2025-06-02', 9, 45, 10),
    ('d4e5f6a7-b8c9-0123-defa-234567890123', '44444444-4444-4444-4444-444444444444', 2, 'b', '2025-06-02', 9, 45, 14),
    ('d4e5f6a7-b8c9-0123-defa-234567890123', '44444444-4444-4444-4444-444444444444', 2, 'c', '2025-06-02', 9, 45, 19),
    ('d4e5f6a7-b8c9-0123-defa-234567890123', '44444444-4444-4444-4444-444444444444', 2, 'd', '2025-06-02', 9, 45, 25),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'a', '2025-06-02', 16, 20, 0),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'b', '2025-06-02', 16, 20, 3),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'c', '2025-06-02', 16, 20, 7),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'd', '2025-06-02', 16, 20, 12),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'e', '2025-06-02', 16, 20, 18),
    ('e5f6a7b8-c9d0-1234-efab-345678901234', '55555555-5555-5555-5555-555555555555', 3, 'f', '2025-06-02', 16, 20, 25);

-- Update a user (triggers CDC update event)
UPDATE impressions.users
SET region = 'us-central', updated_at = NOW()
WHERE username = 'bob';

-- Update pull status (triggers CDC update event)
INSERT INTO impressions.pull_status (page_type, pull_date, pull_hour, status, row_count, started_at, completed_at)
VALUES (2, '2025-06-02', 9, 'completed', 1500, NOW() - INTERVAL '5 minutes', NOW());

-- Delete a user (triggers CDC delete event)
DELETE FROM impressions.users WHERE username = 'eve';
