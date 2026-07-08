"""Pure SQL builders for the CDC impressions job.

Kept free of any ``pyflink`` import so the DDL/query construction can be unit
tested without a Flink runtime installed.
"""


def source_ddl(topic: str, bootstrap: str, group: str) -> str:
    """Kafka source DDL for the unwrapped Debezium events topic.

    ``event_time`` is derived from the Debezium source timestamp and carries a
    bounded-out-of-orderness watermark so windowing is event-time based.
    """
    return f"""
        CREATE TABLE cdc_events (
            event_id BIGINT,
            user_id STRING,
            impression_id STRING,
            page_type INT,
            event_type STRING,
            event_date STRING,
            event_hour INT,
            __op STRING,
            __source_ts_ms BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(__source_ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = '{group}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """


# Tumbling 1-minute count of non-delete events per page_type. Deletes
# (Debezium __op = 'd') are excluded from the counts.
AGG_QUERY = """
    SELECT
        page_type,
        window_start,
        window_end,
        COUNT(*) AS event_count,
        COUNT(DISTINCT impression_id) AS distinct_impressions
    FROM TABLE(
        TUMBLE(TABLE cdc_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    WHERE __op <> 'd'
    GROUP BY page_type, window_start, window_end
"""
