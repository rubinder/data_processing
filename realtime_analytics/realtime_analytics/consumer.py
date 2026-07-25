"""Kafka -> ClickHouse consumer: the simple, boring, correct ingest path.

This is the baseline the Flink job is compared against. It is deliberately
plain -- no framework, no cluster -- and for a large class of workloads it is
the right answer. The README explains when it stops being the right answer.

The three things that actually matter in a ClickHouse ingest loop, and how
each is handled here:

**Batch size.** Every INSERT creates a part, and every part must later be
merged. A consumer that inserts one row per message will bury the server in
merge work and hit ``too many parts`` long before it hits any CPU limit.
Rows are therefore buffered and flushed in large batches -- ClickHouse's own
guidance is roughly 10k-100k rows or one insert per second per table, and
``--batch-size`` defaults into that band.

**Flush deadline.** Batching alone would make freshness unbounded on a quiet
topic, so the buffer also flushes on a timer. Batch size bounds merge
pressure; the timer bounds end-to-end staleness. Those are the two halves of
the ingest SLO.

**Offset commit ordering.** Offsets are committed *after* the insert
succeeds, never before. That makes delivery at-least-once: a crash between
insert and commit replays the batch and duplicates rows. The alternative --
commit first -- would be at-most-once and would silently lose events, which
for billing-adjacent analytics is far worse. Duplicates are handled at the
storage layer; see the ReplacingMergeTree note in the README.

Backpressure is implicit and this is a feature: if ClickHouse slows down, the
insert blocks, the loop stops polling, and consumer lag grows. Lag is the
signal to alert on, and it is visible in Kafka without instrumenting anything.

Usage::

    python -m realtime_analytics.consumer --batch-size 50000
"""
import argparse
import json
import os
import signal
import time
from datetime import datetime

from realtime_analytics.events import COLUMNS

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.getenv("EVENTS_TOPIC", "conversation.events")
DEFAULT_GROUP = os.getenv("CONSUMER_GROUP", "clickhouse-sink")
DEFAULT_TABLE = os.getenv("EVENTS_TABLE", "conversation_events")

#: Columns whose ClickHouse type needs a conversion from the JSON wire form.
_TIMESTAMP_COLUMNS = ("event_ts", "ingest_ts")


class Stats:
    """Minimal counters. Exported for the /metrics endpoint in api.py."""

    def __init__(self):
        self.messages = 0
        self.batches = 0
        self.rows_inserted = 0
        self.insert_errors = 0
        self.parse_errors = 0
        self.insert_seconds = 0.0
        self.started = time.time()

    def snapshot(self) -> dict:
        elapsed = max(time.time() - self.started, 1e-9)
        return {
            "messages": self.messages,
            "batches": self.batches,
            "rows_inserted": self.rows_inserted,
            "insert_errors": self.insert_errors,
            "parse_errors": self.parse_errors,
            "avg_insert_ms": round(
                1000 * self.insert_seconds / max(self.batches, 1), 2
            ),
            "events_per_second": round(self.rows_inserted / elapsed, 1),
        }


def parse_event(raw: dict) -> list:
    """Convert one decoded Kafka message into a ClickHouse row.

    Timestamps arrive as strings and are converted here rather than being left
    for ClickHouse to coerce, so a malformed timestamp fails loudly on one row
    instead of silently becoming 1970 in a dashboard.
    """
    row = []
    for column in COLUMNS:
        value = raw.get(column)
        if column in _TIMESTAMP_COLUMNS:
            value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        row.append(value)
    return row


def flush(client, table: str, buffer: list, stats: Stats) -> bool:
    """Insert a batch. Returns True when the offsets may be committed."""
    if not buffer:
        return True
    started = time.perf_counter()
    try:
        client.insert(table, buffer, column_names=COLUMNS)
    except Exception as exc:  # noqa: BLE001 - surfaced, then retried by Kafka
        stats.insert_errors += 1
        print(f"insert failed ({len(buffer)} rows): {exc}", flush=True)
        return False
    stats.insert_seconds += time.perf_counter() - started
    stats.batches += 1
    stats.rows_inserted += len(buffer)
    return True


def run(args) -> None:
    import clickhouse_connect
    from kafka import KafkaConsumer

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        # Offsets are committed by hand, after the insert lands.
        enable_auto_commit=False,
        max_poll_records=args.batch_size,
        consumer_timeout_ms=1000,
    )

    stats = Stats()
    buffer: list = []
    last_flush = time.time()
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(
        f"Consuming {args.topic} -> {args.table} "
        f"(batch={args.batch_size}, flush={args.flush_interval}s)",
        flush=True,
    )

    while running:
        for message in consumer:
            try:
                buffer.append(parse_event(message.value))
            except Exception as exc:  # noqa: BLE001
                # One bad record must not stall the partition. In production
                # this goes to a dead-letter topic rather than to stdout.
                stats.parse_errors += 1
                if stats.parse_errors <= 5:
                    print(f"skipping malformed event: {exc}", flush=True)
            stats.messages += 1
            if len(buffer) >= args.batch_size:
                break
            if not running:
                break

        deadline_passed = time.time() - last_flush >= args.flush_interval
        if buffer and (len(buffer) >= args.batch_size or deadline_passed):
            if flush(client, args.table, buffer, stats):
                consumer.commit()
                buffer.clear()
                last_flush = time.time()
            else:
                # Do not commit; the batch replays after a short backoff.
                time.sleep(args.retry_backoff)
        elif not buffer:
            last_flush = time.time()

        if stats.batches and stats.batches % 20 == 0:
            print(json.dumps(stats.snapshot()), flush=True)

    if buffer and flush(client, args.table, buffer, stats):
        consumer.commit()
    consumer.close()
    client.close()
    print(json.dumps(stats.snapshot()), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group", default=DEFAULT_GROUP)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--batch-size", type=int, default=50_000,
                        help="rows per INSERT (bounds merge pressure)")
    parser.add_argument("--flush-interval", type=float, default=2.0,
                        help="seconds before a partial batch is flushed "
                             "(bounds end-to-end staleness)")
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
