"""Kafka producer for AI agent conversation events.

Emits whole conversations rather than isolated events, because the downstream
consumers care about conversation-level invariants (one terminal event per
conversation, ordering of turns).

Two production-shaped decisions are baked in:

**Partition key = conversation_id.** Kafka guarantees order only within a
partition, so keying by conversation puts every event of one conversation in
one partition, in order. Flink's keyed state and any session-window logic then
see a coherent per-conversation stream. Keying by account_id instead would
have produced hot partitions -- the largest tenant would pin one broker.

**Deliberate late events.** ``--late-fraction`` re-stamps a slice of events
with an older ``event_ts``, simulating a mobile client that buffered while
offline or a voice transcript that finalized late. This is what the Flink
watermark strategy is tuned against; without it, out-of-orderness handling is
untested code.

Usage::

    python -m realtime_analytics.producer --rate 5000 --duration 60
    python -m realtime_analytics.producer --total 1000000 --late-fraction 0.02
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta

from realtime_analytics.events import generate_conversation

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DEFAULT_TOPIC = os.getenv("EVENTS_TOPIC", "conversation.events")


def build_producer(bootstrap: str):
    """Create a KafkaProducer tuned for throughput over per-message latency.

    ``linger_ms`` + ``batch_size`` trade a few milliseconds of delay for much
    larger batches, which is the right trade for an analytics pipeline whose
    SLO is on query latency, not on individual event delivery.
    ``acks='all'`` keeps durability: an event that a broker loses is an event
    the dashboard silently under-counts.
    """
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="lz4",
        retries=5,
    )


def run(args) -> None:
    producer = build_producer(args.bootstrap)
    rng = random.Random(args.seed)
    account_ids = [f"acct_{i:04d}" for i in range(args.accounts)]
    weights = [1.0 / (i + 1) for i in range(args.accounts)]

    sent = 0
    started = time.time()
    deadline = started + args.duration if args.duration else None
    interval = 1.0 / args.rate if args.rate else 0.0
    next_send = started

    try:
        while True:
            if args.total and sent >= args.total:
                break
            if deadline and time.time() >= deadline:
                break

            account_id = rng.choices(account_ids, weights=weights, k=1)[0]
            start_ts = datetime.now()
            if rng.random() < args.late_fraction:
                # Buffered client: the event happened minutes ago.
                start_ts -= timedelta(seconds=rng.randint(30, args.max_lateness))

            for event in generate_conversation(rng, account_id, start_ts):
                producer.send(
                    args.topic,
                    key=event.conversation_id,
                    value=event.to_dict(),
                )
                sent += 1
                if args.rate:
                    next_send += interval
                    delay = next_send - time.time()
                    if delay > 0:
                        time.sleep(delay)

            if sent % 10000 < 8:
                elapsed = time.time() - started
                print(
                    f"sent={sent:,} elapsed={elapsed:.1f}s "
                    f"rate={sent / max(elapsed, 1e-9):,.0f}/s",
                    flush=True,
                )
    except KeyboardInterrupt:
        print("\nInterrupted", flush=True)
    finally:
        producer.flush()
        producer.close()

    elapsed = time.time() - started
    print(
        f"Done: {sent:,} events in {elapsed:.1f}s "
        f"({sent / max(elapsed, 1e-9):,.0f} events/s)",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--rate", type=int, default=0,
                        help="target events/sec (0 = as fast as possible)")
    parser.add_argument("--duration", type=int, default=0,
                        help="seconds to run (0 = until --total)")
    parser.add_argument("--total", type=int, default=100_000,
                        help="total events to send (0 = unbounded)")
    parser.add_argument("--accounts", type=int, default=25)
    parser.add_argument("--late-fraction", type=float, default=0.02,
                        help="fraction of conversations stamped in the past")
    parser.add_argument("--max-lateness", type=int, default=600,
                        help="max seconds an event can be late")
    parser.add_argument("--seed", type=int, default=7)
    run(parser.parse_args())


if __name__ == "__main__":
    main()
