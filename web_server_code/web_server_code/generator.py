"""Impression data generator.

Generates synthetic impression event data as gzip-compressed CSV.
Each impression follows a funnel of event types a -> b -> c -> d -> e -> f
where reaching a later event implies all earlier events occurred.
"""

import csv
import gzip
import io
import random
import uuid

EVENT_TYPES = ["a", "b", "c", "d", "e", "f"]

# Cumulative probability of reaching at least each event type.
# For each page_type, index i gives the probability an impression
# reaches event_type EVENT_TYPES[i].
FUNNEL_RATES: dict[int, list[float]] = {
    1: [1.0, 0.80, 0.40, 0.10, 0.0, 0.0],
    2: [1.0, 0.85, 0.55, 0.30, 0.10, 0.0],
    3: [1.0, 0.90, 0.70, 0.50, 0.20, 0.10],
}

HEADER = [
    "user_id", "impression_id", "page_type",
    "date", "hour", "min", "second", "event_type",
]

NUM_IMPRESSIONS = 20_000
NUM_USERS = 1_200


def _determine_max_event(funnel: list[float]) -> int:
    """Determine the deepest event reached using a single random roll."""
    roll = random.random()
    max_idx = 0
    for i in range(len(funnel)):
        if roll < funnel[i]:
            max_idx = i
    return max_idx


def _make_impression_id(
    user_id: str, page_type: int, date: str, hour: int, minute: int
) -> str:
    """Generate deterministic impression_id for a given combination."""
    name = f"{user_id}:{page_type}:{date}:{hour}:{minute}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))


def generate_rows(
    page_type: int,
    date: str,
    hour: int,
    num_impressions: int = NUM_IMPRESSIONS,
    num_users: int = NUM_USERS,
) -> list[list]:
    """Generate impression event rows.

    Returns a list of rows (each a list of field values) matching HEADER.
    """
    funnel = FUNNEL_RATES[page_type]
    users = [str(uuid.uuid4()) for _ in range(num_users)]
    rows: list[list] = []
    seen_impressions: set[str] = set()

    generated = 0
    while generated < num_impressions:
        user_id = random.choice(users)
        minute = random.randint(0, 59)

        impression_id = _make_impression_id(
            user_id, page_type, date, hour, minute
        )

        # Skip duplicate impression_ids to keep events consistent
        if impression_id in seen_impressions:
            continue
        seen_impressions.add(impression_id)
        generated += 1

        max_event_idx = _determine_max_event(funnel)

        # Events happen at successive seconds within the minute
        base_second = random.randint(0, 59 - max_event_idx)
        for event_idx in range(max_event_idx + 1):
            second = base_second + event_idx
            rows.append([
                user_id,
                impression_id,
                page_type,
                date,
                hour,
                minute,
                second,
                EVENT_TYPES[event_idx],
            ])

    return rows


def generate_gzip_csv(
    page_type: int,
    date: str,
    hour: int,
    num_impressions: int = NUM_IMPRESSIONS,
    num_users: int = NUM_USERS,
) -> bytes:
    """Generate impression data and return as gzip-compressed CSV bytes."""
    rows = generate_rows(
        page_type, date, hour, num_impressions, num_users
    )

    text_buf = io.StringIO()
    writer = csv.writer(text_buf)
    writer.writerow(HEADER)
    writer.writerows(rows)

    csv_bytes = text_buf.getvalue().encode("utf-8")
    return gzip.compress(csv_bytes)
