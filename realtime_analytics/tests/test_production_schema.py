"""The shipped schema: views agree with the raw table, and dedup works.

These assertions run against ``clickhouse/10_production.sql`` -- the same file
``deploy.sh schema`` applies -- so a change that breaks the aggregate layer
fails here rather than in a dashboard.
"""
from .conftest import _insert_events

RAW_DAILY = """
SELECT
    toDate(event_ts)                                       AS day,
    count()                                                AS events,
    countIf(event_type = 'conversation_started')           AS conversations,
    countIf(event_type = 'resolution')                     AS resolutions,
    countIf(event_type = 'escalation')                     AS escalations
FROM conversation_events
WHERE account_id = {account_id:String}
GROUP BY day
ORDER BY day
"""

MV_DAILY = """
SELECT
    day,
    sum(events)                                            AS events,
    sum(conversations)                                     AS conversations,
    sum(resolutions)                                       AS resolutions,
    sum(escalations)                                       AS escalations
FROM conversation_daily
WHERE account_id = {account_id:String}
GROUP BY day
ORDER BY day
"""


def _as_ints(rows):
    return [
        {k: (int(float(v)) if k != "day" else v) for k, v in row.items()}
        for row in rows
    ]


def test_daily_view_matches_raw_events(ch, busiest_account):
    """The pre-aggregate must equal the scan it replaces."""
    params = {"account_id": busiest_account}
    raw = _as_ints(ch.query(RAW_DAILY, params).rows)
    view = _as_ints(ch.query(MV_DAILY, params).rows)
    assert raw, "no events for the busiest account"
    assert view == raw


def test_platform_view_matches_raw_events(ch):
    raw = _as_ints(
        ch.query(
            "SELECT toDate(event_ts) AS day, count() AS events, "
            "countIf(event_type = 'escalation') AS escalations "
            "FROM conversation_events GROUP BY day ORDER BY day"
        ).rows
    )
    view = _as_ints(
        ch.query(
            "SELECT day, sum(events) AS events, "
            "sum(escalations) AS escalations "
            "FROM platform_daily GROUP BY day ORDER BY day"
        ).rows
    )
    assert view == raw


def test_hourly_view_counts_only_agent_responses(ch, busiest_account):
    params = {"account_id": busiest_account}
    raw = ch.query(
        "SELECT count() AS c FROM conversation_events "
        "WHERE account_id = {account_id:String} "
        "AND event_type = 'agent_response'",
        params,
    ).rows[0]["c"]
    view = ch.query(
        "SELECT sum(responses) AS c FROM agent_hourly "
        "WHERE account_id = {account_id:String}",
        params,
    ).rows[0]["c"]
    assert int(float(view)) == int(float(raw))


def test_latency_percentiles_are_close_to_exact(ch, busiest_account):
    """t-digest is approximate; quantify the error rather than assume it.

    The views store t-digest sketches so percentiles can be merged. This
    asserts the merged p95 stays within 5% of an exact computation over the
    raw events -- the tolerance the benchmark also applies.
    """
    params = {"account_id": busiest_account}
    exact = float(
        ch.query(
            "SELECT quantileExact(0.95)(latency_ms) AS p95 "
            "FROM conversation_events "
            "WHERE account_id = {account_id:String} "
            "AND event_type = 'agent_response'",
            params,
        ).rows[0]["p95"]
    )
    approx = float(
        ch.query(
            "SELECT quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state)[2] "
            "AS p95 FROM conversation_daily "
            "WHERE account_id = {account_id:String}",
            params,
        ).rows[0]["p95"]
    )
    assert abs(approx - exact) / max(exact, 1.0) < 0.05, (
        f"t-digest p95 {approx} drifted from exact {exact}"
    )


def test_view_fires_on_new_inserts(ch):
    """A materialized view is an insert trigger: new rows must flow through."""
    from datetime import datetime, timedelta
    import random

    from realtime_analytics.events import generate_conversation

    account = "acct_mvtest"
    before = ch.query(
        "SELECT sum(events) AS c FROM conversation_daily "
        "WHERE account_id = {account_id:String}",
        {"account_id": account},
    ).rows
    before_count = int(float(before[0]["c"] or 0)) if before else 0

    rng = random.Random(99)
    events = generate_conversation(
        rng, account, datetime.now() - timedelta(hours=1)
    )
    _insert_events(ch, "conversation_events", events)

    after = int(
        float(
            ch.query(
                "SELECT sum(events) AS c FROM conversation_daily "
                "WHERE account_id = {account_id:String}",
                {"account_id": account},
            ).rows[0]["c"]
        )
    )
    assert after == before_count + len(events)


def test_replacing_merge_tree_dedups_replayed_rows(ch):
    """At-least-once delivery duplicates rows; the engine must collapse them.

    This is the storage-side half of the ingest contract: the consumer commits
    offsets only after a successful insert, so a crash replays a batch, and
    ReplacingMergeTree makes that replay harmless once merged.
    """
    from datetime import datetime, timedelta
    import random

    from realtime_analytics.events import generate_conversation

    account = "acct_dedup"
    rng = random.Random(123)
    events = generate_conversation(
        rng, account, datetime.now() - timedelta(hours=2)
    )

    _insert_events(ch, "conversation_events", events)
    _insert_events(ch, "conversation_events", events)  # replay
    ch.command("OPTIMIZE TABLE conversation_events FINAL")

    remaining = int(
        float(
            ch.query(
                "SELECT count() AS c FROM conversation_events "
                "WHERE account_id = {account_id:String}",
                {"account_id": account},
            ).rows[0]["c"]
        )
    )
    assert remaining == len(events)
