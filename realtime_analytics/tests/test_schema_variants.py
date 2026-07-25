"""Every schema variant must return the same answer.

This is the test that makes the tuning writeup trustworthy. A benchmark that
only measures latency can be won by a schema that returns the wrong numbers
faster, so the equality of results across stages is asserted independently of
the benchmark script.
"""
import pytest

from realtime_analytics.queries import (
    Q1_NAIVE,
    Q1_TYPED,
    Q2_NAIVE,
    Q2_TYPED,
    Q3_NAIVE,
    Q3_TYPED,
    Q4_NAIVE,
    Q4_TYPED,
)

from .conftest import STAGE_TABLES

WINDOW = {"start": "2026-06-01 00:00:00", "end": "2026-06-08 00:00:00"}
TYPED_TABLES = ["v1", "v2", "v3", "v4", "v6"]


def _normalize(rows):
    out = []
    for row in rows:
        clean = {}
        for key, value in row.items():
            if isinstance(value, str):
                try:
                    clean[key] = round(float(value), 3)
                except ValueError:
                    clean[key] = value
            elif isinstance(value, float):
                clean[key] = round(value, 3)
            else:
                clean[key] = value
        out.append(clean)
    return out


@pytest.fixture(scope="module")
def account(ch):
    return ch.query(
        f"SELECT account_id, count() AS c FROM {STAGE_TABLES['v1']} "
        f"GROUP BY account_id ORDER BY c DESC LIMIT 1"
    ).rows[0]["account_id"]


@pytest.fixture(scope="module")
def conversation(ch, account):
    return ch.query(
        f"SELECT toString(conversation_id) AS cid FROM {STAGE_TABLES['v1']} "
        f"WHERE account_id = '{account}' LIMIT 1"
    ).rows[0]["cid"]


@pytest.mark.parametrize("stage", TYPED_TABLES)
def test_tenant_dashboard_matches_naive(ch, account, stage):
    params = {"account_id": account, **WINDOW}
    expected = _normalize(
        ch.query(Q1_NAIVE.replace("{table}", STAGE_TABLES["naive"]), params).rows
    )
    actual = _normalize(
        ch.query(Q1_TYPED.replace("{table}", STAGE_TABLES[stage]), params).rows
    )
    assert expected, "fixture produced no rows in the benchmark window"
    assert actual == expected


@pytest.mark.parametrize("stage", TYPED_TABLES)
def test_platform_wide_matches_naive(ch, stage):
    expected = _normalize(
        ch.query(Q3_NAIVE.replace("{table}", STAGE_TABLES["naive"]), WINDOW).rows
    )
    actual = _normalize(
        ch.query(Q3_TYPED.replace("{table}", STAGE_TABLES[stage]), WINDOW).rows
    )
    assert expected
    assert actual == expected


@pytest.mark.parametrize("stage", TYPED_TABLES)
def test_slow_responses_match_naive(ch, account, stage):
    params = {"account_id": account, **WINDOW}
    expected = _normalize(
        ch.query(Q4_NAIVE.replace("{table}", STAGE_TABLES["naive"]), params).rows
    )
    actual = _normalize(
        ch.query(Q4_TYPED.replace("{table}", STAGE_TABLES[stage]), params).rows
    )
    assert expected
    assert actual == expected


@pytest.mark.parametrize("stage", TYPED_TABLES)
def test_conversation_lookup_matches_naive(ch, conversation, stage):
    expected = _normalize(
        ch.query(
            Q2_NAIVE.replace("{table}", STAGE_TABLES["naive"]),
            {"conversation_id": conversation},
        ).rows
    )
    actual = _normalize(
        ch.query(
            Q2_TYPED.replace("{table}", STAGE_TABLES[stage]),
            {"conversation_id": conversation},
        ).rows
    )
    assert expected
    assert actual == expected


def test_bloom_filter_actually_prunes(ch, conversation):
    """The skipping index must reduce rows read, not just exist.

    v3 and v4 are identical except for the indexes, so any difference in
    rows_read is attributable to the bloom filter alone. An index that does
    not change rows_read is costing write throughput for nothing -- which is
    exactly what the benchmark found for the minmax index on latency_ms.
    """
    params = {"conversation_id": conversation}
    without = ch.query(
        Q2_TYPED.replace("{table}", STAGE_TABLES["v3"]), params
    ).rows_read
    with_index = ch.query(
        Q2_TYPED.replace("{table}", STAGE_TABLES["v4"]), params
    ).rows_read
    assert with_index < without, (
        f"bloom filter did not prune: {with_index} vs {without} rows read"
    )


def test_sorting_key_prunes_granules(ch, account):
    """The tenant+time filter must not read the whole table once sorted."""
    params = {"account_id": account, **WINDOW}
    unsorted_read = ch.query(
        Q1_TYPED.replace("{table}", STAGE_TABLES["v1"]), params
    ).rows_read
    sorted_read = ch.query(
        Q1_TYPED.replace("{table}", STAGE_TABLES["v2"]), params
    ).rows_read
    assert sorted_read < unsorted_read / 5


def test_partition_pruning_reduces_scan(ch):
    """A time-only filter should touch a fraction of the partitions."""
    unpartitioned = ch.query(
        Q3_TYPED.replace("{table}", STAGE_TABLES["v2"]), WINDOW
    ).rows_read
    partitioned = ch.query(
        Q3_TYPED.replace("{table}", STAGE_TABLES["v3"]), WINDOW
    ).rows_read
    assert partitioned < unpartitioned
