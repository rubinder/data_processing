"""FastAPI endpoints, exercised against real embedded ClickHouse.

The TestClient talks to the same app object uvicorn serves, and the app talks
to a real ClickHouse engine holding real generated events, so these tests
cover the SQL, the parameter binding, and the response shape together.
"""
import pytest


def test_health(api_client):
    response = api_client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["clickhouse"] == "reachable"


def test_summary_returns_daily_rows(api_client, busiest_account):
    response = api_client.get(f"/v1/accounts/{busiest_account}/summary?days=7")
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "materialized_view:conversation_daily"
    assert body["results"], "expected at least one day of data"
    row = body["results"][0]
    for field in ("day", "conversations", "resolutions", "escalations",
                  "resolution_rate", "escalation_rate", "p50_latency_ms",
                  "p95_latency_ms", "p99_latency_ms"):
        assert field in row


def test_summary_rates_are_consistent(api_client, busiest_account):
    """resolution_rate + escalation_rate should account for closed conversations.

    Every generated conversation ends in exactly one resolution or escalation,
    so the two rates must sum to ~1 on any day with traffic. This catches an
    aggregate wired to the wrong counter, which a smoke test would not.
    """
    body = api_client.get(
        f"/v1/accounts/{busiest_account}/summary?days=7"
    ).json()
    checked = 0
    for row in body["results"]:
        if not row["conversations"]:
            continue
        total = float(row["resolution_rate"]) + float(row["escalation_rate"])
        assert 0.9 <= total <= 1.1, f"rates sum to {total} on {row['day']}"
        checked += 1
    assert checked, "no day had conversations"


def test_percentiles_are_ordered(api_client, busiest_account):
    body = api_client.get(
        f"/v1/accounts/{busiest_account}/agent-latency?hours=96"
    ).json()
    assert body["results"]
    for row in body["results"]:
        assert row["p50_latency_ms"] <= row["p95_latency_ms"] <= row["p99_latency_ms"]
        assert 0 <= float(row["slow_rate"]) <= 1


def test_hourly_endpoint(api_client, busiest_account):
    body = api_client.get(
        f"/v1/accounts/{busiest_account}/hourly?hours=96"
    ).json()
    assert body["results"]
    hours = [row["hour"] for row in body["results"]]
    assert hours == sorted(hours), "hourly series must be time-ordered"


def test_intents_endpoint_reads_raw_table(api_client, busiest_account):
    body = api_client.get(f"/v1/accounts/{busiest_account}/intents?days=7").json()
    assert body["source"].startswith("raw:")
    assert body["results"]
    counts = [int(row["events"]) for row in body["results"]]
    assert counts == sorted(counts, reverse=True)


def test_conversation_detail(api_client, ch, busiest_account):
    conversation_id = ch.query(
        "SELECT toString(conversation_id) AS cid FROM conversation_events "
        "WHERE account_id = {account_id:String} LIMIT 1",
        {"account_id": busiest_account},
    ).rows[0]["cid"]

    response = api_client.get(f"/v1/conversations/{conversation_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["events"]
    assert body["events"][0]["event_type"] == "conversation_started"
    timestamps = [event["event_ts"] for event in body["events"]]
    assert timestamps == sorted(timestamps)


def test_unknown_conversation_is_404(api_client):
    response = api_client.get(
        "/v1/conversations/00000000-0000-0000-0000-000000000000"
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/v1/accounts/bad%20account/summary",
        "/v1/conversations/not-a-uuid",
    ],
)
def test_malformed_identifiers_are_rejected(api_client, path):
    """Input is validated at the edge, before it reaches the query layer."""
    assert api_client.get(path).status_code == 400


def test_lookback_window_is_bounded(api_client, busiest_account):
    """An unbounded range parameter is how a fast API becomes a slow one."""
    response = api_client.get(
        f"/v1/accounts/{busiest_account}/summary?days=100000"
    )
    assert response.status_code == 422


def test_sql_injection_attempt_is_rejected(api_client):
    response = api_client.get(
        "/v1/accounts/acct_0000';DROP TABLE conversation_events;--/summary"
    )
    assert response.status_code in (400, 404)


def test_metrics_endpoint_reports_latency(api_client, busiest_account):
    api_client.get(f"/v1/accounts/{busiest_account}/summary?days=7")
    response = api_client.get("/metrics")
    assert response.status_code == 200
    assert "api_request_latency_ms" in response.text
    assert "api_requests_total" in response.text


def test_responses_carry_timing_header(api_client, busiest_account):
    response = api_client.get(f"/v1/accounts/{busiest_account}/summary?days=7")
    assert "X-Response-Time-Ms" in response.headers
    assert float(response.headers["X-Response-Time-Ms"]) >= 0
