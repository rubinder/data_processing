"""CLI: query output, filters that prune partitions, explain and partitions."""
import json

from polars_deployment.cli import main


def test_query_json(store, capsys):
    rc = main(["--data-dir", str(store.root), "query", "funnel", "--format", "json"])
    assert rc == 0
    body = json.loads(capsys.readouterr().out)
    pt3_f = next(r for r in body if r["page_type"] == 3 and r["event_type"] == "f")
    assert pt3_f["impressions_at_stage"] == 2
    assert pt3_f["pct_of_total"] == 66.67


def test_query_streaming_with_filter(store, capsys):
    rc = main([
        "--data-dir", str(store.root), "query", "page-type-summary",
        "--engine", "streaming", "--format", "json", "--page_type", "2",
    ])
    assert rc == 0
    body = json.loads(capsys.readouterr().out)
    assert [r["page_type"] for r in body] == [2]
    assert body[0]["pct_reaching_e"] == 33.33


def test_query_serialises_dates(store, capsys):
    main([
        "--data-dir", str(store.root), "query", "hourly-traffic",
        "--format", "json", "--date", "2026-01-01", "--hour", "10",
    ])
    body = json.loads(capsys.readouterr().out)
    assert body[0]["event_date"] == "2026-01-01"


def test_explain_shows_pruning(store, capsys):
    rc = main([
        "--data-dir", str(store.root), "explain", "hourly-traffic",
        "--page_type", "1", "--unoptimized",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "== unoptimized ==" in out and "== optimized ==" in out
    optimized = out.split("== optimized ==", 1)[1]
    assert "page_type=1/" in optimized
    assert "other sources" not in optimized
    assert "PROJECT 6/8 COLUMNS" in optimized


def test_partitions(store, capsys):
    rc = main(["--data-dir", str(store.root), "partitions"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "page_type=1 date=2026-01-01 hour=10" in out
    assert "3 partition(s)" in out
