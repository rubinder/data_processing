"""Tests for lambda/athena_lineage.py (no AWS calls)."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "lambda"))

import athena_lineage as al  # noqa: E402


def test_partition_from_key():
    part = al.partition_from_key(
        "raw/impressions/page_type=1/date=2026-09-06/hour=10/data.csv.gz"
    )
    assert part == {"page_type": "1", "date": "2026-09-06", "hour": "10"}
    with pytest.raises(ValueError, match="no page_type/date/hour"):
        al.partition_from_key("raw/other/file.csv")


def test_count_query_filters_on_partition_columns_only():
    q = al.count_query("data-processing_db", "processed",
                       {"page_type": "1", "date": "2026-09-06", "hour": "10"})
    assert q.startswith('SELECT count(*) AS rows FROM "data-processing_db"."processed"')
    assert "page_type = '1'" in q and "date = '2026-09-06'" in q
    assert "hour = '10'" in q          # partition keys are strings in the catalog


def test_run_event_shape_and_parent_facet():
    ev = al.run_event(
        "COMPLETE", "run-1", "athena.processed_partition_count", "ns",
        inputs=[al.athena_dataset("us-east-1", "db", "processed")],
        outputs=[al.s3_dataset("s3://bucket/athena-results/q1.csv")],
        run_facets={"x": {"_producer": "p", "_schemaURL": "s"}},
        parent={"runId": "parent-run", "jobName": "step_function.pipeline"},
    )
    assert ev["eventType"] == "COMPLETE"
    assert ev["job"] == {"namespace": "ns", "name": "athena.processed_partition_count"}
    assert ev["inputs"] == [{"namespace": "awsathena://athena.us-east-1.amazonaws.com",
                             "name": "db.processed"}]
    assert ev["outputs"] == [{"namespace": "s3://bucket", "name": "/athena-results/q1.csv"}]
    assert ev["run"]["facets"]["parent"]["run"] == {"runId": "parent-run"}
    assert ev["run"]["facets"]["x"]["_producer"] == "p"
    json.dumps(ev)  # serialisable


def test_emit_without_url_logs_instead_of_posting(capsys):
    outcome = al.emit(None, {"eventType": "START"})
    assert outcome == "logged"
    logged = json.loads(capsys.readouterr().out.strip())
    assert logged["event"]["eventType"] == "START"


def test_emit_swallows_transport_errors(capsys):
    # Nothing listens on this port; lineage must never fail the pipeline.
    outcome = al.emit("http://127.0.0.1:9", {"eventType": "START"}, timeout=0.5)
    assert outcome == "failed"
    assert "post failed" in capsys.readouterr().out


class _FakeAthena:
    def __init__(self, states, rows=("rows", "68367")):
        self.states = list(states)
        self.rows = rows
        self.calls = []

    def start_query_execution(self, **kw):
        self.calls.append(("start", kw))
        return {"QueryExecutionId": "q-1"}

    def get_query_execution(self, QueryExecutionId):
        state = self.states.pop(0)
        return {"QueryExecution": {
            "QueryExecutionId": QueryExecutionId,
            "Status": {"State": state},
            "Statistics": {"DataScannedInBytes": 0, "EngineExecutionTimeInMillis": 500},
            "ResultConfiguration": {"OutputLocation": "s3://out/athena-results/q-1.csv"},
        }}

    def get_query_results(self, QueryExecutionId):
        return {"ResultSet": {"Rows": [
            {"Data": [{"VarCharValue": self.rows[0]}]},
            {"Data": [{"VarCharValue": self.rows[1]}]},
        ]}}


def test_run_athena_polls_to_terminal_state():
    fake = _FakeAthena(["QUEUED", "RUNNING", "SUCCEEDED"])
    execution = al.run_athena(fake, "SELECT 1", "db", "wg", poll_seconds=0)
    assert execution["Status"]["State"] == "SUCCEEDED"
    assert fake.calls[0][1]["WorkGroup"] == "wg"
    assert al.first_cell(fake, "q-1") == 68367


def test_lambda_handler_end_to_end_with_fake_athena(monkeypatch, capsys):
    fake = _FakeAthena(["SUCCEEDED"])
    monkeypatch.setattr(al.boto3, "client", lambda *a, **k: fake)
    monkeypatch.setenv("GLUE_DATABASE", "data-processing_db")
    monkeypatch.setenv("ATHENA_WORKGROUP", "data-processing-dev")
    monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

    result = al.lambda_handler(
        {"key": "raw/impressions/page_type=1/date=2026-09-06/hour=10/data.csv.gz",
         "execution_name": "exec-1"}, None,
    )
    assert result["rows"] == 68367
    assert result["partition"]["hour"] == "10"
    assert result["lineage"] == "logged"
    out = capsys.readouterr().out
    events = [json.loads(l)["event"]["eventType"] for l in out.splitlines()
              if '"lineage": "not posted' in l]
    assert events == ["START", "COMPLETE"]
