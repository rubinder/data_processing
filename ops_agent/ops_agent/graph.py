"""LangGraph ops agent: sense -> monitor -> classify -> persist -> act.

Offline by default: ``run()`` accepts an engine so the tests never touch a
warehouse or the network, and ``--execute`` is the only thing that turns the
agent's side effect (filing a GitHub issue) from a description into a call.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from typing import Any, TypedDict

import pyarrow as pa
from langgraph.graph import END, StateGraph

from ops_agent import actions, arrival, classifier, contracts, feeds, runner, sensors, tables

# A severity is only worth having if it changes what the agent does.
ACTIONABLE = {"breaking", "renaming"}


class AgentState(TypedDict, total=False):
    as_of: date
    dry_run: bool
    engine: Any
    feed_dir: Any
    findings: list[sensors.Finding]
    classified: list[dict]
    actions_taken: list[str]


def watched_layers(feed_dir=None):
    """Every feed layer that declares ``agent_watch: true``."""
    return tuple((feeds.table_def_for(layer.table), layer.contract, layer.freshness_column)
                 for feed in feeds.load_feeds(feed_dir) for layer in feed.watched_layers)


def sense_and_detect(state: AgentState) -> AgentState:
    engine = state["engine"]
    findings: list[sensors.Finding] = []
    for table_def, contract_file, date_column in watched_layers(state.get("feed_dir")):
        if not engine.table_exists(table_def.name):
            continue
        contract = contracts.contract_for(contract_file)
        enum_columns = tuple(w["column"] for w in contract.enum_watch)
        observed = sensors.observe(engine, table_def, date_column=date_column,
                                   enum_columns=enum_columns)
        # Trailing history excludes the latest snapshot: comparing a batch
        # against a median that includes itself blunts the signal.
        per_snapshot = engine.snapshot_row_counts(table_def.name)
        history = per_snapshot[:-1] or per_snapshot
        findings += sensors.detect(observed, contract, state["as_of"], history)
    return {**state, "findings": findings}


def run_monitors(state: AgentState) -> AgentState:
    """Row-level monitors plus arrival SLAs, folded into the findings.

    Only ``breach`` becomes a finding; ``warn`` is signal for the alert channel
    but not grounds for an incident. Arrival SLAs are skipped for a table that
    does not exist yet: a lakehouse mid-build is not a feed that stopped.
    """
    engine = state["engine"]
    as_of = state["as_of"]
    findings = list(state["findings"])
    results = runner.run_monitors(engine, as_of, state.get("feed_dir"))
    for sla in arrival.arrival_slas(state.get("feed_dir")):
        if engine.table_exists(sla.table):
            results += arrival.check_arrival(engine, sla, as_of)
    for result in results:
        if result.status != "breach":
            continue
        findings.append(sensors.Finding(
            kind="monitor_breach", table=result.table,
            detail=f"[{result.monitor}] {result.detail}",
            evidence={"monitor": result.monitor, "kind": result.kind,
                      "column": result.column, "metric": result.metric,
                      "baseline": result.baseline, "status": result.status}))
    return {**state, "findings": findings}


def classify_findings(state: AgentState) -> AgentState:
    classified = []
    for finding in state["findings"]:
        severity, reasoning = classifier.classify(finding)
        classified.append({"finding": finding, "severity": severity, "reasoning": reasoning})
    return {**state, "classified": classified}


def persist_findings(state: AgentState) -> AgentState:
    """Append this run to ``ops.finding_log`` and ``ops.agent_runs``.

    Runs on every path, including the one where nothing was found -- that is
    the whole reason ``ops.agent_runs`` exists.
    """
    engine = state["engine"]
    as_of = state["as_of"]
    classified = state["classified"]
    actioned = {id(c["finding"]) for c in classified if c["severity"] in ACTIONABLE}

    engine.create_table(tables.OPS_AGENT_RUNS)
    engine.append(tables.OPS_AGENT_RUNS.name, pa.Table.from_pylist([{
        "run_at": as_of, "findings_count": len(classified),
        "actioned_count": len(actioned), "dry_run": state["dry_run"],
    }], schema=tables.OPS_AGENT_RUNS.arrow_schema()))

    if classified:
        engine.create_table(tables.OPS_FINDING_LOG)
        engine.append(tables.OPS_FINDING_LOG.name, pa.Table.from_pylist([{
            "run_at": as_of,
            "finding_key": sensors.finding_key(c["finding"]),
            "table_name": c["finding"].table,
            "kind": c["finding"].kind,
            "severity": c["severity"],
            "column_name": (c["finding"].evidence.get("column")
                            or c["finding"].evidence.get("monitor")),
            "detail": c["finding"].detail,
            "reasoning": c["reasoning"],
            # JSON text, not a typed struct: evidence keys differ per kind.
            "evidence": json.dumps(c["finding"].evidence, default=str, sort_keys=True),
            "actioned": id(c["finding"]) in actioned,
        } for c in classified], schema=tables.OPS_FINDING_LOG.arrow_schema()))
    return state


def act(state: AgentState) -> AgentState:
    taken: list[str] = []
    for item in state["classified"]:
        if item["severity"] not in ACTIONABLE:
            continue
        path = actions.write_incident(item["finding"], item["severity"],
                                      item["reasoning"], state["as_of"])
        taken.append(f"incident: {path.name}")
        taken.append(actions.file_github_issue(item["finding"], item["severity"],
                                               item["reasoning"], execute=not state["dry_run"]))
    return {**state, "actions_taken": taken}


def _should_act(state: AgentState) -> str:
    return "act" if any(c["severity"] in ACTIONABLE for c in state["classified"]) else "end"


def build_graph():
    builder = StateGraph(AgentState)
    builder.add_node("sense", sense_and_detect)
    builder.add_node("monitor", run_monitors)
    builder.add_node("classify", classify_findings)
    builder.add_node("persist", persist_findings)
    builder.add_node("act", act)
    builder.set_entry_point("sense")
    builder.add_edge("sense", "monitor")
    builder.add_edge("monitor", "classify")
    # Persist sits on every path: a run that found nothing is still recorded,
    # or the report cannot distinguish "clean" from "never ran".
    builder.add_edge("classify", "persist")
    builder.add_conditional_edges("persist", _should_act, {"act": "act", "end": END})
    builder.add_edge("act", END)
    return builder.compile()


def run(engine=None, dry_run: bool = True, as_of: date | None = None,
        feed_dir=None) -> AgentState:
    if engine is None:
        from ops_agent.engine import SparkEngine
        engine = SparkEngine()
    if as_of is None:
        from ops_agent import config, lakehouse
        as_of = config.resolve_as_of_date(lakehouse.max_event_date(engine))
    return build_graph().invoke({
        "engine": engine, "as_of": as_of, "dry_run": dry_run, "feed_dir": feed_dir,
        "findings": [], "classified": [], "actions_taken": [],
    })


def main() -> int:
    execute = "--execute" in sys.argv
    state = run(dry_run=not execute)
    print(f"agent: as_of={state['as_of']} findings={len(state['findings'])}")
    for item in state["classified"]:
        print(f"  [{item['severity']}] {item['finding'].detail}")
    for action in state.get("actions_taken", []):
        print(f"  -> {action}")
    if not execute:
        print("agent: dry-run (pass --execute to file GitHub issues)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
