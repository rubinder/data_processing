#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    cat <<'USAGE'
Usage: ./deploy.sh <command>

All commands run against the local filesystem Iceberg catalog under
./iceberg-warehouse (override with ICEBERG_WAREHOUSE, or point at the REST
catalog from ../iceberg_deployment with ICEBERG_CATALOG_TYPE=rest).

  test [args]   Run the pytest suite (rules against a fake engine, plus Spark
                integration tests against real Iceberg tables)
  demo          The full walkthrough: seed, monitor, clean agent run, evolve the
                schema and inject a collapsed batch, agent run, reports
  seed [n]      Append one batch of n impressions (default 200) to db.impressions
  build         Rebuild db.impressions_aggregated, gated by its contract
  monitor       Run every monitor, persist results, route (dry-run) alerts
  arrival       Arrival SLA checks
  agent         Run the ops agent (dry-run; add --execute to file GitHub issues)
  report        Write reports/daily-<as_of>.md from ops.*
  clean         Remove the local warehouse

The logical date is the newest event date in db.impressions; set
AS_OF_DATE=YYYY-MM-DD to reproduce a stale feed.
USAGE
    exit 1
}

ensure_java() {
    if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/libexec/java_home ]; then
        JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || /usr/libexec/java_home -v 11 2>/dev/null || true)"
        export JAVA_HOME
    fi
}

run() { uv run --extra test python -m "$@"; }

ensure_java
case "${1:-}" in
    test)     uv run --extra test pytest "${@:2}" ;;
    demo)     run ops_agent.demo "${@:2}" ;;
    seed)     run ops_agent.lakehouse seed "${@:2}" ;;
    build)    run ops_agent.lakehouse build ;;
    monitor)  run ops_agent.runner "${@:2}" ;;
    arrival)  run ops_agent.arrival ;;
    agent)    run ops_agent.graph "${@:2}" ;;
    report)   run ops_agent.report ;;
    clean)    rm -rf "$SCRIPT_DIR/iceberg-warehouse" ;;
    *)        usage ;;
esac
