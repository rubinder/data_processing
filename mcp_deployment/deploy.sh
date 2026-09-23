#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

usage() {
    cat <<'USAGE'
Usage: ./deploy.sh <command>

Runs against the dbt deployment's PostgreSQL (../dbt_deployment, localhost:5433,
pgvector image). Override with GOLD_DB_HOST / GOLD_DB_PORT / GOLD_DB_NAME and
GOLD_DB_ADMIN_USER / GOLD_DB_ADMIN_PASSWORD; stage-role passwords come from
INGESTION_PASSWORD, TRANSFORM_PASSWORD, PROMOTION_PASSWORD, MCP_READER_PASSWORD.

  test [args]    Run the pytest suite (unit tests always; PostgreSQL tests when reachable)
  apply          Apply sql/*.sql as the admin: roles, gold + promotion functions, catalog
  promote [note] Promote public_analytics -> gold as the promotion role
  rollback       Swap the previous release back in
  releases       Show the release log
  sync-catalog   Regenerate the pgvector catalog from the dbt manifest + templates (transform role)
  serve          Run the MCP server on stdio as mcp_reader (point your MCP client here)
  ask <question> Search the catalog from the command line
  demo           apply -> promote -> sync -> search -> run a template -> rollback, printed
USAGE
    exit 1
}

run() { uv run --extra test python -m "$@"; }

case "${1:-}" in
    test)         uv run --extra test pytest "${@:2}" ;;
    apply)        run mcp_deployment.demo apply ;;
    promote)      run mcp_deployment.promotion promote "${@:2}" ;;
    rollback)     run mcp_deployment.promotion rollback ;;
    releases)     run mcp_deployment.promotion releases ;;
    sync-catalog) run mcp_deployment.demo sync ;;
    serve)        uv run python -m mcp_deployment.server ;;
    ask)          run mcp_deployment.demo ask "${@:2}" ;;
    demo)         run mcp_deployment.demo ;;
    *)            usage ;;
esac
