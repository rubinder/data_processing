#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    cat <<'USAGE'
Usage: ./deploy.sh <command>

Trino over the Iceberg REST catalog (../iceberg_deployment) and the dbt
PostgreSQL (../dbt_deployment). `up` also starts the Iceberg stack; start
PostgreSQL with ../dbt_deployment/deploy.sh up and apply the stage roles with
../mcp_deployment/deploy.sh apply.

  up            Start Iceberg REST + the S3 gateway and the Trino coordinator, wait until healthy
  down          Stop Trino (the Iceberg stack stays up)
  status        Container status
  logs          Tail Trino logs
  cli           The Trino CLI inside the container
  seed [mode]   Fill iceberg.db.impressions: postgres (default), synthetic, truncate
  analyses      The four analyses over Iceberg
  reconcile     Trino-over-Iceberg vs dbt gold, one query, per-metric deltas
  timetravel    Snapshots, FOR VERSION AS OF, partitions
  pushdown      What is and is not pushed into PostgreSQL, with plans and timings
  bench         Engine comparison -> benchmarks/results/engines.md
  demo          Everything above, printed
  test [args]   pytest: unit tests always, Trino tests when the stack is reachable

Endpoints once up: Trino UI http://localhost:8085  (user: any name)
USAGE
    exit 1
}

run() { uv run --extra test python -m "$@"; }

case "${1:-}" in
    up)
        (cd ../iceberg_deployment && docker compose up -d)
        docker compose up -d
        echo "waiting for Trino..."
        until [ "$(docker inspect -f '{{.State.Health.Status}}' trino 2>/dev/null)" = "healthy" ]; do sleep 3; done
        echo "Trino ready at http://localhost:8085"
        ;;
    down)     docker compose down ;;
    status)   docker compose ps ;;
    logs)     docker compose logs -f trino ;;
    cli)      docker exec -it trino trino ;;
    seed)     run trino_deployment.seed "${@:2}" ;;
    analyses) run trino_deployment.analyses "${@:2}" ;;
    reconcile) run trino_deployment.federation ;;
    timetravel) run trino_deployment.timetravel ;;
    pushdown) run trino_deployment.pushdown ;;
    bench)    uv run --extra test python benchmarks/bench_engines.py "${@:2}" ;;
    demo)     run trino_deployment.demo ;;
    test)     uv run --extra test pytest "${@:2}" ;;
    *)        usage ;;
esac
