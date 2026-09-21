#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    echo "Usage: $0 {build|load-data|query|explain|partitions|benchmark|shell|clean}"
    echo ""
    echo "The container runs to completion: each command is one CLI invocation"
    echo "against the Parquet store on the polars-data volume."
    echo ""
    echo "Commands:"
    echo "  build                 Build the image"
    echo "  load-data ARGS        Fetch one partition from the web server API"
    echo "                        ARGS: --page_type N --date YYYY-MM-DD --hour H"
    echo "  query NAME [ARGS]     Run an analysis. NAME is one of:"
    echo "                        funnel | page-type-summary | user-engagement |"
    echo "                        hourly-traffic"
    echo "                        ARGS: --engine in-memory|streaming --format table|json"
    echo "                              --page_type N --date D --hour H"
    echo "  explain NAME [ARGS]   Print the optimized plan (add --unoptimized for both)"
    echo "  partitions            List partitions in the store"
    echo "  benchmark [ARGS]      Run benchmarks/bench_engines.py inside the container"
    echo "                        ARGS: --days N --repeat N --regenerate"
    echo "  shell                 Open a shell in the container"
    echo "  clean                 Remove the container and the data volume"
    exit 1
}

run() {
    docker compose run --rm polars-app "$@"
}

case "${1:-}" in
    build)
        docker compose build polars-app
        echo ""
        echo "Next steps:"
        echo "  1. Start the web server: cd ../web_server_local && ./deploy.sh up"
        echo "  2. Load data: $0 load-data --page_type 1 --date 2026-01-01 --hour 10"
        echo "  3. Query:     $0 query funnel"
        ;;
    load-data)
        shift
        run load "$@"
        ;;
    query)
        shift
        [ -n "${1:-}" ] || { echo "Error: query requires an analysis name"; usage; }
        run query "$@"
        ;;
    explain)
        shift
        [ -n "${1:-}" ] || { echo "Error: explain requires an analysis name"; usage; }
        run explain "$@"
        ;;
    partitions)
        run partitions
        ;;
    benchmark)
        shift
        docker compose run --rm --entrypoint python polars-app \
            benchmarks/bench_engines.py --data-dir /data/bench --out /data/results "$@"
        echo ""
        echo "Results are on the polars-data volume under /data/results."
        echo "Copy them out with: docker compose run --rm --entrypoint cat polars-app /data/results/latest.md"
        ;;
    shell)
        docker compose run --rm --entrypoint /bin/bash polars-app
        ;;
    clean)
        docker compose down -v
        ;;
    *)
        usage
        ;;
esac
