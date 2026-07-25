#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CLICKHOUSE_SVC=analytics-clickhouse
KAFKA_SVC=analytics-kafka
TOPIC="${EVENTS_TOPIC:-conversation.events}"
# The official ClickHouse image disables network access for user "default"
# unless these are set, so the stack defines an explicit user instead.
CH_USER="${CLICKHOUSE_USER:-analytics}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-analytics}"
export CLICKHOUSE_USER="$CH_USER" CLICKHOUSE_PASSWORD="$CH_PASSWORD"

# Ensure shared network exists
docker network inspect data-processing-network >/dev/null 2>&1 || \
    docker network create data-processing-network

usage() {
    cat <<'EOF'
Usage: ./deploy.sh <command> [options]

Lifecycle:
  up            Start Kafka, ClickHouse, Flink, and the analytics API
  down          Stop and remove containers
  restart       Recreate the stack
  status        Show container status
  logs [svc]    Tail logs (optionally for one service)

Pipeline:
  schema        Apply clickhouse/10_production.sql (tables + materialized views)
                plus 21_flink_rollup_ingest.sql (Kafka engine for Flink output)
  kafka-ingest  Apply 20_kafka_ingest.sql: ingest raw events with ClickHouse's
                Kafka engine INSTEAD of the Python consumer (do not run both)
  topic         Create the conversation.events + conversation.minute_agg topics
  produce [n]   Publish n synthetic conversation events (default 100000)
  consume       Run the simple Kafka -> ClickHouse Python consumer
  submit        Submit the PyFlink job to the Flink cluster
  query         Run a sample analytical query against ClickHouse

Measurement:
  bench [rows]  Run the ClickHouse schema tuning benchmark (default 20000000)
  bench-api     Measure end-to-end API latency percentiles
  explain       Regenerate results/explain.md (ClickHouse index analysis)
  chart         Regenerate results/tuning.svg from the last benchmark
  test          Run the pytest suite (embedded ClickHouse, no server needed)

Endpoints once up:
  API          http://localhost:8500/docs
  ClickHouse   http://localhost:8125
  Flink UI     http://localhost:8086
  Kafka        localhost:19092
EOF
    exit 1
}

wait_for_clickhouse() {
    echo "Waiting for ClickHouse..."
    for _ in $(seq 1 40); do
        if docker compose exec -T "$CLICKHOUSE_SVC" \
                clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
                --query "SELECT 1" >/dev/null 2>&1; then
            echo "ClickHouse is ready."
            return 0
        fi
        sleep 2
    done
    echo "Timed out waiting for ClickHouse." >&2
    exit 1
}

ensure_venv() {
    if [ ! -x "$SCRIPT_DIR/.venv/bin/python" ]; then
        echo "Creating local virtualenv with uv..."
        uv venv
        uv pip install -e ".[test]"
    fi
}

case "${1:-}" in
    up)
        echo "Starting the real-time analytics stack..."
        docker compose up -d --build
        wait_for_clickhouse
        echo ""
        echo "Next steps:"
        echo "  1. ./deploy.sh schema       # tables + materialized views"
        echo "  2. ./deploy.sh topic        # create the Kafka topic"
        echo "  3. ./deploy.sh produce      # publish synthetic events"
        echo "  4. ./deploy.sh consume      # or: ./deploy.sh submit (Flink)"
        echo "  5. open http://localhost:8500/docs"
        ;;
    down)
        docker compose down
        ;;
    restart)
        docker compose down
        docker compose up -d --build
        wait_for_clickhouse
        ;;
    status)
        docker compose ps
        ;;
    logs)
        shift
        docker compose logs -f "$@"
        ;;
    schema)
        echo "Applying clickhouse/10_production.sql..."
        docker compose exec -T "$CLICKHOUSE_SVC" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
            --multiquery < "$SCRIPT_DIR/clickhouse/10_production.sql"
        echo "Applying clickhouse/21_flink_rollup_ingest.sql..."
        docker compose exec -T "$CLICKHOUSE_SVC" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
            --multiquery < "$SCRIPT_DIR/clickhouse/21_flink_rollup_ingest.sql"
        echo "Schema applied. Tables:"
        docker compose exec -T "$CLICKHOUSE_SVC" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
            --query "SHOW TABLES" --format PrettyCompact
        ;;
    kafka-ingest)
        echo "Applying clickhouse/20_kafka_ingest.sql (ClickHouse Kafka engine)..."
        echo "NOTE: this replaces './deploy.sh consume'. Running both double-counts."
        docker compose exec -T "$CLICKHOUSE_SVC" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
            --multiquery < "$SCRIPT_DIR/clickhouse/20_kafka_ingest.sql"
        echo "ClickHouse is now consuming $TOPIC directly."
        ;;
    topic)
        for t in "$TOPIC" "${MINUTE_AGG_TOPIC:-conversation.minute_agg}"; do
            echo "Creating topic '$t' with 12 partitions..."
            docker compose exec -T "$KAFKA_SVC" kafka-topics \
                --bootstrap-server analytics-kafka:9092 \
                --create --if-not-exists --topic "$t" \
                --partitions 12 --replication-factor 1
        done
        docker compose exec -T "$KAFKA_SVC" kafka-topics \
            --bootstrap-server analytics-kafka:9092 --list
        ;;
    produce)
        ensure_venv
        total="${2:-100000}"
        echo "Publishing $total events to $TOPIC..."
        KAFKA_BOOTSTRAP=localhost:19092 EVENTS_TOPIC="$TOPIC" \
            "$SCRIPT_DIR/.venv/bin/python" -m realtime_analytics.producer \
            --total "$total"
        ;;
    consume)
        ensure_venv
        echo "Consuming $TOPIC into ClickHouse (Ctrl-C to stop)..."
        KAFKA_BOOTSTRAP=localhost:19092 EVENTS_TOPIC="$TOPIC" \
        CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8125 \
        CLICKHOUSE_USER="$CH_USER" CLICKHOUSE_PASSWORD="$CH_PASSWORD" \
            "$SCRIPT_DIR/.venv/bin/python" -m realtime_analytics.consumer
        ;;
    submit)
        echo "Submitting the PyFlink job..."
        docker compose exec -T analytics-flink-jobmanager \
            /opt/flink/bin/flink run \
            -pyclientexec python3 \
            -py /opt/flink-apps/realtime_analytics/flink_job.py \
            -pyfs /opt/flink-apps
        echo "Job submitted. Flink UI: http://localhost:8086"
        ;;
    query)
        docker compose exec -T "$CLICKHOUSE_SVC" clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" \
            --format PrettyCompact --query "
            SELECT
                account_id,
                sum(conversations)                                  AS conversations,
                sum(escalations)                                    AS escalations,
                round(escalations / nullIf(conversations, 0), 4)    AS escalation_rate,
                round(quantilesTDigestMerge(0.5, 0.95, 0.99)(latency_state)[2])
                                                                    AS p95_latency_ms
            FROM conversation_daily
            GROUP BY account_id
            ORDER BY conversations DESC
            LIMIT 20"
        ;;
    bench)
        ensure_venv
        rows="${2:-20000000}"
        "$SCRIPT_DIR/.venv/bin/python" benchmarks/bench_clickhouse.py \
            --rows "$rows" --backend chdb
        ;;
    explain)
        ensure_venv
        "$SCRIPT_DIR/.venv/bin/python" benchmarks/explain_evidence.py "${@:2}"
        ;;
    chart)
        ensure_venv
        "$SCRIPT_DIR/.venv/bin/python" benchmarks/make_chart.py
        ;;
    bench-api)
        ensure_venv
        shift
        "$SCRIPT_DIR/.venv/bin/python" benchmarks/bench_api.py "$@"
        ;;
    test)
        ensure_venv
        "$SCRIPT_DIR/.venv/bin/python" -m pytest "${@:2}"
        ;;
    *)
        usage
        ;;
esac
