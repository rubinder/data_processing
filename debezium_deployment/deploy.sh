#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"
CONNECT_URL="http://localhost:8083"
# Host port 8085 -> container 8081 (8081/8082 are taken by Flink).
REGISTRY_URL="http://localhost:8085"
# What containers on the network (Connect, Flink) use.
REGISTRY_INTERNAL_URL="http://debezium-schema-registry:8081"
LOCAL_NETWORK="debezium-local-network"
LOCAL=false

EVENTS_SUBJECT="cdc.impressions.events-value"
VALID_COMPAT="BACKWARD BACKWARD_TRANSITIVE FORWARD FORWARD_TRANSITIVE FULL FULL_TRANSITIVE NONE"
DEFAULT_COMPAT="BACKWARD"

usage() {
    echo "Usage: $0 <command> [args] [local]"
    echo ""
    echo "Stack:"
    echo "  up                 Build and start Zookeeper, Kafka, Schema Registry, PostgreSQL,"
    echo "                     Debezium Connect (with Confluent Avro converter), Kafka UI"
    echo "  down               Stop and remove all services (and volumes)"
    echo "  restart            Restart all services"
    echo "  status             Show service status"
    echo "  logs [service]     Tail logs from all services (or one service)"
    echo ""
    echo "Connector:"
    echo "  register [avro|json]  Register the PostgreSQL source connector (default: avro)."
    echo "                        avro: connectors/postgres-source.json, Avro + Schema Registry,"
    echo "                              sets ${DEFAULT_COMPAT} compatibility on the CDC subjects first"
    echo "                        json: connectors/postgres-source-json.json, schemaless JSON"
    echo "  connectors            List registered connectors and their status"
    echo "  topics                List Kafka topics"
    echo "  consume <topic>       Consume raw messages from a topic"
    echo "  consume-avro <topic>  Consume a topic, decoding Avro via the registry"
    echo "  exec-sql <file>       Execute a SQL file against the source database"
    echo ""
    echo "Schema Registry:"
    echo "  schemas               List subjects with their latest version / schema id / fields"
    echo "  compat [mode] [subj]  Show or set compatibility. No mode: show global (or subject)."
    echo "                        mode in: ${VALID_COMPAT}"
    echo "                        No subject: sets the global default."
    echo "  evolve                Apply schema_changes.sql (ADD / RENAME / DROP COLUMN) and print"
    echo "                        every registered version of ${EVENTS_SUBJECT}"
    echo "  evolve-incompatible   Try to register a schema BACKWARD must reject (a required field"
    echo "                        with no default) directly against the registry; expect HTTP 409"
    echo ""
    echo "Options:"
    echo "  local       Use a standalone network (${LOCAL_NETWORK}) instead of the shared"
    echo "              data-processing-network (no Airflow dependency). flink_deployment's"
    echo "              'local' mode joins the same network."
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

# Check for 'local' flag in any argument position
for arg in "$@"; do
    if [ "$arg" = "local" ]; then
        COMPOSE_CMD="docker compose -f docker-compose.yaml -f docker-compose.local.yaml"
        LOCAL=true
        echo "Running in standalone mode (no data-processing-network required)"
        break
    fi
done

# Collect non-'local' arguments
ARGS=()
for arg in "$@"; do
    if [ "$arg" != "local" ]; then
        ARGS+=("$arg")
    fi
done

CMD="${ARGS[0]:-}"

# The local network is external in docker-compose.local.yaml so that
# flink_deployment can join it too; create it here if it does not exist yet.
ensure_local_network() {
    if [ "$LOCAL" = true ]; then
        if ! docker network inspect "$LOCAL_NETWORK" > /dev/null 2>&1; then
            echo "Creating Docker network ${LOCAL_NETWORK}..."
            docker network create "$LOCAL_NETWORK" > /dev/null
        fi
    fi
}

remove_local_network() {
    if [ "$LOCAL" = true ]; then
        # Fails harmlessly while another project (e.g. Flink) is still attached.
        docker network rm "$LOCAL_NETWORK" > /dev/null 2>&1 || true
    fi
}

wait_for_connect() {
    echo "Waiting for Debezium Connect to be ready..."
    local retries=30
    local count=0
    while [ $count -lt $retries ]; do
        if curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
            echo "Debezium Connect is ready."
            return 0
        fi
        count=$((count + 1))
        echo "  Attempt $count/$retries - Connect not ready yet..."
        sleep 5
    done
    echo "Error: Debezium Connect did not become ready in time."
    return 1
}

wait_for_registry() {
    echo "Waiting for Schema Registry to be ready..."
    local retries=30
    local count=0
    while [ $count -lt $retries ]; do
        if curl -s "${REGISTRY_URL}/subjects" > /dev/null 2>&1; then
            echo "Schema Registry is ready."
            return 0
        fi
        count=$((count + 1))
        echo "  Attempt $count/$retries - Schema Registry not ready yet..."
        sleep 5
    done
    echo "Error: Schema Registry did not become ready in time."
    return 1
}

validate_compat() {
    local mode="$1"
    for m in $VALID_COMPAT; do
        if [ "$m" = "$mode" ]; then
            return 0
        fi
    done
    echo "Error: invalid compatibility mode '${mode}'. Valid: ${VALID_COMPAT}"
    return 1
}

# set_compat <mode> [subject]  -- PUT /config or /config/<subject>
set_compat() {
    local mode="$1"
    local subject="${2:-}"
    validate_compat "$mode"
    local path="/config"
    if [ -n "$subject" ]; then
        path="/config/${subject}"
    fi
    local response
    response=$(curl -s -w "\n%{http_code}" -X PUT \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"compatibility\": \"${mode}\"}" \
        "${REGISTRY_URL}${path}")
    local code="${response##*$'\n'}"
    local body="${response%$'\n'*}"
    if [ "$code" = "200" ]; then
        echo "  ${path} -> ${body}"
    else
        echo "  Failed to set compatibility on ${path} (HTTP ${code}): ${body}"
        return 1
    fi
}

# get_compat [subject]
get_compat() {
    local subject="${1:-}"
    if [ -n "$subject" ]; then
        # defaultToGlobal=true shows the effective level when no per-subject override exists.
        echo -n "  ${subject}: "
        curl -s "${REGISTRY_URL}/config/${subject}?defaultToGlobal=true"
        echo ""
    else
        echo -n "  global: "
        curl -s "${REGISTRY_URL}/config"
        echo ""
    fi
}

# print_versions <subject>  -- every version with schema id and top-level field names
print_versions() {
    local subject="$1"
    local versions
    versions=$(curl -s "${REGISTRY_URL}/subjects/${subject}/versions")
    if ! echo "$versions" | python3 -c "import sys,json; v=json.load(sys.stdin); sys.exit(0 if isinstance(v, list) else 1)" 2>/dev/null; then
        echo "  ${subject}: ${versions}"
        return 0
    fi
    echo "Subject ${subject}: versions ${versions}"
    for V in $(echo "$versions" | python3 -c "import sys,json; [print(v) for v in json.load(sys.stdin)]"); do
        # The Python is fed on stdin (heredoc) and the JSON via an env var so
        # neither the shell nor the f-string quoting has to be escaped.
        SCHEMA_JSON=$(curl -s "${REGISTRY_URL}/subjects/${subject}/versions/${V}") python3 - <<'PY'
import json
import os

d = json.loads(os.environ["SCHEMA_JSON"])
schema = json.loads(d["schema"])
fields = schema.get("fields", []) if isinstance(schema, dict) else []


def describe(f):
    t = f["type"]
    opt = isinstance(t, list) and "null" in t
    base = [x for x in t if x != "null"] if isinstance(t, list) else [t]
    base = [b["type"] if isinstance(b, dict) else b for b in base]
    dflt = " default=" + json.dumps(f["default"]) if "default" in f else ""
    return f"{f['name']}:{'|'.join(base)}{'?' if opt else ''}{dflt}"


print(f"  v{d['version']} (id {d['id']}): " + ", ".join(describe(f) for f in fields))
PY
    done
}

case "$CMD" in
    up)
        ensure_local_network
        echo "Starting Debezium CDC stack..."
        ${COMPOSE_CMD} up --build -d
        echo ""
        echo "Services starting:"
        echo "  Kafka UI:          http://localhost:8084"
        echo "  Debezium Connect:  ${CONNECT_URL}"
        echo "  Schema Registry:   ${REGISTRY_URL}  (in-network: ${REGISTRY_INTERNAL_URL})"
        echo "  Kafka:             debezium-kafka:9092 (in-network)"
        echo "  PostgreSQL:        localhost:5434 (cdc_user/cdc_pass/cdc_source)"
        echo ""
        echo "Next steps:"
        echo "  1. Wait for services to be healthy: $0 status"
        echo "  2. Register the connector (Avro + registry): $0 register"
        echo "  3. Apply sample changes: $0 exec-sql sample_changes.sql"
        echo "  4. View topics / schemas: $0 topics ; $0 schemas"
        echo "  5. Consume events: $0 consume-avro cdc.impressions.events"
        echo "  6. Evolve the source schema: $0 evolve"
        ;;
    down)
        echo "Stopping Debezium CDC stack..."
        ${COMPOSE_CMD} down -v
        remove_local_network
        echo "Debezium CDC stack stopped and volumes removed."
        ;;
    restart)
        ensure_local_network
        echo "Restarting Debezium CDC stack..."
        ${COMPOSE_CMD} down
        ${COMPOSE_CMD} up --build -d
        echo "Debezium CDC stack restarted."
        ;;
    status)
        ${COMPOSE_CMD} ps
        echo ""
        echo "Checking Debezium Connect status..."
        if curl -s "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
            echo "Connect API: UP"
            CONNECTORS=$(curl -s "${CONNECT_URL}/connectors")
            echo "Registered connectors: ${CONNECTORS}"
        else
            echo "Connect API: NOT READY (services may still be starting)"
        fi
        echo "Checking Schema Registry status..."
        if curl -s "${REGISTRY_URL}/subjects" > /dev/null 2>&1; then
            echo "Schema Registry: UP"
            echo "Subjects: $(curl -s "${REGISTRY_URL}/subjects")"
            get_compat
        else
            echo "Schema Registry: NOT READY (services may still be starting)"
        fi
        ;;
    logs)
        SERVICE="${ARGS[1]:-}"
        if [ -n "$SERVICE" ]; then
            ${COMPOSE_CMD} logs -f "$SERVICE"
        else
            ${COMPOSE_CMD} logs -f
        fi
        ;;
    register)
        FORMAT="${ARGS[1]:-avro}"
        case "$FORMAT" in
            avro)
                CONNECTOR_FILE="connectors/postgres-source.json"
                ;;
            json)
                CONNECTOR_FILE="connectors/postgres-source-json.json"
                ;;
            *)
                echo "Error: unknown format '${FORMAT}' (use avro or json)"
                exit 1
                ;;
        esac
        wait_for_connect
        if [ "$FORMAT" = "avro" ]; then
            wait_for_registry
            # Set the compatibility mode explicitly *before* the first schema is
            # registered, per subject, so the gate is in place for version 1 and
            # is visible via GET /config/<subject>. BACKWARD: consumers (Flink)
            # are upgraded before producers; Debezium's ADD COLUMN produces
            # nullable-with-default fields, which BACKWARD allows. See README.
            echo "Setting ${DEFAULT_COMPAT} compatibility on CDC subjects..."
            set_compat "$DEFAULT_COMPAT"
            for TABLE in events users pull_status; do
                set_compat "$DEFAULT_COMPAT" "cdc.impressions.${TABLE}-key"
                set_compat "$DEFAULT_COMPAT" "cdc.impressions.${TABLE}-value"
            done
        fi
        echo "Registering PostgreSQL source connector (${FORMAT}) from ${CONNECTOR_FILE}..."
        RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d @"${CONNECTOR_FILE}" \
            "${CONNECT_URL}/connectors")
        if [ "$RESPONSE" = "201" ]; then
            echo "Connector registered successfully."
            echo "CDC topics will be created as changes occur:"
            echo "  - cdc.impressions.events"
            echo "  - cdc.impressions.users"
            echo "  - cdc.impressions.pull_status"
            if [ "$FORMAT" = "avro" ]; then
                echo "Schemas register under <topic>-key / <topic>-value; view with: $0 schemas"
            fi
        elif [ "$RESPONSE" = "409" ]; then
            echo "Connector already exists. Use 'connectors' to check status."
        else
            echo "Failed to register connector (HTTP $RESPONSE)."
            curl -s -X POST \
                -H "Content-Type: application/json" \
                -d @"${CONNECTOR_FILE}" \
                "${CONNECT_URL}/connectors" | python3 -m json.tool
            exit 1
        fi
        ;;
    connectors)
        echo "Registered connectors:"
        curl -s "${CONNECT_URL}/connectors" | python3 -m json.tool
        echo ""
        echo "Connector details:"
        for CONN in $(curl -s "${CONNECT_URL}/connectors" | python3 -c "import sys,json; [print(c) for c in json.load(sys.stdin)]" 2>/dev/null); do
            echo "--- ${CONN} ---"
            curl -s "${CONNECT_URL}/connectors/${CONN}/status" | python3 -m json.tool
            echo ""
        done
        ;;
    topics)
        echo "Kafka topics:"
        docker exec debezium-kafka kafka-topics --bootstrap-server localhost:9092 --list
        ;;
    consume)
        TOPIC="${ARGS[1]:-}"
        if [ -z "$TOPIC" ]; then
            echo "Error: provide a topic name, e.g.: $0 consume cdc.impressions.events"
            exit 1
        fi
        echo "Consuming from topic: $TOPIC (Ctrl+C to stop)"
        docker exec debezium-kafka kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic "$TOPIC" \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" | "
        ;;
    consume-avro)
        TOPIC="${ARGS[1]:-}"
        if [ -z "$TOPIC" ]; then
            echo "Error: provide a topic name, e.g.: $0 consume-avro cdc.impressions.events"
            exit 1
        fi
        echo "Consuming Avro from topic: $TOPIC via ${REGISTRY_INTERNAL_URL} (Ctrl+C to stop)"
        docker exec debezium-schema-registry kafka-avro-console-consumer \
            --bootstrap-server debezium-kafka:9092 \
            --property schema.registry.url="${REGISTRY_INTERNAL_URL}" \
            --topic "$TOPIC" \
            --from-beginning \
            --property print.key=true \
            --property key.separator=" | "
        ;;
    exec-sql)
        SQL_FILE="${ARGS[1]:-}"
        if [ -z "$SQL_FILE" ]; then
            echo "Error: provide a SQL file path, e.g.: $0 exec-sql sample_changes.sql"
            exit 1
        fi
        if [ ! -f "$SQL_FILE" ]; then
            echo "Error: file not found: $SQL_FILE"
            exit 1
        fi
        echo "Executing SQL file: $SQL_FILE"
        docker exec -i debezium-postgres psql -U cdc_user -d cdc_source < "$SQL_FILE"
        echo "SQL executed. CDC events should appear in Kafka topics shortly."
        ;;
    schemas)
        wait_for_registry
        echo "Compatibility:"
        get_compat
        SUBJECTS=$(curl -s "${REGISTRY_URL}/subjects" | python3 -c "import sys,json; [print(s) for s in sorted(json.load(sys.stdin))]")
        if [ -z "$SUBJECTS" ]; then
            echo "No subjects registered yet (register the Avro connector and generate a change)."
            exit 0
        fi
        echo "Subjects (latest version):"
        for SUBJ in $SUBJECTS; do
            SCHEMA_JSON=$(curl -s "${REGISTRY_URL}/subjects/${SUBJ}/versions/latest") python3 - <<'PY'
import json
import os

d = json.loads(os.environ["SCHEMA_JSON"])
schema = json.loads(d["schema"])
fields = [f["name"] for f in schema.get("fields", [])] if isinstance(schema, dict) else []
print(f"  {d['subject']}: v{d['version']} (id {d['id']}) fields={fields}")
PY
        done
        ;;
    compat)
        MODE="${ARGS[1]:-}"
        SUBJECT="${ARGS[2]:-}"
        wait_for_registry
        if [ -z "$MODE" ]; then
            get_compat "$SUBJECT"
        else
            set_compat "$MODE" "$SUBJECT"
        fi
        ;;
    evolve)
        wait_for_registry
        echo "Schema versions BEFORE:"
        print_versions "$EVENTS_SUBJECT"
        echo ""
        echo "Applying schema_changes.sql (ADD COLUMN / RENAME COLUMN / DROP COLUMN, each followed by an INSERT)..."
        docker exec -i debezium-postgres psql -U cdc_user -d cdc_source < schema_changes.sql
        echo ""
        echo "Waiting for Debezium to emit the changed records and register new schema versions..."
        BEFORE_COUNT=$(curl -s "${REGISTRY_URL}/subjects/${EVENTS_SUBJECT}/versions" | python3 -c "import sys,json; v=json.load(sys.stdin); print(len(v) if isinstance(v, list) else 0)")
        for _ in $(seq 1 20); do
            sleep 3
            COUNT=$(curl -s "${REGISTRY_URL}/subjects/${EVENTS_SUBJECT}/versions" | python3 -c "import sys,json; v=json.load(sys.stdin); print(len(v) if isinstance(v, list) else 0)")
            if [ "$COUNT" -ge $((BEFORE_COUNT + 3)) ]; then
                break
            fi
        done
        echo "Schema versions AFTER:"
        print_versions "$EVENTS_SUBJECT"
        echo ""
        echo "Connector task state (a 409 from the registry shows up here as FAILED):"
        curl -s "${CONNECT_URL}/connectors/impressions-postgres-connector/status" | python3 -c "import sys,json; d=json.load(sys.stdin); print('  connector:', d['connector']['state']); [print('  task', str(t['id']) + ':', t['state'], t.get('trace', '').splitlines()[0] if t.get('trace') else '') for t in d['tasks']]"
        ;;
    evolve-incompatible)
        wait_for_registry
        echo "Effective compatibility for ${EVENTS_SUBJECT}:"
        get_compat "$EVENTS_SUBJECT"
        LATEST=$(curl -s "${REGISTRY_URL}/subjects/${EVENTS_SUBJECT}/versions/latest")
        if ! echo "$LATEST" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if 'schema' in d else 1)" 2>/dev/null; then
            echo "Error: no schema registered yet for ${EVENTS_SUBJECT}: ${LATEST}"
            exit 1
        fi
        # Take the latest registered schema and add a REQUIRED field with no
        # default. Under BACKWARD the new (reader) schema must be able to read
        # every record written with the previous schema; a required field the
        # old writer never produced has no value to fall back on, so the
        # registry must refuse it.
        PAYLOAD=$(echo "$LATEST" | python3 -c '
import sys, json
d = json.load(sys.stdin)
schema = json.loads(d["schema"])
schema["fields"].append({"name": "mandatory_no_default", "type": "string"})
print(json.dumps({"schema": json.dumps(schema)}))
')
        echo ""
        echo "POST ${REGISTRY_URL}/subjects/${EVENTS_SUBJECT}/versions"
        echo "  with latest schema + {\"name\": \"mandatory_no_default\", \"type\": \"string\"} (required, no default)"
        RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -d "$PAYLOAD" \
            "${REGISTRY_URL}/subjects/${EVENTS_SUBJECT}/versions")
        CODE="${RESPONSE##*$'\n'}"
        BODY="${RESPONSE%$'\n'*}"
        echo "HTTP ${CODE}"
        echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
        echo ""
        if [ "$CODE" = "409" ]; then
            echo "Rejected as expected: the compatibility gate refused a required field without a default."
        else
            echo "Unexpected response (expected 409). Check compatibility with: $0 compat '' ${EVENTS_SUBJECT}"
            exit 1
        fi
        echo "Versions are unchanged:"
        print_versions "$EVENTS_SUBJECT"
        ;;
    *)
        usage
        ;;
esac
