#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE_CMD="docker compose"

usage() {
    echo "Usage: $0 {up|down|status|logs|test|bench|load|api} [local]"
    echo ""
    echo "Commands:"
    echo "  up       Start Pinecone Local (emulator) and the vector API"
    echo "  down     Stop and remove the containers"
    echo "  status   Show container status and index list"
    echo "  logs     Tail logs"
    echo "  test     Run the test suite against Pinecone Local"
    echo "  bench    Run benchmarks/run_local.py (writes the README tables)"
    echo "  load     Generate and upsert the synthetic corpus into PINECONE_INDEX"
    echo "  api      Run the FastAPI service on the host (port 8090)"
    echo ""
    echo "Options:"
    echo "  local    Standalone network instead of the shared data-processing-network"
    echo ""
    echo "Cloud: set PINECONE_API_KEY (and unset PINECONE_LOCAL_HOST) and the same"
    echo "commands run against a serverless index. EMBEDDER=pinecone uses hosted models."
    exit 1
}

if [ $# -eq 0 ]; then usage; fi
for arg in "$@"; do
    if [ "$arg" = "local" ]; then
        COMPOSE_CMD="docker compose -f docker-compose.yaml -f docker-compose.local.yaml"
        echo "Running in standalone mode"
        break
    fi
done
ARGS=()
for arg in "$@"; do [ "$arg" != "local" ] && ARGS+=("$arg"); done
CMD="${ARGS[0]:-}"

case "$CMD" in
    up)
        docker network inspect data-processing-network >/dev/null 2>&1 || \
            docker network create data-processing-network >/dev/null
        ${COMPOSE_CMD} up -d --build
        echo "Pinecone Local control plane: http://localhost:5080  API: http://localhost:8090/health"
        ;;
    down)    ${COMPOSE_CMD} down ;;
    status)  ${COMPOSE_CMD} ps; curl -s -H "Api-Key: pclocal" http://localhost:5080/indexes | python3 -m json.tool 2>/dev/null | head -40 ;;
    logs)    ${COMPOSE_CMD} logs -f ;;
    test)    uv run --extra test pytest -q ;;
    bench)   PYTHONPATH=. uv run python benchmarks/run_local.py "${ARGS[@]:1}" ;;
    load)
        PYTHONPATH=. uv run python - <<'PY'
import os
from pinecone_deployment.conversations import generate
from pinecone_deployment.embeddings import make_embedder
from pinecone_deployment.store import ConversationStore, connect
client = connect()
name = os.environ.get("EMBEDDER", "hash")
embedder = make_embedder(name, client=client) if name == "pinecone" else make_embedder(name)
store = ConversationStore(client, embedder, os.environ.get("PINECONE_INDEX", "conversations"),
                          isolation=os.environ.get("ISOLATION", "namespace"))
store.ensure_index()
n = store.upsert(generate(int(os.environ.get("COUNT", "2000")), int(os.environ.get("ACCOUNTS", "20"))))
print(f"upserted {n} vectors into {store.index_name}; stats: {store.stats()}")
PY
        ;;
    api)     uv run uvicorn pinecone_deployment.api:app --host 0.0.0.0 --port 8090 ;;
    *)       usage ;;
esac
