# Debezium CDC Deployment

Change Data Capture (CDC) pipeline using Debezium to stream PostgreSQL changes into Kafka topics in real time.

## Architecture

```
PostgreSQL (source) → Debezium Connect → Kafka → Kafka UI (monitoring)
```

**Tables captured:**
- `impressions.events` — impression event data (inserts, updates, deletes)
- `impressions.users` — user dimension table
- `impressions.pull_status` — data pull tracking

**Kafka topics created:**
- `cdc.impressions.events`
- `cdc.impressions.users`
- `cdc.impressions.pull_status`

## Services

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5434 | Source database (wal_level=logical) |
| Kafka | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordination |
| Debezium Connect | 8083 | Kafka Connect with Debezium PostgreSQL connector |
| Kafka UI | 8084 | Web UI for browsing topics and messages |

## Quick Start

```bash
# Start all services (requires data-processing-network from airflow_deployment)
./deploy.sh up

# Or run standalone without shared network
./deploy.sh up local

# Wait for services, then register the connector
./deploy.sh register

# Apply sample DML to generate CDC events
./deploy.sh exec-sql sample_changes.sql

# View Kafka topics
./deploy.sh topics

# Consume CDC events from a topic
./deploy.sh consume cdc.impressions.events

# Open Kafka UI in browser
open http://localhost:8084
```

## Commands

```bash
./deploy.sh up          # Start all services
./deploy.sh down        # Stop all services and remove volumes
./deploy.sh restart     # Restart all services
./deploy.sh status      # Show service and connector status
./deploy.sh logs        # Tail all logs (or: logs debezium-connect)
./deploy.sh register    # Register the PostgreSQL source connector
./deploy.sh connectors  # List connectors and their status
./deploy.sh topics      # List Kafka topics
./deploy.sh consume <topic>    # Consume messages from a topic
./deploy.sh exec-sql <file>    # Run SQL against the source database
```

## Schema Registry and schema evolution

The stack includes Confluent Schema Registry (`localhost:8085`). The default
connector uses the Avro converter, so every table shape is a registered,
versioned schema and the registry's `BACKWARD` compatibility mode refuses a
change existing consumers could not read. Commands:

| Command | Description |
| --- | --- |
| `schemas` | List subjects, latest version and field names |
| `compat [MODE] [subject]` | Show or set compatibility (BACKWARD, FORWARD, FULL, their _TRANSITIVE variants, NONE) |
| `evolve` | Apply `schema_changes.sql` (ADD / RENAME / DROP column) and show the new versions |
| `evolve-incompatible` | Try to register a required field without a default and show the 409 |
| `consume-avro` | Consume a topic with the Avro console consumer |
| `register json` | Register the legacy JSON-converter connector instead |

The measured walkthrough is in `SCHEMA_EVOLUTION.md`. Debezium Connect is
built from `./Dockerfile` because the upstream image does not ship the
Confluent Avro converter.

## Connector Configuration

The connector config is at `connectors/postgres-source.json`. Key settings:

- **`plugin.name: pgoutput`** — uses PostgreSQL's built-in logical decoding
- **`schema.include.list: impressions`** — only captures the impressions schema
- **`transforms.unwrap`** — flattens Debezium envelope to just the after-state, adding operation type and timestamp as metadata fields
- **`REPLICA IDENTITY FULL`** — set on all tables so update/delete events include the full row

## Generating CDC Events

After registering the connector, any DML against the source tables produces Kafka messages:

```bash
# Connect to the source database directly
psql -h localhost -p 5434 -U cdc_user -d cdc_source

# Or use the deploy script
./deploy.sh exec-sql sample_changes.sql
```

The `sample_changes.sql` file includes inserts, updates, and deletes to demonstrate all CDC event types.

## File Structure

```
debezium_deployment/
├── connectors/
│   └── postgres-source.json   # Debezium connector configuration
├── deploy.sh                  # Deployment and management script
├── docker-compose.yaml        # Service definitions (shared network)
├── docker-compose.local.yaml  # Standalone network override
├── init_db.sql                # Source database schema and seed data
├── sample_changes.sql         # Sample DML for generating CDC events
└── README.md
```
