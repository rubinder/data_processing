# Spark Applications

PySpark data processing jobs that run across local, AWS, and Databricks environments.

## Applications

- **hello_world.py** - Simple hello world Spark job that creates a DataFrame and prints a total count. Used for validating Spark connectivity.
- **salted_join.py** - Demonstrates salted joins to mitigate data skew. Adds a random salt column (0-9) to skewed keys, explodes the smaller DataFrame to match, performs the join, then removes the salt to restore the original structure.
- **api_pull.py** - Pulls impression data from the FastAPI web server (see `web_server_code`), updates a status tracking table (DynamoDB/Delta/local depending on mode), saves raw data to storage, reads it back, and writes it as a partitioned table (partitioned by `page_type`, `date`, `hour`).
- **aggregation.py** - Reads impression data for a given date and hour and aggregates it by `user_id`, `impression_id`, `page_type`.

## Utilities

The `utils/` package provides shared functionality:
- `mode.py` - Defines execution modes (local, AWS, Databricks) and CLI argument helpers
- `session.py` - Creates SparkSession instances configured for the target mode
- `storage.py` - Storage adapter abstraction for local filesystem, S3, and DBFS

## Prerequisites

- Python 3.10+
- uv (Python package manager)

## How to Run

```bash
# Install dependencies
uv sync

# Install with dev dependencies (for testing)
uv sync --extra dev

# Run a job locally
uv run python -m spark_applications.hello_world

# Run api_pull with parameters
uv run python -m spark_applications.api_pull --mode local --page_type 1 --date 2026-01-01 --hour 10

# Run aggregation with parameters
uv run python -m spark_applications.aggregation --mode local --date 2026-01-01 --hour 10
```

## Tests

Unit tests are located in the `tests/` directory and are not packaged with the application code.

```bash
# Run all tests
uv run pytest

# Run a specific test
uv run pytest tests/test_hello_world.py
```

## Dependencies

- pyspark 3.5.4
- boto3 (for AWS mode)
- requests (for API calls)
- python-dotenv (for .env loading)
- delta-spark (for Databricks Delta tables)
