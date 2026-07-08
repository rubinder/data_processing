"""DuckDB connection management and schema initialisation.

DuckDB runs in-process (embedded), so there is no server to connect to: the
connection object *is* the database engine. A file-backed database persists to
disk; ``:memory:`` keeps everything in RAM (used by tests).
"""
import duckdb

DEFAULT_DB_PATH = "impressions.duckdb"


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Return an embedded DuckDB connection.

    :param db_path: Path to the database file. ``None`` uses the default
        file-backed database; pass ``":memory:"`` for an in-memory database.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH
    return duckdb.connect(db_path)


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the ``impressions`` table if it does not already exist.

    Mirrors the raw impressions schema used by the dbt deployment:
    page_type/hour/min/second are INTEGER, the rest are VARCHAR.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS impressions (
            user_id VARCHAR,
            impression_id VARCHAR,
            page_type INTEGER,
            date VARCHAR,
            hour INTEGER,
            min INTEGER,
            second INTEGER,
            event_type VARCHAR
        )
        """
    )
