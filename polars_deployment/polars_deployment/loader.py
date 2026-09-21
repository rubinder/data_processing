"""Load impression data from the web server API into the Parquet store.

Mirrors ``duckdb_deployment/app/loader.py`` in shape (one call per
``page_type/date/hour``, replace-then-write), but there is no row loop:
Polars reads the gzip CSV bytes straight into a typed DataFrame.
"""
import requests
import polars as pl

from polars_deployment.schema import COLUMNS, SCHEMA
from polars_deployment.store import ParquetStore


def parse_csv_gz(payload: bytes) -> pl.DataFrame:
    """Parse a gzip-compressed impression CSV into a typed DataFrame.

    ``read_csv`` detects the gzip magic bytes and decompresses on the fly;
    ``schema_overrides`` narrows the integer columns and parses ``date``.
    """
    return pl.read_csv(payload, schema_overrides=SCHEMA).select(COLUMNS)


def fetch_impressions(
    api_base_url: str, page_type: int, date: str, hour: int
) -> pl.DataFrame:
    """GET one partition from the API and return it as a DataFrame."""
    response = requests.get(
        f"{api_base_url}/impression",
        params={"page_type": page_type, "date": date, "hour": hour},
    )
    response.raise_for_status()
    return parse_csv_gz(response.content)


def load_impressions(
    store: ParquetStore,
    api_base_url: str,
    page_type: int,
    date: str,
    hour: int,
) -> int:
    """Fetch one (page_type, date, hour) partition and (re)write it.

    Returns the number of rows written.
    """
    print(f"Fetching data: page_type={page_type}, date={date}, hour={hour}")
    df = fetch_impressions(api_base_url, page_type, date, hour)
    print(f"  Received {df.height} rows")
    path = store.write_partition(df, page_type, date, hour)
    print(f"  Wrote {df.height} rows to {path}")
    return df.height
