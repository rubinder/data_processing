"""Trino over the repository's own stores.

One SQL engine, three catalogs: ``iceberg`` (the REST catalog on the S3 gateway that
``iceberg_deployment`` writes to), ``postgres_raw`` (the raw layer, as the
``ingestion`` role) and ``postgres_gold`` (the promoted gold schema, as
``mcp_reader``). The demonstrations are the ones federation is for: load a
lakehouse table straight from the warehouse's raw layer, compute the four
analyses over Iceberg, and reconcile them against dbt's gold tables in the
same statement; time travel through Trino's Iceberg syntax; and what does
and does not get pushed down into PostgreSQL.
"""
