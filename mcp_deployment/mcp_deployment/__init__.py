"""Governed agent access to the gold layer.

Four pieces, each small: least-privilege PostgreSQL roles per pipeline stage,
blue-green promotion into ``gold`` with a release log and rollback, a pgvector
catalog of every gold table, column and query template generated from the dbt
manifest, and an MCP server that lets an agent search that catalog and run a
pre-approved parameterized template -- never raw SQL. Not "we told it not
to": the server has no tool that takes SQL, and the role it connects as can
read nothing but gold.
"""
