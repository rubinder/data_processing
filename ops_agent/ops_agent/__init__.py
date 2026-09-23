"""Ops agent: data contracts, monitors, arrival SLAs and a LangGraph agent
over the Iceberg impression tables built by ``iceberg_deployment``.

Ported from a standalone lakehouse project and pointed at this repository's
event model. The agent is rules-first and dry-run by default; an LLM is only
consulted for findings no rule matches, and never in the tests.
"""
