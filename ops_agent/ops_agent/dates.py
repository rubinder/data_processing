"""Defensive date parsing for values the sensor cannot trust.

Asking SQL for ``MAX()`` over a raw column and parsing the answer is actively
dangerous: a garbage value that sorts above every real date becomes the
``MAX()``, and parsing it raises. That turns a data-quality problem into a
crashed process, which is the worst failure mode available -- the sensor
dies exactly when the data goes bad, and reports a stack trace instead of a
finding. ``max_parseable_date`` parses every candidate, keeps the max of the
ones that parse, and counts the rest so the caller can surface them.
"""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date


def to_date(value) -> date:
    """Coerce one raw value to a ``date``. Raises on anything that is not one."""
    if isinstance(value, str):
        return date.fromisoformat(value[:10]) if len(value) > 10 and value[10] in "T " \
            else date.fromisoformat(value)
    return value.date() if hasattr(value, "date") else value


def max_parseable_date(values: Iterable) -> tuple[date | None, int]:
    """``(newest parseable date or None, count of non-null values that did not parse)``.

    Never raises. Nulls are skipped and not counted -- a NULL is an absent
    value, not a corrupt one.
    """
    newest: date | None = None
    unparseable = 0
    for value in values:
        if value is None:
            continue
        try:
            parsed = to_date(value)
        except (ValueError, TypeError):
            unparseable += 1
            continue
        if not isinstance(parsed, date):
            unparseable += 1
            continue
        if newest is None or parsed > newest:
            newest = parsed
    return newest, unparseable
