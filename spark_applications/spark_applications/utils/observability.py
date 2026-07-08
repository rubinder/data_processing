"""Structured logging and metric emission.

``print`` statements are invisible to log aggregation and carry no structure.
These helpers emit single-line JSON so a log pipeline can index fields
(job, stage, row counts, duration) and alert on them. No external deps —
just stdlib ``logging`` + ``json``.
"""

import json
import logging
import time
from contextlib import contextmanager


def get_logger(name: str) -> logging.Logger:
    """Return a logger that emits to stdout once (no duplicate handlers)."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def event_payload(event: str, **fields) -> str:
    """Build a single-line JSON log payload (pure -> easy to unit test)."""
    record = {"event": event}
    record.update(fields)
    return json.dumps(record, sort_keys=True, default=str)


def log_event(logger: logging.Logger, event: str, **fields) -> None:
    """Emit a structured event line."""
    logger.info(event_payload(event, **fields))


def log_metrics(logger: logging.Logger, job: str, **metrics) -> None:
    """Emit a metrics event (rows in/out, durations, bytes, ...)."""
    log_event(logger, "metrics", job=job, **metrics)


@contextmanager
def timed(logger: logging.Logger, job: str, stage: str):
    """Time a stage and emit its duration as a metric on exit."""
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        log_metrics(
            logger, job, stage=stage, duration_seconds=round(elapsed, 3)
        )
