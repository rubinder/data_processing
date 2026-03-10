"""Execution mode enum and CLI argument helpers."""

import argparse
from enum import Enum


class Mode(str, Enum):
    LOCAL = "local"
    AWS = "aws"
    DATABRICKS = "databricks"


def add_mode_argument(parser: argparse.ArgumentParser) -> None:
    """Add --mode argument to an argument parser."""
    parser.add_argument(
        "--mode",
        type=str,
        choices=[m.value for m in Mode],
        default=Mode.LOCAL.value,
        help="Execution mode: local, aws, or databricks",
    )


def parse_mode(args: argparse.Namespace) -> Mode:
    """Parse mode from argparse namespace."""
    return Mode(args.mode)
