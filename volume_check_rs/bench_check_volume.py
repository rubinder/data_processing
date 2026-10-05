#!/usr/bin/env python
"""Benchmark: volume_check_rs (Rust) vs pure Python implementation."""

import time
import statistics
from dataclasses import dataclass
from typing import Callable

import volume_check_rs


# Pure Python implementation (from spark_applications/utils/quality.py)
@dataclass
class PythonVolumeCheck:
    status: str
    current: int
    baseline: float | None
    ratio: float | None
    reason: str | None = None

    def as_fields(self) -> dict:
        return {
            "volume_status": self.status,
            "volume_current": self.current,
            "volume_baseline": self.baseline,
            "volume_ratio": self.ratio,
            "volume_reason": self.reason,
        }


def python_check_volume(
    current: int,
    baselines: list[int],
    *,
    min_ratio: float = 0.5,
    max_ratio: float = 2.0,
) -> PythonVolumeCheck:
    """Original pure Python implementation."""
    if current == 0:
        return PythonVolumeCheck(
            status="anomaly",
            current=0,
            baseline=None,
            ratio=None,
            reason="zero rows landed",
        )
    if not baselines:
        return PythonVolumeCheck(
            status="no_baseline",
            current=current,
            baseline=None,
            ratio=None,
        )

    baseline = float(statistics.median(baselines))
    if baseline == 0:
        return PythonVolumeCheck(
            status="no_baseline",
            current=current,
            baseline=0.0,
            ratio=None,
            reason="baseline median is zero",
        )

    ratio = current / baseline
    if ratio < min_ratio:
        reason = (
            f"{current} rows is {ratio:.0%} of the baseline {baseline:.0f}, "
            f"below the {min_ratio:.0%} floor"
        )
        return PythonVolumeCheck("anomaly", current, baseline, ratio, reason)
    if ratio > max_ratio:
        reason = (
            f"{current} rows is {ratio:.0%} of the baseline {baseline:.0f}, "
            f"above the {max_ratio:.0%} ceiling"
        )
        return PythonVolumeCheck("anomaly", current, baseline, ratio, reason)
    return PythonVolumeCheck("ok", current, baseline, ratio)


def benchmark(
    func: Callable, name: str, iterations: int, current: int, baselines: list[int]
) -> tuple[float, any]:
    """Measure function execution time over iterations."""
    start = time.perf_counter()
    result = None
    for _ in range(iterations):
        result = func(current, baselines)
    elapsed = time.perf_counter() - start
    avg_us = (elapsed / iterations) * 1_000_000
    print(f"{name:30s} {avg_us:8.2f} µs   (total: {elapsed:.3f}s for {iterations} runs)")
    return avg_us, result


def verify_equivalence(py_result: PythonVolumeCheck, rs_result):
    """Verify Rust and Python results are equivalent."""
    assert py_result.status == rs_result.status, f"status mismatch: {py_result.status} vs {rs_result.status}"
    assert py_result.current == rs_result.current, f"current mismatch"
    assert py_result.baseline == rs_result.baseline, f"baseline mismatch"
    assert py_result.ratio == rs_result.ratio, f"ratio mismatch"
    assert py_result.reason == rs_result.reason, f"reason mismatch"


if __name__ == "__main__":
    # Test cases: different baseline sizes that are realistic
    test_cases = [
        ("Small baseline (7 days)", 100_000, list(range(95_000, 105_000, 1_500))),
        ("Medium baseline (30 days)", 500_000, list(range(450_000, 550_000, 3_333))),
        ("Large baseline (365 days)", 1_000_000, list(range(950_000, 1_050_000, 275))),
        ("Zero rows", 0, list(range(100_000, 110_000, 1_000))),
        ("No baseline", 100_000, []),
    ]

    print("\n" + "=" * 80)
    print("VOLUME CHECK BENCHMARK: Rust vs Python")
    print("=" * 80)

    for test_name, current, baselines in test_cases:
        print(f"\n{test_name}")
        print(f"  current={current:,}, baseline_size={len(baselines)}")
        print("-" * 80)

        # Verify equivalence first
        py_result = python_check_volume(current, baselines)
        rs_result = volume_check_rs.check_volume(current, baselines)
        verify_equivalence(py_result, rs_result)

        # Run benchmarks (scale iterations based on baseline size)
        iterations = 10_000 if len(baselines) < 50 else 1_000
        py_avg, _ = benchmark(python_check_volume, "Python", iterations, current, baselines)
        rs_avg, _ = benchmark(
            lambda c, b: volume_check_rs.check_volume(c, b),
            "Rust (PyO3)",
            iterations,
            current,
            baselines,
        )

        speedup = py_avg / rs_avg
        print(f"\n  Speedup: {speedup:.1f}x faster ({py_avg:.2f} → {rs_avg:.2f} µs)")
        print()
