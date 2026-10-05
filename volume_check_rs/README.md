# volume_check_rs: PyO3 Case Study

A PyO3 proof-of-concept that demonstrates **when PyO3 optimization makes sense—and when it doesn't**.

## What it does

Reimplements the volume anomaly detection logic from `spark_applications/utils/quality.py:check_volume()` in Rust. The function compares current row counts to historical baselines (median of same hour on prior days) and flags anomalies if the ratio falls outside [min_ratio, max_ratio].

## Benchmark Results

| Scenario | Python | Rust | Ratio | Insight |
|----------|--------|------|-------|---------|
| 7-day baseline | 0.92 µs | 2.32 µs | 0.4x | PyO3 FFI overhead exceeds computation |
| 30-day baseline | 0.59 µs | 2.99 µs | 0.2x | Worse with larger datasets |
| 365-day baseline | 1.40 µs | 20.79 µs | 0.1x | Rust median slower on large arrays |
| Zero rows (early exit) | 0.20 µs | 1.22 µs | 0.2x | Constant overhead regardless of logic |

**Conclusion: Not worth the overhead for this operation.**

## Why Rust Lost

1. **PyO3 FFI boundary cost (~1-2 µs)**: Crossing from Python → Rust costs more than the entire median calculation.
2. **Python's `statistics.median()` is already C-optimized**: The pure Python version just calls a fast C implementation.
3. **Operation is too small**: Rust's advantage only shows when compute time >> boundary crossing cost.
4. **Data sizes are tiny (7-365 ints)**: Sorting 365 integers is microseconds in any language.

## When PyO3 Actually Wins

PyO3 shines when:
- **High-volume operations**: Process millions of rows through the function (boundary cost amortized)
- **Complex algorithms**: Numerical compute, cryptography, compression (seconds per call, not microseconds)
- **String/buffer manipulation**: Rust's zero-copy ownership is faster than Python's copy-heavy model
- **Vectorized operations**: pandas_udf that processes Arrow batches (not row-at-a-time UDFs)

## Lessons Learned

### ✅ What Worked
- Maturin project setup was straightforward
- PyO3 API is clean for simple functions
- Benchmark harness revealed the problem immediately

### ❌ What Didn't Work
- Assumed volume check would benefit from Rust (wrong assumption)
- Didn't profile Python baseline first (we should have; we would've seen it was already < 1 µs)
- FFI overhead is real and non-negotiable; you can't optimize it away

### 🎓 Design Insight
**Start with profiling, not implementation.** The volume check was never slow (< 1 µs is plenty for a once-per-batch check). The real bottleneck in the landing job is likely the API fetch timeout, the CSV parse, or the S3 write—not the validation logic.

## Code Structure

```
volume_check_rs/
├── src/lib.rs          # Rust implementation: check_volume() + VolumeCheck dataclass
├── Cargo.toml          # Rust dependencies (pyo3)
├── pyproject.toml      # Python package metadata
└── bench_check_volume.py  # Benchmark: Python vs Rust
```

## Building

```bash
cd volume_check_rs
maturin develop  # Builds the .so and installs as editable Python package
python bench_check_volume.py  # Run benchmarks
```

## Usage (if you wanted to use it)

```python
from volume_check_rs import check_volume

result = check_volume(
    current=100_000,
    baselines=[95_000, 98_000, 102_000],
    min_ratio=0.5,
    max_ratio=2.0
)

print(result.status)     # "ok" | "anomaly" | "no_baseline"
print(result.reason)     # Description of anomaly, if any
print(result.as_fields())  # Dict for logging
```

## Takeaway for Future Work

**Don't optimize without measuring.** This repo should:
1. ✅ Keep this module as a reference for "when not to use PyO3"
2. ✅ Profile the actual landing job to find real bottlenecks (likely I/O, not validation logic)
3. ✅ If a bottleneck is found, apply PyO3 to high-volume string/numeric code (not validation conditionals)
4. Consider **pandas_udf** for PySpark if masking/transformation is needed on millions of rows

## References

- [PyO3 Performance Guide](https://pyo3.rs/latest/performance.html)
- Original: `spark_applications/utils/quality.py:check_volume()`
- Benchmark framework: `bench_check_volume.py`
