# GitHub Copilot Review Instructions for spark-rapids

## Project Context

spark-rapids is a GPU acceleration plugin for Apache Spark. It translates Spark SQL operations into GPU-accelerated equivalents using NVIDIA RAPIDS/cuDF. The project supports multiple Spark versions (3.2.x through 4.0.x) via a shim layer architecture.

## Review Focus — CRITICAL Issues (must flag)

- **GPU memory leaks**: `ColumnarBatch`, `GpuColumnVector`, `DeviceMemoryBuffer` not closed on exception paths. Must use `withResource()` or `closeOnExcept()` from `Arm.scala`, never bare `.close()`.
- **OOM retry correctness**: GPU-allocating code not wrapped in `withRetry`/`withRetryNoSplit`. Retry functions that are not idempotent.
- **Data correctness**: GPU vs CPU result divergence, especially for nulls, NaN, overflow, empty datasets, decimal precision, and timestamp timezone handling.
- **Shim consistency**: Changes to a shim file that are not replicated across all related Spark version shims.
- **Resource lifecycle**: `SpillableColumnarBatch` used after close, or closed without proper retry handling.

## Review Focus — HIGH Issues (should flag)

- **Performance**: Unnecessary host-device memory copies, missing spill support, exceeding 2GB per-partition shuffle limit.
- **Concurrency**: Missing `GpuSemaphore.acquireIfNecessary()` before GPU work, nested locks without ordering.
- **Fallback gaps**: New operators in `GpuOverrides` without fallback declarations or fallback tests.
- **Test quality**: Tests that don't verify GPU execution (missing `assert_gpu_and_cpu_are_equal_collect`), hardcoded sleeps, unseeded random data.
- **Configuration**: New `RapidsConf` keys without documentation or sensible defaults.

## Do NOT Comment On

- Code formatting or style — handled by scalastyle and pre-commit hooks
- Import ordering
- Naming preferences (unless a name is actively misleading)
- Suggestions to add comments or documentation to code that is self-explanatory
- Minor refactoring preferences

## Key Files Reference

| File | Purpose |
|------|---------|
| `GpuOverrides.scala` | GPU operator registry and fallback rules |
| `RapidsConf.scala` | Configuration keys and defaults |
| `Arm.scala` | Resource management patterns (withResource/closeOnExcept) |
| `RmmRapidsRetryIterator.scala` | OOM retry framework |
| `SpillableColumnarBatch.scala` | Spillable GPU batch wrapper |
| `GpuSemaphore.scala` | GPU access semaphore |
| `integration_tests/.../asserts.py` | GPU vs CPU comparison test assertions |

## Build & Test

```bash
mvn clean verify -DskipTests           # Build
mvn test -pl tests                      # Unit tests
mvn test -pl tests -Dbuildver=341       # Tests for Spark 3.4.1
cd integration_tests && pytest -v       # Integration tests
```
