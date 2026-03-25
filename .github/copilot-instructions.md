# GitHub Copilot Review Instructions for spark-rapids

## Project Context

spark-rapids is a GPU acceleration plugin for Apache Spark. It translates Spark SQL operations into GPU-accelerated equivalents using NVIDIA RAPIDS/cuDF. The project supports multiple Spark versions via a shim layer architecture. See `sql-plugin/src/main/` for the list of supported Spark version shims.

## Review Focus — CRITICAL Issues (must flag)

- **[C1] Resource leaks**: `ColumnarBatch`, `GpuColumnVector`,
  `DeviceMemoryBuffer`, and other `AutoCloseable` resources (GPU
  memory, host memory, etc.) not closed on exception paths. Must use
  `withResource()` or `closeOnExcept()` from `Arm.scala`, never bare
  `.close()`. For collections, use `safeClose`/`safeMap` from
  `RapidsPluginImplicits`.
- **[C2] OOM retry correctness**: GPU-allocating code not wrapped in
  `withRetry`/`withRetryNoSplit`. Retry functions that are not
  idempotent.
- **[C3] Data correctness**: GPU vs CPU result divergence, especially
  for nulls, NaN, overflow, empty datasets, decimal precision, and
  timestamp timezone handling.
- **[C4] Shim consistency**: Changes to a shim file that are not
  adjusted across all related Spark version shims as necessary.
- **[C5] Resource lifecycle**: `SpillableColumnarBatch` used after
  close, or closed without proper retry handling.

## Review Focus — HIGH Issues (should flag)

- **[H1] Performance**: Unnecessary host-device memory copies,
  missing spill support, exceeding 2GB per-partition shuffle limit.
- **[H2] Concurrency**: Missing `GpuSemaphore.acquireIfNecessary()`
  before GPU work, nested locks without ordering.
- **[H3] Fallback gaps**: New operators in `GpuOverrides` without
  fallback declarations or fallback tests.
- **[H4] Test quality**: Tests that don't verify GPU execution
  (missing `assert_gpu_and_cpu_are_equal_collect`), hardcoded sleeps,
  unseeded random data.
- **[H5] Configuration**: New `RapidsConf` keys without documentation
  or sensible defaults. New config keys should use `.internal()` if
  not user-visible. New features should default to off until
  sufficiently tested.

## Do NOT Comment On

- Code formatting or style — handled by scalastyle and pre-commit hooks
- Import ordering
- Naming preferences (unless a name is actively misleading)
- Suggestions to add comments or documentation to code that is self-explanatory
- Minor refactoring preferences

## Further Reference

For coding conventions, build commands, project structure, shim
layer architecture, and common patterns, see
[AGENTS.md](../AGENTS.md) — the single source of truth for all
AI-facing project documentation.
