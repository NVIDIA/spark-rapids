# spark-rapids Code Review Rules

For full coding conventions, build commands, project structure,
and shim layer architecture, see [AGENTS.md](../AGENTS.md) — the
single source of truth for all AI-facing project documentation.

The rules below are concise review-time reminders. AGENTS.md has
the authoritative details and code examples.

## GPU Resource Management (ARM Pattern)

Use the ARM pattern from `Arm.scala` — never bare `.close()`:
- `withResource(r)(block)` — always closes `r` in finally
- `closeOnExcept(r)(block)` — closes only on exception
- `safeClose`, `safeMap` from `RapidsPluginImplicits` for
  collections

## OOM Retry Handling

GPU-allocating code must use retry handlers from
`RmmRapidsRetryIterator.scala`:
- `withRetry` / `withRetryNoSplit` — retry function must be
  idempotent
- Input should be `SpillableColumnarBatch` before entering retry

## Memory & Type Safety

- Enforce 2GB per-partition shuffle limit
- Avoid unnecessary host-device copies
- Decimal precision/scale must match Spark behavior
- Maintain `TIMESTAMP_NTZ` vs `TIMESTAMP_LTZ` distinction
- Handle nulls, NaN, empty datasets in all expressions

## Concurrency

- `GpuSemaphore.acquireIfNecessary()` before GPU work
- No nested locks without documented ordering

## GPU Operator Fallback

- New operators must declare fallback in `GpuOverrides`
- Fallback tests required using `assert_gpu_fallback_collect`

## Spark Version Compatibility

See `AGENTS.md` "Shim Layer Architecture" for full details on
`spark-rapids-shim-json-lines` annotations, directory conventions,
Databricks shims, Scala 2.12/2.13, and Delta Lake modules.

When modifying a shim, verify all related versions are updated.
Pre-merge CI only tests selected shims — inconsistencies in
untested shims surface only in nightly builds.

## Configuration Changes

New `RapidsConf` keys must have documentation, sensible defaults,
and be mentioned in the PR description. Use `.internal()` for
non-user-visible keys. New features default to off.

## Performance Validation

Choose benchmark strategy by change scope:

| Change Type | Benchmark |
|---|---|
| Specific expression/operator | Targeted micro-benchmark |
| Columnar processing / memory | Columnar micro-benchmark |
| Framework-level (shuffle, spill) | Real-world (NDS/TPC-DS) |

- Verify test settings are reasonable: GPU type, cluster size,
  data size, distribution, and query
- Run multiple times to confirm outside noise range
- Report absolute numbers + relative delta with variance

## Test Quality

- Clean up GPU resources in afterAll/afterEach
- No hardcoded sleeps, order-dependent tests, unseeded random data
- Cover: nulls, empty batches, single-row, max-size, fallback
- Use `assert_gpu_and_cpu_are_equal_collect` — not CPU-only tests

## Pre-merge CI Coverage Gaps

- Only selected shims run unit tests; others are build-only
- Databricks CI needs `[databricks]` in PR title
- Feature-gated tests (`@delta_lake`, `@iceberg`, `@cudf_udf`)
  only run when explicitly enabled
- Scala 2.13 coverage is limited in pre-merge

## Core Upstream Dependencies

spark-rapids depends on SNAPSHOT artifacts from upstream repos
(spark-rapids-jni/cudf, spark-rapids-private, Gluten). Changes
here may break due to concurrent upstream changes. See AGENTS.md
for cross-repo review guidance.
