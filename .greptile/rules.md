# spark-rapids Code Review Rules

## GPU Resource Management

Close all GPU resources (`ColumnVector`, `Table`, `DeviceMemoryBuffer`, `SpillableColumnarBatch`) via try-with-resources or close() in a finally block.

Prefer `SpillableColumnarBatch`/`SpillableBuffer` over raw device memory to enable spill-to-disk.

## Memory Safety

Enforce size limits on buffers, especially in `shuffle-plugin/`. The 2GB per-partition limit for shuffle must be respected.

Avoid unnecessary host-device memory copies. Keep data on GPU between consecutive operations.

## Null Safety

Null-check cuDF native objects and results from aggregations/joins that may be empty.

## Type Correctness

- Decimal precision/scale must not silently overflow; match Spark behavior for DECIMAL128
- Maintain `TIMESTAMP_NTZ` vs `TIMESTAMP_LTZ` distinction
- SQL expressions must handle: nulls, empty datasets, NaN/Infinity, overflow, empty strings

## Concurrency

- No nested locks without documented ordering
- Long-running operations (RPC, IPC, GPU kernels) need configurable timeouts

## GPU Operator Fallback

New operators must declare fallback in `GpuOverrides` with clear log messages for unsupported types.

## Spark Version Compatibility

Shim all Spark API interactions for supported versions. Shims live at `sql-plugin/src/main/spark{VERSION}/`. Avoid removed/relocated internal APIs.

## Iceberg

Code in `iceberg/` and `sql-plugin/src/**/iceberg/`. Handle schema evolution, partition spec changes, and merge-on-read delete files correctly.

## Delta Lake

Code in `delta-lake/`. Verify row-level tracking, commit protocol, and conflict resolution for DML operations.

## Test Quality

- Clean up GPU resources and SparkSession in afterAll/afterEach
- No hardcoded Thread.sleep, order-dependent tests, or unseeded random data
- Cover: nulls, empty batches, single-row, max-size batches, CPU/GPU fallback paths

## Pre-merge CI Coverage Gaps

Pre-merge only tests selected shims, not all supported versions. Check `premergeUT1.buildvers` and `premergeUT2.buildvers` in `pom.xml` for the current list. Changes to shims not covered by pre-merge may break in nightly. Reviewers should watch for:
- **Shim coverage**: Only a few shims run unit tests in pre-merge. Changes to other shim versions are only build-verified, not tested.
- **Databricks**: Only triggered when files match `sql-plugin/src/main/.*[0-9x-]db/` or `databricks` in path. Shared code affecting Databricks needs `[databricks]` in PR title.
- **Fuzz tests, large data tests, mortgage benchmarks**: Nightly only.
- **Feature-gated tests** (`@delta_lake`, `@iceberg`, `@cudf_udf`): Only run when explicitly enabled; may pass pre-merge but fail nightly.
- **Scala 2.13**: Pre-merge runs unit tests on limited shims and integration tests against a single Spark version.

## Core Upstream Dependencies

spark-rapids depends on nightly SNAPSHOT artifacts from several upstream repos. Code changes here may be affected by concurrent upstream changes that are not visible in this repo's PRs:
- **spark-rapids-jni / cudf**: Native JNI layer and GPU compute library. Changes to cuDF operator semantics, memory management, or JNI APIs upstream can break code here without any local change.
- **spark-rapids-private**: Proprietary extensions built from a separate repo. API contracts between private and public code must stay in sync.
- **spark-rapids-hybrid (Gluten)**: Hybrid CPU/GPU execution bridge. Upstream Gluten changes can break the hybrid execution path.

When reviewing code that touches these integration boundaries (JNI calls, private API interfaces, hybrid execution paths), consider whether the code depends on upstream behavior that may be changing concurrently.

## Build

Verify dependency version alignment across all build profiles and shaded JAR consistency.
