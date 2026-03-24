# spark-rapids Code Review Rules

## GPU Resource Management (ARM Pattern)

Use the project's ARM pattern from `Arm.scala` — never bare `.close()`:
- `withResource(r)(block)` — always closes `r` in finally, even on success
- `closeOnExcept(r)(block)` — closes `r` only if an exception occurs (use when caller owns the resource on success)
- `safeClose`, `safeMap`, `safeConsume` from `RapidsPluginImplicits` for collections

Prefer `SpillableColumnarBatch`/`SpillableBuffer` over raw device memory to enable spill-to-disk.

## OOM Retry Handling

GPU memory-allocating code must use retry handlers from `RmmRapidsRetryIterator.scala`:
- `withRetry(input, splitPolicy)(fn)` — retries on OOM, optionally splitting input
- `withRetryNoSplit(input)(fn)` — retries without splitting
- The retry function must be idempotent
- Input should be `SpillableColumnarBatch` before entering retry

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
- `GpuSemaphore.acquireIfNecessary(context)` must be called before GPU work; release with `releaseIfNecessary`

## GPU Operator Fallback

New operators must declare fallback in `GpuOverrides` with clear log messages for unsupported types.

CPU fallback declarations must:
- Include a test that verifies the fallback triggers correctly
- Log at INFO level with a message explaining why the fallback occurred
- Be covered by integration tests using `assert_gpu_fallback_collect`

## Spark Version Compatibility

Shim all Spark API interactions for supported versions. Shims live at `sql-plugin/src/main/spark{VERSION}/`. Avoid removed/relocated internal APIs.

Every shim-specific source file must have a `spark-rapids-shim-json-lines` comment block after the copyright header:
```
/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "340"}
spark-rapids-shim-json-lines ***/
```
The canonical location for a file shared by multiple shims is `src/main/<lowest_buildver>/`.

When modifying a shim file, verify that all related shim versions are updated consistently. Pre-merge CI only tests selected shims — inconsistencies in untested shims will only surface in nightly builds.

## Iceberg

Code in `iceberg/` and `sql-plugin/src/**/iceberg/**`. Handle schema evolution, partition spec changes, metadata consistency, and merge-on-read delete files correctly.

## Delta Lake

Code in `delta-lake/` and `sql-plugin/src/**/delta/`. Verify row-level tracking, commit protocol, and conflict resolution for DML operations.

## Configuration Changes

New `RapidsConf` keys must:
- Have clear documentation strings explaining purpose and impact
- Include sensible defaults that match existing behavior
- Be mentioned in the PR description

## Performance Validation

Choose the right benchmark strategy based on the scope of the change:

| Change Type | Recommended Benchmark |
|---|---|
| Specific expression or operator (e.g., LIKE, CAST) | Targeted micro-benchmark with representative data |
| Columnar processing or memory layout | Columnar micro-benchmark (e.g., ColumnarToRow) |
| Framework-level change (scheduling, shuffle, spill) | Real-world benchmark (NDS / TPC-DS) |
| Micro-benchmark shows regression | Escalate to real-world benchmark for context |

Guidelines:
- Do NOT use NDS to validate expression-level changes — NDS may not exercise the affected code path sufficiently (e.g., NDS has very few LIKE operations)
- Distinguish planning-time vs runtime performance impact and measure each appropriately
- Run benchmarks multiple times to confirm results are outside noise range
- Cover edge cases: empty tables, skewed partitions, max-size batches
- Report absolute numbers and relative delta with variance

## Test Quality

- Clean up GPU resources and SparkSession in afterAll/afterEach
- No hardcoded Thread.sleep, order-dependent tests, or unseeded random data
- Cover: nulls, empty batches, single-row, max-size batches, CPU/GPU fallback paths
- Integration tests must verify GPU execution using `assert_gpu_and_cpu_are_equal_collect` or `assert_gpu_fallback_collect` — do not write tests that silently run on CPU only

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

Changes to upstream SNAPSHOT dependency versions must be highlighted in the PR description with rationale.

### Cross-repo Review Guidance (cuDF / spark-rapids-jni)

The `patternRepositories` config enables cross-repo context lookup. When reviewing code that calls into cuDF or JNI, verify:

- **cuDF column API usage**: Check `rapidsai/cudf` for the correct function signatures, null handling semantics, and ownership transfer rules. cuDF column operations may have different null propagation than Spark SQL expects.
- **JNI memory ownership**: When a JNI call returns a native pointer (e.g., `ColumnVector` from `spark-rapids-jni`), verify whether the caller or callee owns the memory. Double-free and use-after-free are common bugs at this boundary.
- **cuDF kernel semantics**: GPU kernel behavior for edge cases (NaN, overflow, empty input) may differ from Spark CPU behavior. Cross-reference the cuDF implementation when the spark-rapids code relies on specific cuDF behavior for correctness.
- **API version alignment**: cuDF and spark-rapids-jni are SNAPSHOT dependencies. If a PR depends on a newly added upstream API, flag whether the upstream change has been merged or is still in-flight.

## Build

Verify dependency version alignment across all build profiles and shaded JAR consistency.
