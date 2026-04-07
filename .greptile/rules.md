# spark-rapids Code Review Rules

For full coding conventions, build commands, code examples, and
shim layer architecture, see `AGENTS.md` at the repo root.

## Review Checklist

- [ ] Resource leaks — AutoCloseable not closed on exception paths; use withResource/closeOnExcept/safeClose, never bare .close() (see AGENTS.md § Resource management)
- [ ] OOM retry — GPU-allocating code not in withRetry/withRetryNoSplit; retry function must be idempotent (see AGENTS.md § OOM retry)
- [ ] Data correctness — GPU vs CPU divergence: nulls, NaN, overflow, decimal precision/scale, TIMESTAMP_NTZ vs LTZ, empty result handling
- [ ] Shim consistency — shim change not adjusted across all Spark versions (see AGENTS.md § Shim Layer)
- [ ] Resource lifecycle — SpillableColumnarBatch used after close or without retry handling
- [ ] Performance — unnecessary host-device copies, redundant materializations, avoidable data serialization
- [ ] Concurrency — missing GpuSemaphore.acquireIfNecessary(context), nested locks without ordering
- [ ] Fallback gaps — new operator in GpuOverrides without fallback declaration or test
- [ ] Test quality — no GPU execution verification, hardcoded sleeps, unseeded random data; GPU resource cleanup in afterAll/afterEach
- [ ] Configuration — new RapidsConf without docs/defaults; should use .internal() if not user-visible; new features default off
- [ ] Magic numbers — unexplained numeric literals without named constants or comments
- [ ] Pre-merge CI gaps — only selected shims run unit tests; [databricks] needed for DB CI; feature-gated tests need explicit enable; limited Scala 2.13 coverage
- [ ] Upstream dependencies — SNAPSHOT changes from spark-rapids-jni/cudf may break; verify API usage against upstream repos
