# spark-rapids Code Review Rules

For full coding conventions, build commands, code examples, and
shim layer architecture, see `AGENTS.md` at the repo root.

## Review Checklist

- [ ] Resource management — use withResource/closeOnExcept/safeClose, never bare .close() (see AGENTS.md § Resource management)
- [ ] OOM retry — GPU-allocating code in withRetry/withRetryNoSplit, idempotent retry function (see AGENTS.md § OOM retry)
- [ ] Reliability — >2GB partition shuffle and missing spill support cause crashes (not just perf); enforce size limits and spill paths in GPU-resident data
- [ ] Memory & type safety — correct decimal precision, TIMESTAMP_NTZ vs LTZ, null/NaN/empty handling
- [ ] Performance — no unnecessary host-device copies, redundant materializations, or avoidable data serialization
- [ ] Concurrency — GpuSemaphore.acquireIfNecessary(context) before GPU work, no undocumented nested locks
- [ ] Operator fallback — new operators declare fallback in GpuOverrides with tests using assert_gpu_fallback_collect
- [ ] Shim consistency — changes adjusted across all Spark version shims (see AGENTS.md § Shim Layer Architecture)
- [ ] Configuration — new RapidsConf keys have docs, sensible defaults, .internal() if not user-visible, new features default off
- [ ] Performance — benchmark matches change scope: micro for operators, NDS/TPC-DS for framework; reasonable test settings; multiple runs with variance
- [ ] Test quality — GPU resource cleanup in afterAll/afterEach, no hardcoded sleeps, use assert_gpu_and_cpu_are_equal_collect
- [ ] Pre-merge CI gaps — only selected shims run unit tests; [databricks] needed for DB CI; feature-gated tests need explicit enable; limited Scala 2.13 coverage
- [ ] Upstream dependencies — SNAPSHOT changes from spark-rapids-jni/cudf may break; verify API usage against upstream repos
