# GitHub Copilot Review Instructions for spark-rapids

spark-rapids is a GPU acceleration plugin for Apache Spark.
For full project context, coding conventions, and code examples,
see [AGENTS.md](../AGENTS.md).

## Cross-repo References

When reviewing shim code or GPU operator implementations, cross-reference
these upstream repositories to verify correctness:
- **apache/spark** — verify GPU behavior matches the CPU implementation in the target Spark version (expression evaluation semantics, null handling, type coercion, catalog/partition behavior)
- **rapidsai/cudf** — verify cuDF API usage (column operations, memory allocation semantics, null handling)
- **NVIDIA/spark-rapids-jni** — verify JNI binding signatures, memory ownership rules, error codes

## Review Checklist — CRITICAL (must flag)

- [ ] C1: Resource leaks — AutoCloseable not closed on exception paths (see AGENTS.md § Resource management)
- [ ] C2: OOM retry — GPU-allocating code not in withRetry/withRetryNoSplit (see AGENTS.md § OOM retry)
- [ ] C3: Data correctness — GPU vs CPU divergence (nulls, NaN, overflow, decimals, timestamps)
- [ ] C4: Shim consistency — shim change not adjusted across all Spark versions (see AGENTS.md § Shim Layer)
- [ ] C5: Resource lifecycle — SpillableColumnarBatch used after close or without retry handling
- [ ] C6: Reliability — >2GB partition shuffle without size enforcement, missing spill support in GPU-resident data paths (both cause crashes)

## Review Checklist — HIGH (should flag)

- [ ] H1: Performance — unnecessary host-device copies, redundant materializations, avoidable data serialization
- [ ] H2: Concurrency — missing GpuSemaphore.acquireIfNecessary(context), nested locks without ordering
- [ ] H3: Fallback gaps — new operator in GpuOverrides without fallback declaration or test
- [ ] H4: Test quality — no GPU execution verification, hardcoded sleeps, unseeded random data
- [ ] H5: Configuration — new RapidsConf without docs/defaults; should use .internal() if not user-visible; new features default off
- [ ] H6: Magic numbers — unexplained numeric literals without named constants or comments

## Do NOT Comment On

- Code formatting or import ordering (handled by scalastyle + pre-commit)
- Naming preferences (unless actively misleading)
- Self-explanatory documentation suggestions
- Minor refactoring preferences
