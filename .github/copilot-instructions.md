# GitHub Copilot Review Instructions for spark-rapids

spark-rapids is a GPU acceleration plugin for Apache Spark.
For full project context, coding conventions, and code examples,
see [AGENTS.md](../AGENTS.md).

## Review Checklist — CRITICAL (must flag)

- [ ] C1: Resource leaks — AutoCloseable not closed on exception paths (see AGENTS.md § Resource management)
- [ ] C2: OOM retry — GPU-allocating code not in withRetry/withRetryNoSplit (see AGENTS.md § OOM retry)
- [ ] C3: Data correctness — GPU vs CPU divergence (nulls, NaN, overflow, decimals, timestamps)
- [ ] C4: Shim consistency — shim change not adjusted across all Spark versions (see AGENTS.md § Shim Layer)
- [ ] C5: Resource lifecycle — SpillableColumnarBatch used after close or without retry handling

## Review Checklist — HIGH (should flag)

- [ ] H1: Performance — unnecessary host-device copies, missing spill support, >2GB partition shuffle
- [ ] H2: Concurrency — missing GpuSemaphore.acquireIfNecessary(context), nested locks without ordering
- [ ] H3: Fallback gaps — new operator in GpuOverrides without fallback declaration or test
- [ ] H4: Test quality — no GPU execution verification, hardcoded sleeps, unseeded random data
- [ ] H5: Configuration — new RapidsConf without docs/defaults; should use .internal() if not user-visible; new features default off
- [ ] H6: Magic numbers — unexplained numeric literals without named constants or comments
- [ ] H7: Idiomatic Scala — explicit return, Java-style loops instead of functional combinators

## Do NOT Comment On

- Code formatting or import ordering (handled by scalastyle + pre-commit)
- Naming preferences (unless actively misleading)
- Self-explanatory documentation suggestions
- Minor refactoring preferences
