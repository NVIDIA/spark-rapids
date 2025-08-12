# Plan: Port OptimizeTableCommand to Spark RAPIDS for Delta Lake 3.3.x (delta-33x)

This plan describes how to add GPU support for the Delta SQL command `OPTIMIZE` (Compaction mode) in the Spark RAPIDS plugin for Apache Spark targeting the Delta Lake 3.3.x shim (`delta-33x`). The outcome is a new GPU command implementation and metadata rule that leverages the existing GPU optimize executor, plus end-to-end integration tests validating parity with CPU.

Scope

- In-scope
  - Implement a GPU runnable command for `OPTIMIZE` compaction only.
  - Add a corresponding GPU meta (tagging/conversion) rule and register it in the 33x provider.
  - Reuse the existing common 33x optimize executor for actual file binning/writing/commit.
  - Add integration tests that exercise `OPTIMIZE` and compare CPU vs GPU results and metrics.
- Out-of-scope
  - Z-Ordering or Liquid Clustering optimizations.
  - Writing Deletion Vectors (DVs) during optimize. DV-writing is not supported on GPU today and must be tagged unsupported to fall back to CPU.
  - Non-33x shims (Databricks variants are separate and already have their own executors if needed).

Background and Existing Building Blocks

- Common 33x GPU Optimize executor (already implemented):
  - `delta-lake/common/src/main/delta-33x/scala/org/apache/spark/sql/delta/rapids/commands/GpuOptimizeExecutor.scala`
  - Handles candidate selection, bin-packing, running per-bin optimize jobs, commit w/ retry, DV checks, and metrics.
- Auto-Compaction hook (for reference):
  - `delta-lake/common/src/main/delta-33x/scala/org/apache/spark/sql/delta/hooks/GpuAutoCompact.scala`
  - Constructs a `DeltaOptimizeContext` and calls `GpuOptimizeExecutor.optimize()`.
- 33x GPU transaction and provider:
  - `delta-lake/delta-33x/src/main/scala/org/apache/spark/sql/delta/rapids/delta33x/GpuOptimisticTransaction.scala`
  - `delta-lake/delta-33x/src/main/scala/com/nvidia/spark/rapids/delta/delta33x/Delta33xProvider.scala`
- Patterns to mirror (GPU RunnableCommand + Meta + Provider registration):
  - Delete: `GpuDeleteCommand.scala`, `DeleteCommandMeta.scala`
  - Update: `GpuUpdateCommand.scala`, `UpdateCommandMeta.scala`

High-Level Design

- Add a new GPU command class that mirrors the CPU `OptimizeTableCommand` signature/behavior for Delta 3.3.x where feasible.
- The GPU command will:
  - Parse the target Delta table and options from the CPU command wrapper.
  - Construct a `DeltaOptimizeContext` (similar to Auto-Compact) reflecting compaction parameters (e.g., min/max file size, optional partition predicates).
  - Run `GpuOptimizeExecutor.optimize(spark, log, txn, context, strategy)` with a Compaction strategy.
  - Return the same schema/rows as the CPU command (history/metrics are recorded via commit; the command itself may also return a summary as Rows depending on OSS 3.3 behavior).
- Add a Meta class to:
  - Detect unsupported modes (Z-Order, Liquid Clustering, or any DV-writing path) and tag them so the planner falls back to CPU.
  - Invoke RAPIDS Delta write tagging utilities to ensure GPU write compatibility for the current table and session settings.
  - Convert to the `GpuOptimizeTableCommand` instance on acceptance.
- Register the new Meta in `Delta33xProvider.getRunnableCommandRules` so the query planner can match and replace the CPU command.

New Files to Add

- GPU command implementation
  - Path: `delta-lake/delta-33x/src/main/scala/org/apache/spark/sql/delta/rapids/delta33x/GpuOptimizeTableCommand.scala`
  - Purpose: Implements the runnable command that orchestrates `GpuOptimizeExecutor` for compaction.
- GPU meta (tagging/conversion)
  - Path: `delta-lake/delta-33x/src/main/scala/com/nvidia/spark/rapids/delta/delta33x/OptimizeTableCommandMeta.scala`
  - Purpose: Tags unsupported cases (DV-writing, Z-Order, Liquid Clustering) and converts CPU to GPU.

Provider Registration

- File to update: `delta-lake/delta-33x/src/main/scala/com/nvidia/spark/rapids/delta/delta33x/Delta33xProvider.scala`
- Add a new `RunnableCommandRule` similar to Delete/Update/Merge that recognizes the CPU `OptimizeTableCommand` and uses `OptimizeTableCommandMeta` for tagging and conversion.

Detailed Implementation Steps

1) Analyze CPU OptimizeTableCommand for Delta 3.3.x
- Inputs: table identifier (or DeltaLog), optional predicate/where clause, and optimize options.
- Modes: For 3.3.x in OSS Delta, ensure we handle compaction. If Z-Order/Clustering is surfaced via options/params, they must be tagged unsupported on GPU.
- Outputs: Check whether CPU returns a result DataFrame schema (e.g., summary rows). If so, mirror the structure and populate from GPU executor metrics.

2) GpuOptimizeTableCommand: shape and behavior
- Constructor fields mirror the CPU command (table ident/spec, where/filter, options/maps) needed to build `DeltaOptimizeContext` and to locate the table.
- on run(sparkSession):
  - Resolve the `DeltaLog` and begin a `GpuOptimisticTransactionBase`.
  - Build `DeltaOptimizeContext`:
    - Set `minFileSize`, `maxFileSize` and thresholds from session confs or command options (align with CPU defaults/behavior).
    - Apply the where predicate (if present) to limit candidate files (partition filters only are supported; otherwise tag fallback in Meta).
  - Use `OptimizeTableStrategy.Compaction` (or equivalent) for strategy.
  - Call `GpuOptimizeExecutor.optimize(...)` to perform the work.
  - Collect/return Rows consistent with CPU behavior (if the CPU command returns a summary; otherwise return empty Seq).

3) OptimizeTableCommandMeta: tagging and conversion
- Tag unsupported conditions:
  - Any request implying Z-Order or Liquid Clustering.
  - Any path requiring writing Deletion Vectors.
  - Any where clause that cannot be satisfied by partition pruning supported on GPU.
  - Any table features incompatible with current GPU Delta write path (reuse `RapidsDeltaUtils.tagForDeltaWrite`).
- Conversion:
  - Build and return a `GpuOptimizeTableCommand` with the same parameters as the CPU node.

4) Provider rule
- In `Delta33xProvider.getRunnableCommandRules`, add a new rule:
  - Match the CPU `OptimizeTableCommand` class for 3.3.x.
  - Supply the meta factory that returns `OptimizeTableCommandMeta`.

5) Metrics and history
- `GpuOptimizeExecutor` already populates metrics and commits the OPTIMIZE operation to table history.
- Ensure `GpuOptimizeTableCommand` reports/returns metrics consistent with CPU (if CPU returns a result set). Otherwise, rely on history for validation.

6) Configuration and safety checks
- Respect Delta SQL confs that influence optimize thresholds where applicable.
- Ensure DV checks are enforced by calling the existing DV guard in `GpuOptimizeExecutor` (it currently calls `ensureDeletionVectorDisabled`).
- Validate identity columns and other writer constraints via `GpuOptimisticTransactionBase`.

Testing Plan

- New integration tests: `integration_tests/src/main/python/delta_lake_optimize_table_test.py`
  - Scenarios (happy path):
    - Unpartitioned table: write many small files; run `OPTIMIZE` on CPU and GPU; compare:
      - Final data equality (collect + sort); and/or file layout properties (fewer/larger files). Use existing helpers such as `assert_gpu_and_cpu_writes_are_equal_collect` when applicable.
      - Table history contains `OPTIMIZE` with similar operation metrics.
    - Partitioned table: ensure partition pruning does not break and results match CPU.
  - Negative/unsupported cases (tagging behavior):
    - Table with Deletion Vectors enabled → GPU should tag unsupported and fall back to CPU (or test is skipped on GPU if configured to require GPU).
    - Requests implying Z-Order or Liquid Clustering (if exposed in 3.3.x) → ensure GPU tags unsupported.
  - Test utilities/patterns to reuse:
    - See `integration_tests/src/main/python/delta_lake_auto_compact_test.py` for asserting OPTIMIZE history entries and metrics comparisons.
    - Use `with_cpu_session`/`with_gpu_session` helpers and marks from the existing test harness.

Validation Criteria (GPU = CPU)

- Data parity: After `OPTIMIZE`, a read of the table returns the same rows in the same schema for CPU and GPU runs.
- Operation history: An `OPTIMIZE` entry exists for both, and GPU operation metrics are consistent with CPU within acceptable bounds.
- No DV writes: GPU path must not attempt DV-writing; if required, it must be tagged unsupported.
- No Z-Order/Liquid Clustering: Requests for these modes must be tagged unsupported on GPU for now.

Dev Workflow and Branching

- Create a feature branch: `feature/delta33x-optimize-table`.
- Implement files and provider registration as above.
- Run formatter/style if applicable and compile.
- Run relevant unit/integration tests locally.
- Iterate until tests pass with CPU-GPU parity.
- Commit changes with clear messages and open a PR as needed.

Risk and Mitigations

- Divergence between OSS Delta 3.3.x Optimize APIs and Databricks variants: Confine implementation to `delta-33x` and reuse only common pieces (`GpuOptimizeExecutor`).
- Metrics schema drift across Delta versions: The test compares presence and key metrics but tolerates minor differences where necessary.
- DV or advanced optimize modes: Tagged unsupported to avoid partial/incorrect GPU behavior.

Acceptance Checklist

- New files compile and are picked up by the build.
- Provider rule visible in explain plans and replacement occurs for supported scenarios.
- Integration tests pass on both unpartitioned and partitioned cases with CPU-GPU equivalence.
- Unsupported scenarios are correctly tagged and either fall back or are skipped per test expectations.

References (in-repo)

- `delta-lake/common/src/main/delta-33x/scala/org/apache/spark/sql/delta/rapids/commands/GpuOptimizeExecutor.scala`
- `delta-lake/common/src/main/delta-33x/scala/org/apache/spark/sql/delta/hooks/GpuAutoCompact.scala`
- `delta-lake/delta-33x/src/main/scala/org/apache/spark/sql/delta/rapids/delta33x/GpuOptimisticTransaction.scala`
- `delta-lake/delta-33x/src/main/scala/org/apache/spark/sql/delta/rapids/delta33x/GpuDeleteCommand.scala`
- `delta-lake/delta-33x/src/main/scala/com/nvidia/spark/rapids/delta/delta33x/DeleteCommandMeta.scala`
- `delta-lake/delta-33x/src/main/scala/org/apache/spark/sql/delta/rapids/delta33x/GpuUpdateCommand.scala`
- `integration_tests/src/main/python/delta_lake_auto_compact_test.py`

Next Step

- Review this plan. On approval, proceed to branch creation and implementation following the steps above.
