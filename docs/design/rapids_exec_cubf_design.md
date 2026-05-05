---
layout: page
title: CuBF GPU Exec Design
nav_order: 18
parent: Developer Overview
---
# CuBF GPU Exec Design

This document covers the following topics:

* [1. Background & Motivation](#1-background--motivation)
* [2. Scope & Non-Scope](#2-scope--non-scope)
  * [2.1 Scope](#21-scope)
  * [2.2 Contracts Expected From the Planner Layer](#22-contracts-expected-from-the-planner-layer)
  * [2.3 Non-Scope](#23-non-scope)
* [3. Design Goals](#3-design-goals)
* [4. End-to-End Architecture](#4-end-to-end-architecture)
* [5. Component Design](#5-component-design)
  * [5.1 BFSpec](#51-bfspec)
  * [5.2 GpuGenerateBloomFilterExec](#52-gpugeneratebloomfilterexec)
  * [5.3 InlineBFBuildReplacement](#53-inlinebfbuildreplacement)
  * [5.4 GpuOverrides Registration](#54-gpuoverrides-registration)
  * [5.5 Build-Side Accumulator and Delivery Contract](#55-build-side-accumulator-and-delivery-contract)
  * [5.6 Probe-Side Metadata and Instrumentation](#56-probe-side-metadata-and-instrumentation)
  * [5.7 GpuBloomFilterMightContain Compatibility](#57-gpubloomfiltermightcontain-compatibility)
  * [5.8 Shim Wiring and Supported Profiles](#58-shim-wiring-and-supported-profiles)
* [6. Inertness & Compatibility](#6-inertness--compatibility)
* [7. Resource Management & Failure Behavior](#7-resource-management--failure-behavior)
  * [7.1 Pass-Through Semantics](#71-pass-through-semantics)
  * [7.2 GPU Resource Ownership](#72-gpu-resource-ownership)
  * [7.3 Closing Behavior](#73-closing-behavior)
  * [7.4 Accumulator Merge Behavior](#74-accumulator-merge-behavior)
  * [7.5 Unsafe or Unavailable Bloom Filters](#75-unsafe-or-unavailable-bloom-filters)
* [8. Cost Analysis](#8-cost-analysis)
  * [8.1 Per-Bloom-Filter Size](#81-per-bloom-filter-size)
  * [8.2 Build-Side Overhead Per Task](#82-build-side-overhead-per-task)
  * [8.3 Build-Side Overhead Total](#83-build-side-overhead-total)
  * [8.4 Probe-Side Overhead Per Task](#84-probe-side-overhead-per-task)
  * [8.5 What Drives Total Overhead](#85-what-drives-total-overhead)
  * [8.6 Sensitivity Analysis](#86-sensitivity-analysis)
  * [8.7 Case Study: NDS SF3K Query 50](#87-case-study-nds-sf3k-query-50)
* [9. Alternatives Considered](#9-alternatives-considered)
  * [9.1 Reuse the Spark OSS Scalar-Subquery Build Path](#91-reuse-the-spark-oss-scalar-subquery-build-path)
  * [9.2 Hard-Link Against Planner-Layer Classes](#92-hard-link-against-planner-layer-classes)
  * [9.3 Change Existing OSS Bloom Filter Behavior Globally](#93-change-existing-oss-bloom-filter-behavior-globally)
  * [9.4 Build Bloom Filters With an Extra Build-Side Scan](#94-build-bloom-filters-with-an-extra-build-side-scan)
* [10. Validation Strategy](#10-validation-strategy)
* [11. Limitations and Resource Risks](#11-limitations-and-resource-risks)
  * [11.1 GPU Memory on Executors](#111-gpu-memory-on-executors)
  * [11.2 Network Between Executors and Driver](#112-network-between-executors-and-driver)
  * [11.3 Driver Heap and Merge CPU](#113-driver-heap-and-merge-cpu)
  * [11.4 Accumulator Cache Soft Leak](#114-accumulator-cache-soft-leak)
  * [11.5 Configuration Derivation Summary](#115-configuration-derivation-summary)
* [12. Future Work](#12-future-work)

## 1. Background & Motivation

Spark OSS can already inject runtime bloom filters through `InjectRuntimeFilter`.
When Spark produces the standard `BloomFilterAggregate` and
`BloomFilterMightContain` expressions, RAPIDS supports accelerating that path on
the GPU. That support remains the baseline behavior: Spark-owned runtime bloom
filter planning continues to work without requiring CuBF-specific nodes.

CUDA bloom filter (CuBF) is a RAPIDS-owned extension point for additional bloom
filter opportunities that are beneficial when the build and probe pipelines are
already GPU-resident. The goal is to let a planner identify opportunities that
Spark OSS does not inject, while keeping the RAPIDS execution layer responsible
for GPU resource management, bloom filter construction, delivery, and probe-side
compatibility.

This document describes the RAPIDS GPU execution support for CuBF. It does not
define the full logical optimization policy: join eligibility, benefit
estimation, user-facing policy, and marker generation are separate planner-layer
responsibilities.

## 2. Scope & Non-Scope

### 2.1 Scope

This design covers the RAPIDS-side execution architecture:

| Area | Responsibility |
|------|----------------|
| Build marker replacement | Detect an optional planner-emitted build marker and replace it with a GPU execution node. |
| Inline GPU build | Build one or more bloom filters while the build-side columnar batches flow through unchanged. |
| Delivery contract | Publish merged bloom filter bytes or a skip state keyed by bloom filter id. |
| Probe compatibility | Keep `GpuBloomFilterMightContain` compatible with both Spark OSS bloom filters and CuBF-produced bloom filters. |
| Shim integration | Wire the replacement and already-GPU exec registration only in supported Spark profiles. |
| Observability hooks | Provide optional build/probe metadata plumbing without changing canonical execution semantics. |

### 2.2 Contracts Expected From the Planner Layer

The RAPIDS execution layer expects the planner layer to:

1. Emit a physical build marker or stub only when a CuBF bloom filter should be
   built.
2. Provide stable bloom filter ids that link build-side specs to probe-side
   consumers.
3. Provide build specifications that are valid for the build-side output schema.
4. Provide probe-side consumers that interpret missing or skipped bloom filters
   as no-op filters, not as negative membership results.
5. Avoid depending on CuBF execution classes in Spark profiles where CuBF is not
   supported.

### 2.3 Non-Scope

The following items are intentionally outside this execution design:

- Logical bloom filter injection rules.
- Join eligibility and benefit heuristics.
- Composite-key bloom filter planning and execution semantics.
- Runtime selectivity, feedback, or cost-benefit policy decisions.
- Replacement of Spark OSS `InjectRuntimeFilter`.
- Global behavior changes for existing Spark OSS runtime bloom filters.

## 3. Design Goals

| Goal | Description |
|------|-------------|
| Inert by default | Normal RAPIDS planning is unchanged unless CuBF planner nodes are present. |
| Pass-through execution | Build-side rows, schema, and batch objects flow through unchanged. |
| Inline construction | Bloom filters are built during the existing build-side GPU pass, avoiding an extra build-side scan. |
| Multi-filter support | One build node can construct multiple bloom filters over the same child stream. |
| Safe delivery | Consumers can distinguish real bloom filter bytes from a skip/no-op state. |
| OSS compatibility | Existing `InjectRuntimeFilter` acceleration remains unchanged. |
| Profile isolation | Unsupported Spark profiles do not need CuBF-specific planner classes or execution classes. |

## 4. End-to-End Architecture

At a high level, the logical/planner layer owns the decision to create CuBF
markers. RAPIDS owns replacing those markers with GPU execution and preserving
safe behavior when markers are absent.

```
+---------------------------------------------------------------------------+
|                         Logical / Planner Layer                           |
|                                                                           |
|  choose opportunity -> emit build marker/stub -> emit probe consumer       |
|                        (bfId + BFSpec data)       (same bfId)             |
+----------------------------------+----------------------------------------+
                                   |
                                   | physical plan
                                   v
+---------------------------------------------------------------------------+
|                       RAPIDS Replacement Layer                             |
|                                                                           |
|  ColumnarOverrideRules.preColumnarTransitions                             |
|    -> SparkShimImpl.applyPreGpuOverridesRules                             |
|    -> InlineBFBuildReplacement.applyIfNeeded                              |
|    -> GpuOverrides                                                        |
|                                                                           |
|  InlineBFBuildReplacement                                                 |
|    - detects optional build marker reflectively                            |
|    - reads one or more BFSpec values                                       |
|    - produces GpuGenerateBloomFilterExec                                   |
+----------------------------------+----------------------------------------+
                                   |
                                   | GPU physical plan
                                   v
+---------------------------------------------------------------------------+
|                         Build-Side GPU Pipeline                            |
|                                                                           |
|  child batches -> GpuGenerateBloomFilterExec -> same child batches         |
|                       |                                                   |
|                       +-- build per-partition bloom filters                |
|                       +-- publish bytes or skip state by bfId              |
+----------------------------------+----------------------------------------+
                                   |
                                   | delivery contract
                                   v
+---------------------------------------------------------------------------+
|                         Probe-Side Consumption                             |
|                                                                           |
|  delivery mechanism -> bloom filter bytes / no-op ->                      |
|  GpuBloomFilterMightContain consumes Spark-compatible bloom filter bytes   |
+---------------------------------------------------------------------------+
```

The execution layer is deliberately narrow: it does not decide whether a join
should receive a bloom filter, and it does not assume the planner layer is always
available on the classpath.

## 5. Component Design

### 5.1 `BFSpec`

`BFSpec` is the per-bloom-filter build specification copied from the optional
planner marker into RAPIDS execution. It contains:

| Field | Meaning |
|-------|---------|
| `bfId` | Stable id that links build delivery to probe consumption. |
| `keyColumnIndex` | Build-side output column index used as the bloom filter key. |
| `numHashes` | Number of bloom filter hash functions. |
| `numBits` | Bloom filter bit count. |

The build exec receives one or more specs. Multi-spec support is important
because a planner may coalesce sibling bloom filter builds that share the same
child stream. The shared parameters that describe wire format and hashing, such
as `bfVersion`, `seed`, and `xxHashSeed`, are carried by the build exec rather
than duplicated in every spec.

For example, a plan may have one filtered dimension-side stream that can provide
two bloom filters to different probe-side consumers:

```
                   Filtered dimension keys
                          child stream
                               |
             +-----------------+-----------------+
             |                                   |
      build bloom filter                  build bloom filter
      for customer_id                     for order_id
      (bfId = bf_customer)                (bfId = bf_order)
```

Rather than running two inline build wrappers over the same child, the planner
can represent the shared child once and ask `GpuGenerateBloomFilterExec` to build
both filters in the same columnar pass:

```
GpuGenerateBloomFilterExec(
  specs = Seq(
    BFSpec("bf_customer", keyColumnIndex = 0, numHashes, numBits),
    BFSpec("bf_order",    keyColumnIndex = 1, numHashes, numBits)),
  child = filteredDimensionKeys)
```

If multiple joins can consume the same bloom filter, the planner should model
that as one producer and multiple consumers of the same `bfId`:

```
                            shared build stream
                                   |
                                   v
             GpuGenerateBloomFilterExec(BFSpec("bf_customer", ...))
                                   |
                                   v
                         delivery for bf_customer
                            /              \
                           /                \
                  probe predicate      probe predicate
                    in join A            in join B
```

In that case the same `BFSpec` and `bfId` describe one logical bloom filter, and
multiple probe-side predicates may read the same delivered bytes. However, a
duplicated `BFSpec` inside two independently executed build nodes is not enough
to guarantee sharing; each build node would still construct and publish its own
partition-local bloom filters. The planner layer is responsible for coalescing
or reusing the shared build producer when it proves the build child, key column,
hash parameters, bloom filter size, and lifecycle are identical.

The current execution contract is a single-key-column contract: each `BFSpec`
points at one build-side key column. Composite-key support should extend the
spec and probe contracts deliberately rather than overloading
`keyColumnIndex`. Build and probe execution must agree on row-level composite
hashing, null handling, and type compatibility before a multi-column key can
share the same correctness guarantees as the single-key path.

Key invariants:

1. `specs` is non-empty.
2. Every `bfId` is stable and unique for the bloom filter it identifies.
3. `keyColumnIndex` references a build-side column that can be hashed with the
   probe-side bloom filter semantics.
4. All partition-local bloom filters for a given `bfId` have the same serialized
   size and header layout.

### 5.2 `GpuGenerateBloomFilterExec`

`GpuGenerateBloomFilterExec` is a unary `GpuExec` that builds bloom filters
inline with the build-side GPU pipeline.

Shared build-exec parameters:

| Parameter | Type | Meaning |
|-----------|------|---------|
| `bfVersion` | `Int` | Bloom filter format version. Version 1 is used by Spark 3.x; version 2 is used by Spark 4.x and carries an additional seed field in the serialized header. |
| `seed` | `Int` | Bloom filter hash seed used by version 2 serialized bloom filters; version 1 uses `0`. |
| `xxHashSeed` | `Long` | XxHash64 seed used for build keys. This must match the probe-side hash used by `BloomFilterMightContain`. |
| `buildCostUpdaters` | `Map[String, BloomFilterBuildCostUpdater]` | Optional per-`bfId` build-cost update sinks. The default is `Map.empty`, keeping build-cost instrumentation inert. |

```
       build-side ColumnarBatch iterator
                    |
                    v
+---------------------------------------------+
|       GpuGenerateBloomFilterExec            |
|                                             |
|  for each non-empty batch:                  |
|    for each BFSpec:                         |
|      key column -> XxHash64 -> put          |
|                                             |
|  on partition completion:                   |
|    bloom filter scalar -> host bytes        |
|      -> accumulator                         |
|                                             |
|  output: original batch object              |
+---------------------------------------------+
                    |
                    v
          unchanged ColumnarBatch iterator
```

The operator has pass-through semantics:

- `output` is the child output.
- Columnar batches are returned unchanged.
- Batch sizes are not intentionally changed.
- Row-based execution is unsupported.
- Canonicalization delegates to the child so the wrapper does not disrupt plan
  equality and exchange/subquery reuse.

Build behavior:

1. Each task lazily creates a GPU bloom filter per spec as data arrives.
2. Build keys are hashed with the same XxHash64 seed expected by the probe-side
   `BloomFilterMightContain` expression.
3. Partition-local bloom filters are copied to Spark-compatible serialized bytes.
4. One build accumulator per `bfId` receives the partition-local bytes.
5. Empty partitions do not publish bloom filter bytes.

Safety behavior:

- Oversized bloom filters are marked skipped before launching GPU build work.
- The effective size cap is resolved through an optional capability helper when
  available; failure to resolve it falls back to a conservative cap.
- GPU resources are closed on normal iterator exhaustion and through task
  completion cleanup if a task fails or the iterator is abandoned.

### 5.3 `InlineBFBuildReplacement`

`InlineBFBuildReplacement` is a pre-`GpuOverrides` physical rule. It looks for
an optional planner-emitted inline bloom filter build marker and replaces it with
`GpuGenerateBloomFilterExec`.

The shim entry point performs a fast class-name scan with `isNeeded` before
instantiating the replacement rule, so plans without CuBF markers bypass rule
construction entirely. When runtime-feedback instrumentation is enabled, the
replacement rule also resolves per-`bfId` build-cost updaters and passes them to
the GPU exec. See Section 5.6 for the observability contract.

The replacement is intentionally reflection-based:

```
physical plan
    |
    v
InlineBFBuildReplacement
    |
    +-- no marker class/name present -------> return original plan
    |
    +-- marker found and fields readable ---> GpuGenerateBloomFilterExec
    |
    +-- marker found but unreadable --------> return original marker
```

This avoids a compile-time dependency on planner-layer classes. The preferred
marker shape exposes a `specs` sequence. A legacy single-spec shape can be
adapted into a one-element `Seq[BFSpec]`, which keeps the downstream execution
contract uniform.

If replacement cannot be performed, the rule leaves the original plan unchanged.
The planner layer must ensure that any unreplaced marker has safe no-op behavior
or is not emitted in unsupported environments.

### 5.4 `GpuOverrides` Registration

After replacement, `GpuGenerateBloomFilterExec` is already a GPU physical
operator. It is still registered with `GpuOverrides` through
`InlineBFBuildGpuOverride` so the normal RAPIDS planning pass recognizes it as
GPU-compatible.

The registration is pass-through:

- Tagging does not reject the exec.
- Conversion returns the existing `GpuGenerateBloomFilterExec`.
- The rule exists to keep the normal RAPIDS override pipeline aware of the
  already-GPU node.

### 5.5 Build-Side Accumulator and Delivery Contract

`BloomFilterBuildAccumulator` is the build-side delivery primitive. Each
`bfId` has a separate accumulator. Partition-local bloom filter bytes are merged
on the driver by bitwise OR over the serialized bloom filter data section.

```
Partition 0 bloom filter bytes --+
Partition 1 bloom filter bytes --+--> BloomFilterBuildAccumulator
Partition 2 bloom filter bytes --+              |
                                                v
                                      merged bloom filter bytes

Oversized / unsafe build ---------------------> skip sentinel
```

Merge invariants:

1. Headers for a given `bfId` are identical across partitions.
2. Only the data section is OR-merged; the header is preserved.
3. Serialized bloom filter versions with different header sizes are handled by
   detecting the version header. Version 1 uses a 12-byte header; version 2 uses
   a 16-byte header that includes an additional 4-byte hash seed field.
4. A size mismatch is a contract violation and must not be silently merged.

Skip behavior uses a four-byte all-zero sentinel. The sentinel is not a valid
serialized bloom filter because real bloom filter payloads start with a non-zero
version header. Once any partition or driver-side guard publishes the sentinel,
skip wins over later real bytes. Downstream delivery consumers must interpret the
sentinel as "do not apply this bloom filter".

The delivery contract has three meaningful states:

| State | Meaning | Consumer behavior |
|-------|---------|-------------------|
| Merged bytes | A valid bloom filter was built. | Publish bytes to the probe predicate. |
| Skip sentinel | The bloom filter should not be used. | Apply no bloom filter for this `bfId`. |
| No bytes | No partition produced a filter. | Treat as no-op unless the planner contract says otherwise. |

### 5.6 Probe-Side Metadata and Instrumentation

The core probe expression remains `GpuBloomFilterMightContain`. CuBF adds
optional metadata plumbing around that existing expression:

- `bfId` identifies the CuBF bloom filter associated with a probe predicate.
- `BloomFilterPredicateUpdater` can receive per-batch `(rowsIn, rowsPassed)`
  updates.
- `BloomFilterProbeAccumulator` can aggregate those updates on the driver.
- `BloomFilterBuildCostAccumulator` can aggregate build-side
  `(buildWallNanos, bfBytes)` updates.

These hooks are optional and default to absent. They are enabled only when the
relevant runtime-feedback and instrumentation configuration is on and the
`bfId` can be discovered from the probe-side subquery plan.

Discovery walks the probe-side bloom filter subquery plan to find a
planner-emitted execution node that carries the `bfId` linking build and probe.
The target node is identified by class name, and its `bfId` is read
reflectively, so there is no compile-time dependency on planner classes. Because
AQE can wrap the subquery plan in `AdaptiveSparkPlanExec`, discovery also probes
AQE plan accessors reflectively to reach the underlying physical plan.
If discovery fails at any step, `bfId` and `probeUpdater` remain `None` and the
expression behaves as the standard OSS replacement.

Probe-side discovery flow:

```
GpuBloomFilterMightContain
          |
          v
  bloom filter subquery plan
          |
          +-- optional AQE wrapper
          |       |
          |       v
          |   underlying physical plan
          |
          v
  class-name match for bfId carrier
          |
          +-- success -> read bfId -> optional probe updater
          |
          +-- failure -> bfId = None, probeUpdater = None
                        standard OSS replacement behavior
```

`BloomFilterBuildCostAccumulator.driverGetOrCreate` and
`BloomFilterProbeAccumulator.driverGetOrCreate` keep driver-side static caches
keyed by `bfId`. These caches are not cleaned up by the current implementation.
In a long-running driver JVM, entries accumulate per unique `bfId`. Each entry is
small, roughly 100 bytes plus map overhead, so this is not expected to block
initial enablement. The growth is still unbounded; a future cleanup mechanism
should evict stale entries when query or execution lifecycle boundaries make them
unreachable.

Instrumentation must not change execution semantics:

1. Probe updates are per columnar batch, not per row.
2. Build updates are per bloom filter build, not per input batch.
3. Canonicalization drops `bfId` and updater fields so observability wiring does
   not change plan equivalence.
4. When metadata is absent, `GpuBloomFilterMightContain` behaves like the
   existing Spark OSS bloom filter expression replacement.

### 5.7 `GpuBloomFilterMightContain` Compatibility

`GpuBloomFilterMightContain` consumes Spark-compatible serialized bloom filter
bytes and probes a `LongType` value column on the GPU. CuBF-produced bloom
filters must use the same serialized wire format and the same hash semantics as
the existing Spark OSS path.

Compatibility expectations:

- A valid bloom filter scalar is deserialized into `GpuBloomFilter`.
- A null bloom filter scalar follows the existing expression behavior.
- CuBF skip/no-op state must be handled before bytes reach
  `GpuBloomFilterMightContain`.
- Optional CuBF metadata does not affect type checking, canonicalization, or the
  existing OSS `BloomFilterMightContain` replacement path.

### 5.8 Shim Wiring and Supported Profiles

CuBF execution support is wired through the Spark shim layer:

1. A generic shim hook allows pre-`GpuOverrides` physical rules.
2. Supported bloom filter profiles apply `InlineBFBuildReplacement` before the
   normal GPU override pass.
3. The same supported profiles register `GpuGenerateBloomFilterExec` as an
   already-GPU physical operator.
4. Profiles without Spark runtime bloom filter support keep empty or no-op bloom
   filter shims.

The supported profile set should be governed by shim ownership: a profile should
enable CuBF execution only when it supports the required Spark bloom filter
expressions, serialized bloom filter format, and RAPIDS JNI bloom filter
primitives. Unsupported profiles should either never see CuBF planner markers or
leave them with safe no-op behavior.

Current profile behavior is split into three tiers:

| Profile tier | CuBF behavior |
|--------------|---------------|
| Pre-3.3.0 profiles | No Spark bloom filter expression support is registered. |
| Databricks shim variants | Spark bloom filter expression support is retained, but CuBF build exec wiring and pre-`GpuOverrides` replacement are not enabled. These profiles fall through to the base shim identity and remain inert for CuBF build markers. |
| OSS Spark 3.3.0+ profiles | Spark bloom filter expression support, `InlineBFBuildReplacement`, and `GpuGenerateBloomFilterExec` registration are enabled. |

## 6. Inertness & Compatibility

The design is intentionally inert unless the planner layer emits CuBF nodes.

```
No CuBF marker in plan
    |
    v
pre-GpuOverrides quick check returns original plan
    |
    v
normal RAPIDS GpuOverrides planning
    |
    v
existing Spark OSS bloom filter support unchanged
```

Compatibility properties:

- There is no compile-time dependency on optional planner marker classes.
- Marker detection is based on an optional class name and reflective accessors.
- If marker classes are absent, no replacement occurs and normal planning
  continues.
- If optional probe metadata is absent, constructor defaults preserve existing
  `GpuBloomFilterMightContain` behavior.
- Existing Spark OSS `InjectRuntimeFilter` support remains unchanged: standard
  Spark `BloomFilterAggregate` and `BloomFilterMightContain` nodes still follow
  the established RAPIDS acceleration path.
- Unsupported Spark profiles can keep no-op shim hooks and do not need
  CuBF-specific planner classes on the classpath.

## 7. Resource Management & Failure Behavior

### 7.1 Pass-Through Semantics

`GpuGenerateBloomFilterExec` must not change the build-side data stream. Its
child batches are returned to the parent operator unchanged, and the output
schema is the child schema. This property is required so adding a CuBF build node
does not affect join build-side semantics.

### 7.2 GPU Resource Ownership

The build exec owns GPU bloom filter scalars it creates. Temporary hashed columns
and host buffers used to serialize bloom filters are closed with RAPIDS resource
helpers. The probe expression owns the deserialized `GpuBloomFilter` it creates
from a bloom filter scalar and closes it on task completion. The probe-side
`GpuBloomFilter` wraps its device buffer in a `SpillableBuffer`, so the
deserialized bloom filter participates in RAPIDS memory spilling under GPU
memory pressure.

### 7.3 Closing Behavior

Normal completion finalizes all bloom filters when the input iterator is
exhausted. A task completion listener closes any still-live GPU bloom filters if
the task fails, is interrupted, or stops consuming the iterator before normal
exhaustion. Closing is best-effort and must not mask the original task failure.

### 7.4 Accumulator Merge Behavior

Accumulator merge is a driver-side reduction over serialized partition-local
bloom filters. Real bloom filter bytes merge by OR-ing the data section. The
skip sentinel is absorbing: once present, the accumulator value remains skipped.
This makes oversize and unsafe-build decisions deterministic across partition
merge order.

### 7.5 Unsafe or Unavailable Bloom Filters

If the execution layer can determine that a bloom filter cannot be built safely,
it publishes skip/no-op state instead of partial bytes. Examples include an
effective size cap exceeded before kernel launch or an unavailable optional
capability helper causing the guard to fail closed.

Unexpected runtime failures follow normal Spark task failure behavior. Resource
cleanup still runs, and consumers must not treat missing or skipped CuBF output
as evidence that probe-side rows are absent. The safe outcome is to skip the
CuBF filter and preserve query correctness.

## 8. Cost Analysis

CuBF adds overhead in three locations: GPU memory on executors during build and
probe, network between executors and the driver during accumulator shipping, and
driver JVM heap for merged bloom filter bytes and merge CPU. The build and probe
phases do not overlap in time: the build stage must complete and deliver merged
bytes before any probe task can consume them, so build-side and probe-side GPU
costs are never concurrent for the same bloom filter.

### 8.1 Per-Bloom-Filter Size

```
bf_bytes = ceil(numBits / 8) + header_bytes

header_bytes = 12   (V1, Spark 3.x)
             = 16   (V2, Spark 4.x)
```

`numBits` is determined by the planner heuristic. Typical sizing uses
`numItems * bitsPerItem`, where `bitsPerItem` depends on the target
false-positive probability: about 10 bits/item at FPP 0.01 with 7 hashes, and
about 5 bits/item at FPP 0.1 with 3 hashes.

A size guard caps `bf_bytes` at `effectiveMaxFilterBytes`. The cap is resolved
from an optional capability helper and falls back to the approximate 256 MB V1
indexing ceiling. Bloom filters exceeding the cap are marked skipped before any
GPU allocation or network transfer.

### 8.2 Build-Side Overhead Per Task

Each build-side task processes one partition of the build child stream. For `K`
bloom filter specs in one `GpuGenerateBloomFilterExec`, the persistent GPU
memory during the task lifetime is:

```
GPU_build_persistent = K * bf_bytes
```

These are GPU bloom filter scalars. They are lazily created on the first
non-empty batch and held until finalize. For typical dimension-side bloom
filters, from 1 KB to 200 KB, this is negligible. Near a configured size cap,
for example 8 MB per bloom filter, `K=3` would be 24 MB per task.

Transient GPU memory per batch is:

```
GPU_hash_transient = batch_rows * 8 bytes
```

This is one XxHash64 `INT64` column. The build loop processes specs
sequentially, so only one hash column exists at a time. At a typical RAPIDS
batch target of about 1M rows, this is about 8 MB. This cost is constant
regardless of bloom filter size; it depends only on batch row count.

Transient host memory at finalize is:

```
Host_finalize = K * 2 * bf_bytes
```

This accounts for one host buffer for the GPU-to-host copy plus one `byte[]` for
the accumulator payload per spec. Both are short-lived: allocated at finalize,
then released after the accumulator update.

GPU kernel launches per batch are:

```
kernels_per_batch = K * 2
```

There is one XxHash64 kernel and one bloom filter `put` kernel per spec. GPU
kernel launch overhead is roughly 5 to 10 microseconds each. For `K=3` and 50
batches per partition, that is 300 launches, or about 1.5 to 3 ms total, which
is negligible relative to batch processing time.

### 8.3 Build-Side Overhead Total

Network overhead from executors to the driver is:

```
Network_build = P_build * K * bf_bytes
```

`P_build` is the number of build-side partitions. Each task ships `K`
serialized byte arrays to the driver through Spark's accumulator protocol as
part of task completion messages, not as separate transfers. This is the
dominant build-side overhead for large bloom filters with many partitions.

Driver heap for merged bloom filters is:

```
Driver_heap_build = K * bf_bytes
```

The driver holds one merged `byte[]` per `bfId`. Merge is incremental: each
partition's bytes are OR'd into the existing merged array. Peak heap is one copy
per bloom filter, not `P_build` copies, plus a transient second copy during each
merge step.

Driver CPU merge work is:

```
Driver_merge_bytes = K * (P_build - 1) * (bf_bytes - header_bytes)
```

This is the total data-section byte count OR'd across all merges. It runs
single-threaded on the DAGScheduler event loop. For typical dimension-side bloom
filter sizes (under 200 KB), merge overhead per partition is negligible.

### 8.4 Probe-Side Overhead Per Task

Each probe task deserializes the full merged bloom filter once, via a lazy value
triggered on first `columnarEval`, and holds it for the task lifetime.

Persistent probe GPU memory during the task lifetime is:

```
GPU_probe_persistent = S * bf_bytes
```

`S` is the number of stacked bloom filter predicates on the same probe-side
scan. If `K` bloom filters probe different join stages, each task holds only one
filter (`S=1`). If all `K` are stacked on the same probe table, each probe task
holds all `K` filters (`S=K`).

Each deserialized bloom filter is wrapped in a `SpillableBuffer` at
`ACTIVE_ON_DECK_PRIORITY`. Under GPU memory pressure, bloom filters can spill to
host memory. Re-materialization requires one host-to-device copy per batch that
accesses a spilled bloom filter.

Per-executor probe GPU memory is:

```
GPU_probe_per_executor = T * S * bf_bytes
```

`T` is the effective number of concurrent GPU tasks per executor. It is bounded
by both CPU task slots and GPU resource slots:

```
T = min(executor_cores / spark.task.cpus,
        executor_gpus / spark.task.resource.gpu.amount)
```

`executor_cores` is the number of cores assigned to the executor
(`spark.executor.cores`). `executor_gpus` is the number of GPU resources
assigned to the executor (`spark.executor.resource.gpu.amount`).

Transient GPU memory per probe batch is:

```
GPU_probe_batch = batch_rows * 1 byte
```

This is the `BOOL8` result column. At 1M rows per batch, it is about 1 MB. It is
created per `columnarEval`, consumed by the parent filter, then closed.

When probe instrumentation is enabled, one additional `sum(DType.INT64)`
reduction produces a scalar per batch per stacked bloom filter. This is
sub-microsecond on the GPU. The `(rowsIn, rowsPassed)` tuple piggybacks on
Spark's accumulator heartbeat protocol.

### 8.5 What Drives Total Overhead

| Resource | Dominant factor | Formula | When it matters |
|----------|-----------------|---------|-----------------|
| Executor GPU, build | Specs times bloom filter size | `K * bf_bytes` per task | Near size cap with multiple specs |
| Executor GPU, probe | Tasks times stacked filters times size | `T * S * bf_bytes` per executor | Near size cap with high task concurrency |
| Network | Partitions times specs times size | `P_build * K * bf_bytes` total | Large filters with many build partitions |
| Driver heap | Number of unique filters | `K * bf_bytes` | Near size cap with many concurrent filters |
| Driver CPU | Merge operations | `K * P_build * bf_bytes` bytes OR'd | Many build partitions |

For most queries, bloom filter sizes are 1 KB to 200 KB and all overhead
dimensions are negligible relative to the data volumes being filtered.

### 8.6 Sensitivity Analysis

The following tables show how overhead scales with the primary tuning
parameters. All values assume V1 format with a 12-byte header. The GPU hash
transient cost, about 8 MB per batch at 1M rows, is constant across all rows and
is omitted.

Table A shows overhead versus bloom filter size with `K=1`, `P_build=200`, and
`T=16` tasks per executor. This is the primary sensitivity because the bloom
filter size cap is the main user-controlled knob.

| `bf_bytes` | GPU build/task | Network total | Driver heap | GPU probe/executor |
|------------|----------------|---------------|-------------|--------------------|
| 64 KB | 64 KB | 12.5 MB | 64 KB | 1 MB |
| 256 KB | 256 KB | 50 MB | 256 KB | 4 MB |
| 1 MB | 1 MB | 200 MB | 1 MB | 16 MB |
| 4 MB | 4 MB | 800 MB | 4 MB | 64 MB |
| 8 MB | 8 MB | 1.6 GB | 8 MB | 128 MB |

All dimensions scale linearly with `bf_bytes`. Network is the first dimension to
become non-trivial because it multiplies by `P_build`. At the 8 MB fact cap,
network totals 1.6 GB. This is still modest compared to the multi-TB shuffle
volumes these bloom filters target, but it is visible in Spark UI accumulator
panels.

Table B shows probe GPU memory versus bloom filter size and concurrency per
executor. Rows vary `bf_bytes`; columns vary `T * S`, the product of concurrent
tasks and stacked bloom filters per task.

| `bf_bytes` | `T*S=1` | `T*S=4` | `T*S=16` | `T*S=48` |
|------------|---------|---------|----------|----------|
| 65 KB, q50 observed | 65 KB | 260 KB | 1 MB | 3 MB |
| 256 KB | 256 KB | 1 MB | 4 MB | 12 MB |
| 1 MB | 1 MB | 4 MB | 16 MB | 48 MB |
| 4 MB | 4 MB | 16 MB | 64 MB | 192 MB |
| 8 MB | 8 MB | 32 MB | 128 MB | 384 MB |

The `T*S=48` column represents 16 concurrent tasks with 3 stacked bloom filters
each. At 8 MB per bloom filter this reaches 384 MB per executor, which is
significant on a 16 to 24 GB GPU. However, all probe-side bloom filters are
wrapped in `SpillableBuffer`, so they are eligible for RAPIDS memory spilling
under GPU pressure. In practice, bloom filter sizes observed in NDS SF3K
cluster runs are 1 KB to 200 KB, placing the operating point firmly in the
sub-10-MB-per-executor range even at `T*S=48`.

Table C shows network overhead versus build partitions with `K=1`. Network
overhead is linear in both `bf_bytes` and `P_build`.

| `bf_bytes` \\ `P_build` | 1 | 10 | 50 | 200 |
|-------------------------|---|----|----|-----|
| 65 KB | 65 KB | 650 KB | 3.2 MB | 12.7 MB |
| 1 MB | 1 MB | 10 MB | 50 MB | 200 MB |
| 4 MB | 4 MB | 40 MB | 200 MB | 800 MB |
| 8 MB | 8 MB | 80 MB | 400 MB | 1.6 GB |

Small dimension tables, for example `date_dim` with `P_build` near 1, produce
negligible network traffic regardless of bloom filter size. Network overhead
becomes material only when large bloom filters are built over many-partition
tables.

### 8.7 Case Study: NDS SF3K Query 50

The following numbers are from a production EMR-EKS NDS SF3K cluster validation
run on 2026-05-03.

Cluster configuration:

- 8 executors, 14 cores each, 1 GPU each.
- `spark.task.resource.gpu.amount = 0.0625`. GPU resource slots per executor:
  `1 / 0.0625 = 16`. CPU task slots: `14 / 1 = 14` (default `spark.task.cpus=1`).
  Effective `T = min(14, 16) = 14`.
- `spark.sql.shuffle.partitions = 200`.
- Scale factor: SF3K, or 3 TB.

Query 50 bloom filter parameters:

- 3 CuBF bloom filters sharing one build child, `store_returns` after filters.
- Multi-spec: one `GpuGenerateBloomFilterExec` with 3 `BFSpec` entries.
- Probe-side stack depth: `S=3`, all filtering `store_sales`.
- Build NDV: 110,799.
- Each bloom filter: `numBits=531007`, `numHashes=3`,
  `bf_bytes=66388`, about 65 KB, FPP 0.1 (the planner driver log
  reports data-section size 66376; the full serialized payload adds
  the 12-byte V1 header).
- V1 format with a 12-byte header.
- Probe table: `store_sales`, about 8.25 billion rows.

Build-side per task:

- GPU persistent: `3 * 65 KB = 195 KB`.
- GPU hash transient: `1M rows * 8 = 8 MB`, one spec at a time.
- Host finalize: `3 * 2 * 65 KB = 390 KB`, transient.

Build-side total with `P_build=200`:

- Network: `200 * 3 * 65 KB = about 38 MB`.
- Driver heap: `3 * 65 KB = 195 KB`.
- Driver merge CPU: `199 * 3 * 65 KB = about 38 MB` OR'd, negligible at this
  filter size.

Probe-side per task with 3 stacked bloom filters:

- GPU persistent: `3 * 65 KB = 195 KB`, each in a `SpillableBuffer`.
- GPU transient: about 1 MB per batch for the `BOOL8` result.

Probe-side per executor with `T=14` and `S=3`:

- GPU persistent: `14 * 3 * 65 KB = about 2.7 MB`.

Comparison to data volumes:

- `store_sales` probe data: about 8.25B rows, or about 770 GB.
- Query 50 observed shuffle reads: about 139 GB.
- Total CuBF overhead: about 38 MB network, about 2.7 MB GPU per executor, and
  195 KB driver heap.
- Overhead ratio: less than 0.03% of the data volume being filtered.

Observed bloom filter size distribution across the full NDS SF3K run of 103
queries:

| Size range | Count injected | Examples |
|------------|----------------|----------|
| < 1 KB | 2 | 5 B tiny dimension; 342 B `household_demographics`, NDV 285 |
| 1-2 KB | 5 | 1,393-1,880 B, filtered `item` and `customer_demographics` |
| 2-16 KB | 3 | 5,576-11,280 B, medium dimensions |
| 64-200 KB | 4 | 66,388 B q50 `store_returns` times 3; 199,319 B q69 `customer_address` |
| Skipped by size guard | 14 | 15.4-169 MB high-NDV fact-side candidates |

All 15 injected bloom filters were under 200 KB. The 14 oversized candidates,
from 15 MB to 169 MB, were caught by the size guard before any GPU allocation or
network transfer. This confirms that the practical operating point is in the
sub-MB range, with the size guard serving as the critical scalability boundary.

The planner's bloom filter size cap is the primary lever for bounding worst-case
overhead. Bloom filters exceeding the configured cap are skipped before any GPU
allocation or network transfer occurs. The sensitivity tables above show the
overhead envelope for capacity planning when adjusting this cap.

## 9. Alternatives Considered

### 9.1 Reuse the Spark OSS Scalar-Subquery Build Path

The Spark OSS path is already valuable and remains supported. Reusing only that
path for CuBF, however, limits CuBF to opportunities Spark already knows how to
represent and can require a separate scalar-subquery build pipeline. Inline build
support gives RAPIDS a narrower execution contract for planner-selected GPU
opportunities.

### 9.2 Hard-Link Against Planner-Layer Classes

Direct type references would simplify replacement code, but they would require
the RAPIDS public execution module to compile and run with planner-layer classes
present. Reflection keeps the public execution layer independently loadable and
lets unsupported profiles remain no-op.

### 9.3 Change Existing OSS Bloom Filter Behavior Globally

Changing the existing `BloomFilterAggregate` or `BloomFilterMightContain`
behavior globally would risk regressions in Spark-owned `InjectRuntimeFilter`
queries. CuBF instead adds optional metadata and a separate inline build path
while preserving the established OSS path.

### 9.4 Build Bloom Filters With an Extra Build-Side Scan

An extra scan can keep the main build pipeline simpler, but it increases I/O and
GPU work and can interfere with exchange or subquery reuse. Inline construction
uses the existing build-side stream and keeps the bloom filter build tied to the
data already flowing through the GPU pipeline.

## 10. Validation Strategy

Validation should cover both execution contracts and inertness:

| Category | What to validate |
|----------|------------------|
| Accumulator merge | Real bloom filter bytes OR correctly, headers are preserved, size mismatches are rejected. |
| Skip/no-op behavior | The skip sentinel wins over real bytes and is distinguishable from valid bloom filter bytes. |
| Multi-spec build contract | One build node registers one accumulator per spec and avoids cross-contamination between `bfId` values. |
| Replacement reflection | Test-only stubs exercise multi-spec and legacy single-spec marker shapes without optional classpath dependencies. |
| Already-GPU planning | `GpuGenerateBloomFilterExec` survives `GpuOverrides` as an already-GPU node. |
| Inert planning | Plans with no CuBF markers are unchanged by pre-`GpuOverrides` rules. |
| Probe metadata | Optional `bfId` and updater wiring does not affect canonicalization or normal predicate behavior. |
| Spark OSS regression | Existing runtime bloom filter tests for Spark `InjectRuntimeFilter` continue to pass unchanged. |

End-to-end query tests should be added when the logical/planner layer is present
in the public test environment. Until then, execution tests should keep using
public or test-only marker stubs.

## 11. Limitations and Resource Risks

This section summarizes the production resource risks that follow from the cost
analysis in Section 8, the remedies available in the current implementation, and
how to derive safe configuration values from cluster resources.

### 11.1 GPU Memory on Executors

**Risk.** GPU memory scales with concurrent tasks per executor. During the build
stage, each task holds `K` bloom filter GPU Scalars, so per-executor build memory
is `T * K * bf_bytes`. During the probe stage, each task holds `S` deserialized
bloom filters, so per-executor probe memory is `T * S * bf_bytes`. Build and
probe do not overlap in time for the same bloom filter, so peak GPU pressure is
the larger of the two:

```
GPU_peak_per_executor = T * max(K, S) * bf_bytes
```

`T` is the effective number of concurrent GPU tasks per executor (see Section 8.4
for the full definition), `K` is bloom filter specs per build exec, and `S` is
stacked bloom filter predicates per probe-side scan. At `bf_bytes=8 MB`, `T=16`,
and `max(K, S)=3`, peak footprint reaches 384 MB per executor, which is
significant on a 16-24 GB GPU.

**Remedy.** Probe-side bloom filters are wrapped in `SpillableBuffer` at
`ACTIVE_ON_DECK_PRIORITY`. Under GPU memory pressure, RAPIDS can spill bloom
filters to host memory and re-materialize them on demand, reducing steady-state
GPU pressure. However, initial deserialization allocates a device buffer before
wrapping, so sufficient GPU memory must be available at deserialization time. In
addition, re-materialization after spill requires a device allocation that can
fail under severe concurrent pressure. `SpillableBuffer` is a mitigation that
reduces long-lived GPU memory, not a guarantee against out-of-memory failures.

Build-side GPU Scalars are not spillable. They are held for the partition
lifetime and released at finalize.

**Safe configuration.** To keep peak GPU footprint under a target budget `G`:

```
bf_bytes_max <= G / (T * max(K, S))
```

For example, to stay under 256 MB on a 16 GB GPU with `T=16` and `max(K,S)=3`:
`bf_bytes_max <= 256 MB / 48 = about 5.3 MB`. Setting the planner's size cap at
or below 4 MB keeps the footprint under 192 MB with headroom.

In observed NDS SF3K workloads, all injected bloom filters were under 200 KB,
placing the operating point below 10 MB per executor even at `T*max(K,S)=48`.

### 11.2 Network Between Executors and Driver

**Risk.** Build-side accumulator shipping totals `P_build * K * bf_bytes` bytes
across all partitions for all K specs in one build exec. At `bf_bytes=8 MB`,
`K=3`, and `P_build=200`, this is 4.7 GB total. The bytes travel as part of
Spark task-completion messages, not as separate transfers, so they share the
driver RPC channel with all other accumulator traffic.

Each build task ships K serialized byte arrays, one per spec, as accumulator
updates inside `DirectTaskResult`. These contribute to Spark's task result size
accounting (`spark.driver.maxResultSize`, default 1 GB), which is an aggregate
cap across serialized task results for a job, not a per-task limit. However,
large per-task payloads also create RPC pressure on individual task-completion
messages. Per-task accumulator payload is `K * bf_bytes`. At the 256 MB V1
indexing ceiling fallback with `K=3`, worst-case per-task payload reaches
768 MB, which creates significant RPC pressure. With a planner-set size cap in
the low-MB range, per-task payload is well within normal limits.

**Remedy.** The size guard (`effectiveMaxFilterBytes`) rejects oversized bloom
filters before any GPU allocation or network transfer. The planner's size cap
bounds both total network volume and per-task payload.

**Safe configuration.** For total network budget `N_total` across all K specs:

```
bf_bytes_max <= N_total / (P_build * K)
```

For aggregate task result budget (accounting for `spark.driver.maxResultSize`):

```
P_build * K * bf_bytes <= task_result_budget
```

For per-task RPC payload:

```
K * bf_bytes <= per_task_payload_budget
```

Both budgets should include margin for normal task result values and other
accumulators. For example, to keep total network under 200 MB with
`P_build=200` and `K=3`: `bf_bytes_max <= 200 MB / 600 = about 340 KB`.
Dimension-side bloom filters, which are the common case, have `P_build` in the
single digits and produce negligible network traffic regardless of bloom filter
size.

### 11.3 Driver Heap and Merge CPU

**Risk.** The driver holds one merged `byte[]` per bloom filter id. For `B`
total bloom filters across all concurrent queries (each build exec contributes
K bloom filters), the driver heap for merged arrays is `B * bf_bytes`. Merge is
incremental, OR'ing each partition's data section into the existing array, so
peak heap is not `P_build` copies. However, merge runs single-threaded on the
DAGScheduler event loop. At `P_build=200` and `bf_bytes=8 MB`, the driver
processes `200 * 8 MB = 1.6 GB` of OR operations per bloom filter. At large
filter sizes this becomes visible driver CPU and GC work. The size cap should
keep merge volume within normal operating ranges for typical dimension-side
bloom filters.

**Remedy.** The size guard bounds `bf_bytes`. Standard JVM heap sizing applies
for concurrent bloom filters.

**Safe configuration.** Ensure:

```
driver_heap >= baseline + B * bf_bytes + margin
```

where `B` is the maximum number of bloom filters alive on the driver at once.
For a single query with `K=3` at 1 MB each: 3 MB of additional driver heap,
which is negligible. For 5 concurrent build execs with `K=3` at 8 MB each:
`B=15`, requiring 120 MB of additional driver heap.

### 11.4 Accumulator Cache Soft Leak

**Risk.** `BloomFilterBuildCostAccumulator.cache` and
`BloomFilterProbeAccumulator.cache` are static `ConcurrentHashMap` instances
with no cleanup path. Each bloom filter id adds one entry to each cache. In
long-running driver JVMs processing many queries, these maps grow without bound.
Each entry holds a metadata accumulator object (two `Long` fields plus
`AccumulatorV2` bookkeeping, string key, and map node overhead), not the bloom
filter bytes themselves. Per-entry retained size is on the order of a few
hundred bytes. Growth is slow relative to bloom filter payloads, but it is
unbounded and should be cleaned up in long-lived sessions.

**Remedy.** No cleanup mechanism exists today. This is tracked as future work
(Section 12, item 8). A listener-based cleanup keyed on query completion is the
expected fix.

### 11.5 Configuration Derivation Summary

The following table maps cluster resources to the configuration value to derive.
`T` is the effective number of concurrent GPU tasks per executor (see Section
8.4), `K` is specs per build exec, `S` is stacked predicates per probe scan,
`P_build` is build-side partitions, and `B` is total concurrent bloom filters on
the driver.

| Cluster resource | Constraint | Derive |
|------------------|------------|--------|
| GPU memory per executor | `T * max(K, S) * bf_bytes <= GPU_budget` | `bf_bytes_max = GPU_budget / (T * max(K, S))` |
| Network total | `P_build * K * bf_bytes <= network_budget` | `bf_bytes_max = network_budget / (P_build * K)` |
| Task result aggregate | `P_build * K * bf_bytes <= task_result_budget` | `bf_bytes_max = task_result_budget / (P_build * K)` |
| Per-task RPC payload | `K * bf_bytes <= per_task_payload_budget` | `bf_bytes_max = per_task_payload_budget / K` |
| Driver heap | `B * bf_bytes <= heap_budget` | `bf_bytes_max = heap_budget / B` |

The planner or user policy should set the bloom filter size cap to the minimum
of these derived values. Bloom filters above the configured cap should be
skipped before reaching GPU allocation or network transfer.

The RAPIDS execution layer provides a separate fail-closed guard
(`effectiveMaxFilterBytes`, resolved from an optional capability helper or the
format-specific indexing ceiling). This guard rejects bloom filters that exceed
the format or capability limit, but it is not intended as the primary knob for
cluster-specific resource budgets. The planner's size cap is the intended
configuration point for GPU, network, per-task payload, and driver heap
constraints.

In all dimensions, `bf_bytes` is the shared scaling factor. Small dimension-side
bloom filters (the common case, under 200 KB in NDS SF3K) are well within safe
bounds on any reasonable cluster configuration. The risk materializes only when
large fact-side bloom filters are permitted, and the planner's size cap exists
precisely to prevent that.

## 12. Future Work

The execution layer is designed to support the following extensions without
changing existing Spark OSS bloom filter behavior:

1. Logical injection rules that decide where CuBF markers should appear.
2. Join eligibility and benefit heuristics.
3. User-facing configuration for enabling CuBF policy, where appropriate.
4. Planner-side build and probe marker generation.
5. End-to-end query integration tests that require logical planning support.
6. Composite-key bloom filter support for multi-column join keys, including
   stable build/probe composite hashing, null semantics, type support, and
   marker/spec shape.
7. Runtime feedback, selectivity gating, or cost-benefit policies.
8. Accumulator cache cleanup for `BloomFilterBuildCostAccumulator` and
   `BloomFilterProbeAccumulator` to bound memory in long-running driver JVMs.
9. Additional profile enablement as Spark bloom filter support and shim coverage
   evolve.
