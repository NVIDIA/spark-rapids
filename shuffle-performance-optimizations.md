# Recent Shuffle-Related Performance Work

This note summarizes recent shuffle-related work in the Spark RAPIDS / cuDF for
Spark repository, grouped by the kind of performance impact. It focuses on PRs
that either directly improve shuffle throughput or reduce memory, spill, OOM,
or metric-analysis overhead around shuffle-heavy workloads.

## Direct Shuffle Throughput Improvements

- [PR #13724](https://github.com/NVIDIA/cudf-spark/pull/13724) - Rapids shuffle manager V2 phase 1: writer use as much memory as allowed and pipelined write
  - Adds a pipelined writer path for RSM V2.
  - Introduces `SpillablePartialFileHandle`, which keeps partial shuffle files in host memory when possible and spills to disk under memory pressure.
  - Intended impact: reduce shuffle disk I/O and improve shuffle write throughput.

- [PR #14220](https://github.com/NVIDIA/cudf-spark/pull/14220) - Resubmit shuffle manager V2 phase 2
  - Resubmits [PR #14090](https://github.com/NVIDIA/cudf-spark/pull/14090) after the temporary revert in [PR #14219](https://github.com/NVIDIA/cudf-spark/pull/14219).
  - Skips the partial shuffle file merge when possible.
  - PR #14090 reported approximately 8.7% NDS overall improvement from phase 1 and about 18% early overall improvement from phase 1 + phase 2.

- [PR #14288](https://github.com/NVIDIA/cudf-spark/pull/14288) - Use dynamic shared buffer size in `KudoSerializedBatchIterator`
  - Replaces the fixed 20 MB shared buffer for small shuffle batches with a dynamic buffer size based on sampled batch size.
  - Intended impact: reduce host memory waste and avoid unnecessary spill on shuffle read.
  - PR validation on TPC-DS query93 with 1000 shuffle partitions showed host memory peak dropping from 75 GB to 40 GB and spill count dropping from 489 to 0.

## Memory, Spill, and OOM Improvements

- [PR #13975](https://github.com/NVIDIA/cudf-spark/pull/13975) - Support `withRetry` with split for shuffle exchange exec base
  - Converts shuffle exchange preparation state into a spliterator so GPU shuffle serialization can split and retry after OOM.
  - Intended impact: make large shuffle exchanges recoverable instead of failing on the first oversized attempt.

- [PR #14010](https://github.com/NVIDIA/cudf-spark/pull/14010) - Support `withRetry` with split for GPU shuffle coalesce
  - Adds split retry support to GPU shuffle coalesce.
  - Intended impact: improve behavior in low-memory or large coalesce scenarios.

- [PR #14684](https://github.com/NVIDIA/cudf-spark/pull/14684) - Split target size by data size if target size halving is a no-op
  - Fixes the split policy used by GPU shuffle coalesce and join gather paths.
  - If halving `targetSize` would not actually reduce the current data size, the retry target is derived from the estimated data size instead.
  - Intended impact: avoid repeated OOM retries that do not actually shrink the batch.

- [PR #14537](https://github.com/NVIDIA/cudf-spark/pull/14537) - Fix bounded retry logging for shuffle coalesce
  - Avoids huge `toString()` allocation when logging retry context for `CloseableTableSeqWithTargetSize`.
  - Intended impact: prevent Java heap OOM and GC-heavy slowdowns when thousands of shuffle partitions are coalesced in one batch.

- [PR #14466](https://github.com/NVIDIA/cudf-spark/pull/14466) - Use `Long` instead of `Int` to track Kudo buffer sizes and offsets in `GpuShuffleCoalesce`
  - Fixes 32-bit overflow in Kudo coalesce buffer-size and offset accounting.
  - Intended impact: improve correctness and stability for large shuffle coalesce batches near or above 2 GB accounting boundaries.

- [PR #14175](https://github.com/NVIDIA/cudf-spark/pull/14175) - Fix OOM when shuffle partition exceeds 2 GB in threaded writer
  - Avoids buffering all compressed records for one partition in a single large byte array.
  - Stores per-record buffers and lets the merger thread release them after writing.
  - Intended impact: handle skewed shuffle partitions without hitting Java array-size limits.

- [PR #14974](https://github.com/NVIDIA/cudf-spark/pull/14974) - Fail fast on greater-than-2 GB GPU-serialized shuffle batch
  - Prevents silent corruption from truncating serialized shuffle slice offsets to `Int`.
  - Intended impact: safer behavior for large GPU-serialized shuffle batches, especially when tuning batch size for throughput.

## Metrics and Bottleneck Analysis Improvements

- [PR #14936](https://github.com/NVIDIA/cudf-spark/pull/14936) - Exclude shuffle-read op time from consumers across AQE query stages
  - Fixes double-counting of reduce-side shuffle read `op time` under AQE.
  - The shuffle read time is reported by the exchange and should be excluded from the consuming operator's exclusive `op time`.
  - PR validation on TPC-H SF10 reduced per-stage `sum(op time + op time shuffle read) / executorRunTime` from about 1.16x to about 1.00x.

- [PR #14714](https://github.com/NVIDIA/cudf-spark/pull/14714) - Shuffle bytes double-count metric fix
  - Stops shuffle write bytes from being double-counted as spill when shuffle blocks are removed at unregister.
  - Intended impact: make shuffle/spill metrics more trustworthy for diagnosing disk and shuffle pressure.

## Stability and Cleanup Around RSM

- [PR #14513](https://github.com/NVIDIA/cudf-spark/pull/14513) - Fix thread-safety issue in `RapidsShuffleInternalManagerBase.unregisterShuffle`
  - Synchronizes iteration over map task IDs during shuffle unregister.
  - Intended impact: avoid races in long-running workloads with shuffle cleanup.

- [PR #14686](https://github.com/NVIDIA/cudf-spark/pull/14686) - Fix unregister/remove path for wrapped shuffle resolver
  - Ensures the wrapped `IndexShuffleBlockResolver` is used to remove shuffle files.
  - PR description notes that without the fix, NDS-H at SF1000 could leave roughly 50% more shuffle disk space in use.
  - Intended impact: prevent stale shuffle files and disk-full failures.

- [PR #14718](https://github.com/NVIDIA/cudf-spark/pull/14718) - Use `GpuShuffleBlockResolverBase.wrapped` in `unregisterShuffle`
  - Cleans up the wrapped resolver access path for proprietary dependency layouts.
  - Intended impact: keep shuffle cleanup behavior consistent across runtime/classpath variants.

- [PR #14525](https://github.com/NVIDIA/cudf-spark/pull/14525) - Fix OOM by correctly setting fanout
  - Explicitly disables fanout writer where catalog table properties were not being honored.
  - Intended impact: avoid Java heap OOM in affected shuffle write paths.

- [PR #14448](https://github.com/NVIDIA/cudf-spark/pull/14448) - Check RAPIDS shuffle configured instead of available before heartbeat init
  - Fixes UCX/RSM heartbeat initialization on Spark 4.x where `SparkEnv` is not ready during driver plugin init.
  - Intended impact: keep UCX shuffle startup reliable on Spark 4.x.

- [PR #14226](https://github.com/NVIDIA/cudf-spark/pull/14226) - Disable RAPIDS Shuffle Manager when `spark.shuffle.checksum.enabled=true`
  - Falls back to Spark `SortShuffleManager` when Spark row-based shuffle checksum is enabled.
  - Intended impact: avoid unsupported RSM execution mode on Spark versions/configurations that require row-based shuffle checksums.

## Short Summary

The most important direct performance work is RSM V2:

- Phase 1: pipelined write and memory-first partial shuffle storage, [PR #13724](https://github.com/NVIDIA/cudf-spark/pull/13724).
- Phase 2: skip partial file merge when possible, [PR #14220](https://github.com/NVIDIA/cudf-spark/pull/14220).

The next most important group is memory/spill reduction:

- Dynamic Kudo shared buffers, [PR #14288](https://github.com/NVIDIA/cudf-spark/pull/14288).
- Split retry for shuffle exchange/coalesce, [PR #13975](https://github.com/NVIDIA/cudf-spark/pull/13975), [PR #14010](https://github.com/NVIDIA/cudf-spark/pull/14010), and [PR #14684](https://github.com/NVIDIA/cudf-spark/pull/14684).
- Large partition / 2 GB boundary fixes, [PR #14175](https://github.com/NVIDIA/cudf-spark/pull/14175), [PR #14466](https://github.com/NVIDIA/cudf-spark/pull/14466), and [PR #14974](https://github.com/NVIDIA/cudf-spark/pull/14974).

For customer-facing performance analysis, [PR #14936](https://github.com/NVIDIA/cudf-spark/pull/14936) and [PR #14714](https://github.com/NVIDIA/cudf-spark/pull/14714) are important because they make shuffle timing and byte metrics easier to trust.
