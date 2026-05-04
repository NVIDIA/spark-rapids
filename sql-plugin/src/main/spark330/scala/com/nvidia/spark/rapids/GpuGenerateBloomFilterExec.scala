/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "411"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnView, HostMemoryBuffer, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.{BloomFilter, Hash}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.AccumulatorV2

/**
 * Per-BF build specification carried by the multi-spec operator.
 *
 * This matches the shape exposed by the optional inline-build
 * planner node. `InlineBFBuildReplacement` copies those fields via
 * reflection so there is no compile-time dependency on optional
 * planner classes.
 *
 * @param bfId            unique ID linking build and probe sides
 * @param keyColumnIndex  index of the build key in the output schema
 * @param numHashes       number of hash functions for this BF
 * @param numBits         total bits in this BF
 */
case class BFSpec(
    bfId: String,
    keyColumnIndex: Int,
    numHashes: Int,
    numBits: Long)

/**
 * Pass-through GPU operator that builds N bloom filters inline with
 * the join's build-side pipeline in a single columnar pass.
 *
 * All columnar batches are passed through unchanged. For each batch,
 * the operator iterates `specs` and, per spec, extracts the build key
 * column (at `spec.keyColumnIndex`), hashes it with XxHash64, and
 * inserts the hashed values into a per-partition per-spec GPU bloom
 * filter. After all batches in a partition are consumed, each spec's
 * BF is transferred to host memory and added to that spec's
 * `BloomFilterBuildAccumulator`.
 *
 * One accumulator is registered per spec and captured per task by
 * closure, so each partition publishes its partial bloom filter to
 * the matching `bfId`.
 *
 * Every GPU Scalar handle is closed on success and failure. The
 * iterator's normal exhaust path closes via `finalizeAllBFs`; a
 * `TaskCompletionListener` covers task failure or interruption when
 * the iterator is abandoned mid-batch.
 *
 * `buildCostUpdaters` is optional observability wiring. The default
 * `Map.empty` keeps this path inert until a planner explicitly
 * supplies updaters.
 *
 * @param specs              per-BF build specs (size >= 1)
 * @param bfVersion          BF format version (1 for Spark 3.x, 2 for 4.x)
 * @param seed               hash seed (V2 only; 0 for V1; shared across specs)
 * @param xxHashSeed         XxHash64 seed (matches probe-side hash; shared)
 * @param child              the build-side plan
 * @param buildCostUpdaters  optional per-bfId observability sink
 */
case class GpuGenerateBloomFilterExec(
    specs: Seq[BFSpec],
    bfVersion: Int,
    seed: Int,
    xxHashSeed: Long,
    child: SparkPlan,
    buildCostUpdaters: Map[String, BloomFilterBuildCostUpdater] = Map.empty)
    extends ShimUnaryExecNode with GpuExec with Logging {

  require(specs.nonEmpty,
    "GpuGenerateBloomFilterExec requires at least one BFSpec")

  override def output: Seq[Attribute] = child.output

  // This operator does not change batch sizes; no coalescing needed.
  override val coalesceAfter: Boolean = false

  // Keep this pass-through wrapper invisible to Spark's canonical-form
  // plan matchers. The optional planner is responsible for only
  // coalescing equivalent inline-build specifications.
  override protected def doCanonicalize(): SparkPlan = child.canonicalized

  /**
   * One accumulator per spec, keyed by `bfId`. Driver-side lazy val:
   * the first access (from `internalDoExecuteColumnar` on the driver,
   * before task dispatch) registers every accumulator with the
   * SparkContext under the name `cuBF-<bfId>`.
   */
  @transient lazy val accumulators: Map[String, BloomFilterBuildAccumulator] = {
    val sc = sparkContext
    specs.map { spec =>
      val acc = new BloomFilterBuildAccumulator()
      sc.register(acc, s"cuBF-${spec.bfId}")
      (spec.bfId, acc)
    }.toMap
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // Capture fields for serialization (avoid capturing 'this').
    val specsCapture = specs
    val bfVer = bfVersion
    val bfSeed = seed
    val hashSeed = xxHashSeed
    val accMap = accumulators // triggers eager driver-side registration
    val updatersCapture = buildCostUpdaters

    // Compute the effective BF byte ceiling once on the driver. Any
    // spec whose byte footprint exceeds that ceiling is marked as
    // skipped in its accumulator without kicking off a GPU build.
    val effMaxBytes = GpuGenerateBloomFilterExec.resolveEffectiveMaxFilterBytes()
    val oversizedBfIds: Set[String] = specsCapture.flatMap { spec =>
      val bfBytes = (spec.numBits + 7L) / 8L
      if (bfBytes > effMaxBytes) {
        logWarning(s"[CuBF-GpuGenerate] OVERSHOOT bfId=${spec.bfId} " +
          s"bfBytes=$bfBytes > max=$effMaxBytes -> SKIP")
        accMap(spec.bfId).markSkipped()
        Some(spec.bfId)
      } else None
    }.toSet

    val oversizedCapture = oversizedBfIds
    child.executeColumnar().mapPartitions { iter =>
      val ctx = TaskContext.get()
      val partId = ctx.partitionId()
      val numSpecs = specsCapture.size
      val bfs: Array[Scalar] = new Array[Scalar](numSpecs)
      val rowsProcessed: Array[Long] = Array.fill(numSpecs)(0L)
      val skipSpec: Array[Boolean] = specsCapture
        .map(s => oversizedCapture.contains(s.bfId)).toArray
      @volatile var finalized = false
      // Build-cost timer: start at first non-empty input batch, stop at finalize. One
      // update per `(bfId, partition)` pair regardless of batch count.
      var taskStartNanos: Long = 0L

      def closeAllBfs(): Unit = {
        var i = 0
        while (i < bfs.length) {
          val bf = bfs(i)
          if (bf != null) {
            try bf.close() catch {
              case e: Throwable =>
                logWarning(s"[CuBF-GpuBuild] bfId=" +
                  s"${specsCapture(i).bfId} partition=$partId " +
                  s"close failed: ${e.getMessage}")
            }
            bfs(i) = null
          }
          i += 1
        }
      }

      // Belt-and-suspenders: close every BF if the task fails or is
      // interrupted mid-batch (normal exhaust path also closes, but
      // only when hasNext returns false).
      ctx.addTaskCompletionListener[Unit] { _ => closeAllBfs() }

      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val has = iter.hasNext
          if (!has && !finalized) {
            finalized = true
            finalizeAllBFs()
          }
          has
        }

        override def next(): ColumnarBatch = {
          val batch = iter.next()
          if (batch.numRows() > 0) {
            if (taskStartNanos == 0L) {
              taskStartNanos = System.nanoTime()
            }
            var i = 0
            while (i < numSpecs) {
              val spec = specsCapture(i)
              if (!skipSpec(i) && batch.numCols() > spec.keyColumnIndex) {
                if (bfs(i) == null) {
                  bfs(i) = BloomFilter.create(bfVer,
                    spec.numHashes, spec.numBits, bfSeed)
                }
                val keyCol = batch.column(spec.keyColumnIndex)
                  .asInstanceOf[GpuColumnVector].getBase
                // Hash with XxHash64 before inserting into the BF.
                // The probe side uses BloomFilterMightContain(bf,
                // XxHash64(probeKey)), so the build side must use the
                // same hash to produce matching bit positions.
                withResource(Hash.xxhash64(hashSeed,
                    Array[ColumnView](keyCol))) { hashedCol =>
                  BloomFilter.put(bfs(i), hashedCol)
                }
                rowsProcessed(i) += batch.numRows()
              }
              i += 1
            }
          }
          batch // pass through unchanged
        }

        private def finalizeAllBFs(): Unit = {
          try {
            // Compute wall time once per finalize and reuse it for every spec
            // in this task.
            val wallNanos = if (taskStartNanos != 0L) {
              System.nanoTime() - taskStartNanos
            } else 0L
            var i = 0
            while (i < numSpecs) {
              val spec = specsCapture(i)
              if (skipSpec(i)) {
                // Overshoot: driver already called markSkipped().
                // Tasks do not ship a partial BF for this spec.
                logInfo(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                  s"partition=$partId SKIPPED (overshoot)")
              } else {
                val bf = bfs(i)
                if (bf != null) {
                  val bytes =
                    GpuGenerateBloomFilterExec.scalarToHostBytes(bf)
                  accMap(spec.bfId).add(bytes)
                  logInfo(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                    s"partition=$partId SENT ${bytes.length} bytes " +
                    s"(${rowsProcessed(i)} rows)")
                  // One update per `(bfId, partition)` pair; skipped and empty
                  // partitions do not contribute build cost.
                  updatersCapture.get(spec.bfId).foreach { u =>
                    u.update(wallNanos, bytes.length.toLong)
                  }
                } else {
                  logWarning(s"[CuBF-GpuBuild] bfId=${spec.bfId} " +
                    s"partition=$partId EMPTY " +
                    s"(0 rows processed, no BF constructed)")
                }
              }
              i += 1
            }
          } finally {
            closeAllBfs()
          }
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuGenerateBloomFilterExec does not support row-based execution")
  }

  /**
   * Per-build update sink for `buildCostUpdaters`. Visible for test:
   * the GPU-free unit test calls this directly with synthetic counts
   * to assert the once-per-build invariant structurally.
   * Production runtime calls the equivalent inline at finalize
   * (avoiding `this` capture in the executor closure).
   */
  private[rapids] def recordBuildUpdate(
      bfId: String, buildWallNanos: Long, bfBytes: Long): Unit = {
    buildCostUpdaters.get(bfId).foreach(_.update(buildWallNanos, bfBytes))
  }
}

object GpuGenerateBloomFilterExec extends Logging {

  private val SparkVersionBFCapsClassName =
    "com.nvidia.spark.rapids.optimizer.cubloomfilter.SparkVersionBFCaps$"

  /**
   * V1 BloomFilterImpl indexing ceiling (~256 MB), used as the
   * fail-safe fallback when the optional capability helper cannot
   * be resolved.
   */
  private val V1IndexingCeilingBytes: Long = (1L << 31) / 8L

   /**
   * Driver-side lookup of `SparkVersionBFCaps.effectiveMaxFilterBytes(Long)`.
   *
   * Resolved reflectively to avoid a compile-time classpath dependency
   * on optional planner code. If reflection fails, return the V1
   * indexing ceiling instead of `Long.MaxValue`, keeping the overshoot
   * guard fail-closed.
   */
  def resolveEffectiveMaxFilterBytes(): Long = {
    try {
      val cls = Class.forName(SparkVersionBFCapsClassName)
      val module = cls.getField("MODULE$").get(null)
      val method = cls.getMethod("effectiveMaxFilterBytes", classOf[Long])
      method.invoke(module, java.lang.Long.valueOf(Long.MaxValue))
        .asInstanceOf[java.lang.Long].longValue()
    } catch {
      case e: Throwable =>
        logWarning(s"[CuBF-GpuGenerate] Could not resolve " +
          s"SparkVersionBFCaps.effectiveMaxFilterBytes via reflection: " +
          s"${e.getMessage}. Falling back to V1 ceiling " +
          s"($V1IndexingCeilingBytes bytes).")
        V1IndexingCeilingBytes
    }
  }

  /**
   * Copy a GPU bloom filter Scalar to host byte array.
   * The Scalar is a LIST(UINT8) containing the full serialized BF
   * (header + bit data) in Spark's BloomFilter wire format.
   */
  def scalarToHostBytes(bf: Scalar): Array[Byte] = {
    withResource(bf.getListAsColumnView) { view =>
      val devBuf = view.getData
      val len = devBuf.getLength.toInt
      withResource(HostMemoryBuffer.allocate(len)) { hostBuf =>
        hostBuf.copyFromDeviceBuffer(devBuf)
        val bytes = new Array[Byte](len)
        hostBuf.getBytes(bytes, 0, 0, len)
        bytes
      }
    }
  }
}

/**
 * GpuOverrides registration for GpuGenerateBloomFilterExec.
 *
 * Since InlineBFBuildReplacement converts InlineBFBuildExec to
 * GpuGenerateBloomFilterExec BEFORE GpuOverrides runs, GpuOverrides
 * needs to recognize GpuGenerateBloomFilterExec as an already-GPU
 * node. This registers it with a pass-through meta: tagPlanForGpu()
 * is a no-op (always eligible) and convertToGpu() returns the node
 * unchanged.
 */
object InlineBFBuildGpuOverride {
  val execRules: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
    GpuOverrides.exec[GpuGenerateBloomFilterExec](
      "Pass-through GPU operator that builds a bloom filter inline " +
        "with the join's build-side pipeline",
      ExecChecks(TypeSig.all, TypeSig.all),
      (exec, conf, parent, rule) =>
        new SparkPlanMeta[GpuGenerateBloomFilterExec](exec, conf, parent, rule) {
          override def tagPlanForGpu(): Unit = {}
          override def convertToGpu(): GpuExec = exec
        }
    )
  ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
}

/**
 * AccumulatorV2 that merges partial bloom filters via bitwise OR.
 *
 * Each partition adds one partial BF (serialized as byte array).
 * The driver merges them incrementally: for each incoming partial,
 * it ORs the data portion (skipping the header) into the current
 * merged value.
 *
 * The header (version, numHashes, optional seed, numWords) is
 * identical across all partials since they were created with the
 * same parameters.
 *
 * Thread safety: AccumulatorV2 merge() is called from the
 * DAGScheduler event loop, serialized per accumulator.
 *
 * A task may publish a 4-byte all-zero skip sentinel via `add()` or
 * the exec's overshoot path may call `markSkipped()` directly. Once
 * any partition reports the sentinel, subsequent adds/merges leave
 * the accumulator in the sentinel state (skip wins). A real BF
 * payload cannot collide with this shape: every serialized
 * `BloomFilter` starts with a non-zero 4-byte version header.
 */
class BloomFilterBuildAccumulator extends AccumulatorV2[Array[Byte], Array[Byte]] {

  import BloomFilterBuildAccumulator.SkipSentinel

  private var merged: Array[Byte] = _

  override def isZero: Boolean = merged == null

  override def copy(): AccumulatorV2[Array[Byte], Array[Byte]] = {
    val acc = new BloomFilterBuildAccumulator()
    if (merged != null) {
      // Preserve sentinel identity on copy; never clone a sentinel
      // reference into a fresh array; the driver-side merge relies
      // on identity short-circuits.
      acc.merged = if (merged eq SkipSentinel) SkipSentinel else merged.clone()
    }
    acc
  }

  override def reset(): Unit = {
    merged = null
  }

  /** Publish the skip sentinel into this accumulator. Used by the
   *  build exec when overshoot is detected pre-kernel-launch. */
  def markSkipped(): Unit = {
    merged = SkipSentinel
  }

  override def add(partial: Array[Byte]): Unit = {
    if (isSkipShape(partial) || isSkipShape(merged)) {
      merged = SkipSentinel
    } else if (merged == null) {
      merged = partial.clone()
    } else {
      mergeBytes(partial)
    }
  }

  override def merge(other: AccumulatorV2[Array[Byte], Array[Byte]]): Unit = {
    val otherAcc = other.asInstanceOf[BloomFilterBuildAccumulator]
    if (otherAcc.merged != null) {
      add(otherAcc.merged)
    }
  }

  override def value: Array[Byte] = merged

  /** Length-4 all-zero check covers both the driver-local sentinel
   *  identity and the deserialized content-equivalent form after
   *  task-to-driver accumulator shipping. */
  private def isSkipShape(bytes: Array[Byte]): Boolean = {
    if (bytes == null) false
    else if (bytes eq SkipSentinel) true
    else bytes.length == 4 &&
      bytes(0) == 0.toByte && bytes(1) == 0.toByte &&
      bytes(2) == 0.toByte && bytes(3) == 0.toByte
  }

  /**
   * Bitwise OR the data portion of `other` into `merged`.
   * The header is skipped (identical across all partials).
   */
  private def mergeBytes(other: Array[Byte]): Unit = {
    require(merged.length == other.length,
      s"BF size mismatch: ${merged.length} vs ${other.length}")
    // Detect header size from version byte
    val version = ((merged(0) & 0xFF) << 24) | ((merged(1) & 0xFF) << 16) |
      ((merged(2) & 0xFF) << 8) | (merged(3) & 0xFF)
    val dataOffset = if (version == 2) 16 else 12
    var i = dataOffset
    while (i < merged.length) {
      merged(i) = (merged(i) | other(i)).toByte
      i += 1
    }
  }
}

object BloomFilterBuildAccumulator {
  /**
   * Driver-local skip-sentinel identity. The registry side recognizes
   * the sentinel by content as well as identity, so accumulator state
   * shipped executor-to-driver is still detected even after Spark's
   * serialization breaks the reference link.
   */
  val SkipSentinel: Array[Byte] = Array[Byte](0, 0, 0, 0)
}
