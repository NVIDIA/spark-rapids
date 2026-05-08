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

package com.nvidia.spark.rapids

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.jni.{GpuSplitAndRetryOOM, RmmSpark}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuAdd
import org.apache.spark.sql.rapids.catalyst.expressions.GpuRand
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class ProjectSplitRetrySuite extends RmmSparkRetrySuiteBase {
  private val NUM_ROWS = 500
  private val RAND_SEED = 10
  private val intAttr = AttributeReference("int", IntegerType)(ExprId(10))
  private val batchAttrs = Seq(intAttr)

  // Reset retry counters so a leaked count from one test cannot mask a
  // missed injection in the next.
  override def afterEach(): Unit = {
    RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1)
    RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1)
    super.afterEach()
  }

  private def buildBatch(): ColumnarBatch = {
    val ints = 0 until NUM_ROWS
    new ColumnarBatch(
      Array(GpuColumnVector.from(ColumnVector.fromInts(ints: _*), IntegerType)),
      ints.length)
  }

  private def newSpillable(): SpillableColumnarBatch =
    SpillableColumnarBatch(buildBatch(), SpillPriorities.ACTIVE_ON_DECK_PRIORITY)

  // GpuAdd(int, 1) — pure, deterministic, retryable.
  private def addOneExprs(): Seq[GpuExpression] = Seq(
    GpuAlias(GpuAdd(
      GpuBoundReference(0, IntegerType, true)(NamedExpression.newExprId, "int"),
      GpuLiteral(1, IntegerType),
      failOnError = false)(),
      "plus_one")())

  private def mixedRandExprs(): Seq[GpuExpression] = Seq(
    GpuAlias(GpuAdd(
      GpuBoundReference(0, IntegerType, true)(NamedExpression.newExprId, "int"),
      GpuLiteral(1, IntegerType),
      failOnError = false)(), "plus_one")(),
    GpuAlias(GpuRand(GpuLiteral(RAND_SEED, IntegerType), false), "rnd")())

  private def collectInts(cb: ColumnarBatch, col: Int): Array[Int] = {
    val gcv = cb.column(col).asInstanceOf[GpuColumnVector]
    withResource(gcv.copyToHost()) { hcv =>
      (0 until cb.numRows()).map(hcv.getInt).toArray
    }
  }

  private def collectDoubles(cb: ColumnarBatch, col: Int): Array[Double] = {
    val gcv = cb.column(col).asInstanceOf[GpuColumnVector]
    withResource(gcv.copyToHost()) { hcv =>
      (0 until cb.numRows()).map(hcv.getDouble).toArray
    }
  }

  // Helper: build the SpillableColumnarBatch BEFORE injecting the OOM, so
  // that the alloc inside ColumnVector.fromInts doesn't accidentally absorb
  // the injection. Only the projection itself should trip the OOM.
  private def withInjectedOOM[T](inject: => Unit)(body: SpillableColumnarBatch => T): T = {
    val sb = newSpillable()
    inject
    body(sb)
  }

  test("split-retry produces same output as a single-batch projection") {
    val out = withInjectedOOM {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs())
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      assertResult(1)(cb.numCols())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0,
      "expected at least one SplitAndRetryOOM to have been observed")
  }

  test("conf=false routes split-retry OOM to legacy path which fails") {
    val sqlConf = new SQLConf()
    sqlConf.setConfString(RapidsConf.PROJECT_SPLIT_RETRY_ENABLED.key, "false")
    SQLConf.withExistingConf(sqlConf) {
      val sb = newSpillable()
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
      assertThrows[GpuSplitAndRetryOOM] {
        GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs()).close()
      }
    }
  }

  test("tiered project split-retry produces correct output") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      addOneExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(tier.areAllRetryable && tier.areAllDeterministic)
    val out = withInjectedOOM {
      RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      tier.projectAndCloseWithRetrySingleBatch(sb)
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  // A non-deterministic projection (containing GpuRand) must NOT take the
  // split path even when the conf is on, because row-splitting would
  // change rand state across the halves and break the row-aligned stitch
  // between deterministic and rand columns. forceRetryOOM (plain, not
  // split) verifies the legacy withRetryNoSplit path is selected and
  // checkpoint/restore reproduces the rand sequence on retry.
  test("mixed deterministic + GpuRand falls back to legacy retry path") {
    val batches = Seq(true, false).safeMap { forceRetry =>
      val tier = GpuBindReferences.bindGpuReferencesTiered(
        mixedRandExprs(), batchAttrs, new SQLConf(), Map.empty)
      assert(tier.areAllRetryable && !tier.areAllDeterministic)
      val sb = newSpillable()
      if (forceRetry) {
        RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
          RmmSpark.OomInjectionType.GPU.ordinal, 0)
      }
      tier.projectAndCloseWithRetrySingleBatch(sb)
    }
    withResource(batches) { case Seq(retried, ref) =>
      assertResult(ref.numRows())(retried.numRows())
      assertResult(ref.numCols())(retried.numCols())
      val refPlus = collectInts(ref, 0)
      val retPlus = collectInts(retried, 0)
      val refRand = collectDoubles(ref, 1)
      val retRand = collectDoubles(retried, 1)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(refPlus(i))(retPlus(i))
        assertResult(refRand(i))(retRand(i))
      }
    }
  }

  test("flat mixed deterministic + GpuRand stays off split-retry path") {
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, mixedRandExprs()).close()
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  test("tiered mixed deterministic + GpuRand stays off split-retry path") {
    val tier = GpuBindReferences.bindGpuReferencesTiered(
      mixedRandExprs(), batchAttrs, new SQLConf(), Map.empty)
    assert(tier.areAllRetryable && !tier.areAllDeterministic)
    val sb = newSpillable()
    RmmSpark.forceSplitAndRetryOOM(RmmSpark.getCurrentThreadId, 1,
      RmmSpark.OomInjectionType.GPU.ordinal, 0)
    assertThrows[GpuSplitAndRetryOOM] {
      tier.projectAndCloseWithRetrySingleBatch(sb).close()
    }
    assert(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1) > 0)
  }

  // A plain GpuRetryOOM under the new path is resolved before the splitter
  // is invoked, so the result comes back as a single piece — exercising the
  // single-piece path through ConcatAndConsumeAll.buildNonEmptyBatchFromTypes.
  test("plain GpuRetryOOM under split-retry path returns a single piece") {
    val out = withInjectedOOM {
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId, 1,
        RmmSpark.OomInjectionType.GPU.ordinal, 0)
    } { sb =>
      GpuProjectExec.projectAndCloseWithRetrySingleBatch(sb, addOneExprs())
    }
    withResource(out) { cb =>
      assertResult(NUM_ROWS)(cb.numRows())
      val got = collectInts(cb, 0)
      (0 until NUM_ROWS).foreach { i =>
        assertResult(i + 1)(got(i))
      }
    }
    assertResult(0)(RmmSpark.getAndResetNumSplitRetryThrow(/*taskId*/ 1))
    assert(RmmSpark.getAndResetNumRetryThrow(/*taskId*/ 1) > 0)
  }
}
