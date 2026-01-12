/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.catalyst.expressions

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuLiteral, NvtxRegistry, RetryStateTracker}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionWithRandomSeed}
import org.apache.spark.sql.rapids.execution.RapidsAnalysisException
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.random.rapids.RapidsXORShiftRandom

/**
 * An expression expected to be evaluated inside a retry with checkpoint-restore context.
 * It will throw an exception if it is retried without being checkpointed.
 * All the nondeterministic GPU expressions that support Retryable should extend from
 * this trait.
 */
trait GpuExpressionRetryable extends GpuExpression with Retryable {
  private var checked = false

  def doColumnarEval(batch: ColumnarBatch): GpuColumnVector
  def doCheckpoint(): Unit
  def doRestore(): Unit

  def doContextCheck(): Boolean // For tests

  override final def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (doContextCheck && !checked) { // This is for tests
      throw new IllegalStateException(
        "The Retryable was called outside of a checkpoint-restore context")
    }
    if (!checked && RetryStateTracker.isCurThreadRetrying) {
      // It is retrying the evaluation without checkpointing, which is not allowed.
      throw new IllegalStateException(
        "The Retryable should be retried only inside a checkpoint-restore context")
    }
    doColumnarEval(batch)
  }

  override final def checkpoint(): Unit = {
    checked = true
    doCheckpoint()
  }

  override final def restore(): Unit = doRestore()
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
case class GpuRand(child: Expression, doContextCheck: Boolean) extends ShimUnaryExpression
  with ExpectsInputTypes with ExpressionWithRandomSeed with GpuExpressionRetryable {

  def this(doContextCheck: Boolean) = this(GpuLiteral(Utils.random.nextLong(), LongType),
    doContextCheck)

  override def withNewSeed(seed: Long): GpuRand = GpuRand(GpuLiteral(seed, LongType),
    doContextCheck)

  // Added in Spark 4.1.0
  def withShiftedSeed(shift: Long): Expression = {
    val newSeed = child match {
      case GpuLiteral(s, IntegerType) => s.asInstanceOf[Int].toLong + shift
      case GpuLiteral(s, LongType) => s.asInstanceOf[Long] + shift
      case _ => shift
    }
    withNewSeed(newSeed)
  }

  def seedExpression: Expression = child

  override lazy val deterministic: Boolean = false
  override val selfNonDeterministic: Boolean = true

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: RapidsXORShiftRandom = _

  private lazy val seed: Long = child match {
    case GpuLiteral(s, IntegerType) => s.asInstanceOf[Int]
    case GpuLiteral(s, LongType) => s.asInstanceOf[Long]
    case _ => throw new RapidsAnalysisException(
      s"Input argument to $prettyName must be an integer, long or null literal.")
  }

  private var previousPartition: Int = 0

  private var curXORShiftRandomSeed: Option[Long] = None

  private def wasInitialized: Boolean = rng != null

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegerType, LongType))

  private def initRandom(): Unit = {
    val partId = TaskContext.getPartitionId()
    if (partId != previousPartition || !wasInitialized) {
      rng = new RapidsXORShiftRandom(seed + partId)
      previousPartition = partId
    }
  }

  override def doColumnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (curXORShiftRandomSeed.isEmpty) {
      // checkpoint not called, need to init the random generator here
      initRandom()
    } else {
      // make sure here uses the same random generator with checkpoint
      assert(wasInitialized)
    }
    NvtxRegistry.RANDOM_EXPR {
      val numRows = batch.numRows()
      withResource(HostColumnVector.builder(DType.FLOAT64, numRows)) { builder =>
        (0 until numRows).foreach(_ => builder.append(rng.nextDouble()))
        GpuColumnVector.from(builder.buildAndPutOnDevice(), dataType)
      }
    }
  }

  override def doCheckpoint(): Unit = {
    // In a task, checkpoint is called before columnarEval, so need to try to
    // init the random generator here.
    initRandom()
    curXORShiftRandomSeed = Some(rng.currentSeed)
  }

  override def doRestore(): Unit = {
    assert(wasInitialized && curXORShiftRandomSeed.isDefined)
    rng.setHashedSeed(curXORShiftRandomSeed.get)
  }
}
