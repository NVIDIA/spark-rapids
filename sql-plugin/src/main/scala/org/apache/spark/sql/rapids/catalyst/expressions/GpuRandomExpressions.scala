/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, HostColumnVector, NvtxColor, NvtxRange}
import com.nvidia.spark.Retryable
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuLiteral}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionWithRandomSeed}
import org.apache.spark.sql.rapids.execution.RapidsAnalysisException
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.util.random.rapids.RapidsXORShiftRandom

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
case class GpuRand(child: Expression) extends ShimUnaryExpression with GpuExpression
  with ExpectsInputTypes with ExpressionWithRandomSeed with Retryable {

  def this() = this(GpuLiteral(Utils.random.nextLong(), LongType))

  override def withNewSeed(seed: Long): GpuRand = GpuRand(GpuLiteral(seed, LongType))

  def seedExpression: Expression = child

  override lazy val deterministic: Boolean = false
  override val selfNonDeterministic: Boolean = true

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: RapidsXORShiftRandom = _

  @transient protected lazy val seed: Long = child match {
    case GpuLiteral(s, IntegerType) => s.asInstanceOf[Int]
    case GpuLiteral(s, LongType) => s.asInstanceOf[Long]
    case _ => throw new RapidsAnalysisException(
      s"Input argument to $prettyName must be an integer, long or null literal.")
  }

  @transient protected var previousPartition: Int = 0

  @transient protected var curXORShiftRandomSeed: Option[Long] = None

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

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    if (curXORShiftRandomSeed.isEmpty) {
      // checkpoint not called, need to init the random generator here
      initRandom()
    } else {
      // make sure here uses the same random generator with checkpoint
      assert(wasInitialized)
    }
    withResource(new NvtxRange("GpuRand", NvtxColor.RED)) { _ =>
      val numRows = batch.numRows()
      withResource(HostColumnVector.builder(DType.FLOAT64, numRows)) { builder =>
        (0 until numRows).foreach(_ => builder.append(rng.nextDouble()))
        GpuColumnVector.from(builder.buildAndPutOnDevice(), dataType)
      }
    }
  }

  override def checkpoint(): Unit = {
    // In a task, checkpoint is called before columnarEval, so need to try to
    // init the random generator here.
    initRandom()
    curXORShiftRandomSeed = Some(rng.currentSeed)
  }

  override def restore(): Unit = {
    assert(wasInitialized && curXORShiftRandomSeed.isDefined)
    rng.setHashedSeed(curXORShiftRandomSeed.get)
  }
}
