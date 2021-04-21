/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric.{NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftExistence, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.GpuNoColumnCrossJoin
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val condition: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = condition.toSeq

  override def tagPlanForGpu(): Unit = {
    join.joinType match {
      case Inner =>
      case Cross =>
      case _ => willNotWorkOnGpu(s"$join.joinType currently is not supported")
    }

    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    val Seq(leftPlan, rightPlan) = childPlans
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => leftPlan
      case GpuBuildRight => rightPlan
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu(
        "the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)
    ShimLoader.getSparkShims.getGpuBroadcastNestedLoopJoinShim(
      left, right, join,
      join.joinType,
      condition.map(_.convertToGpu()),
      conf.gpuTargetBatchSizeBytes)
  }
}

object GpuBroadcastNestedLoopJoinExecBase extends Arm {
  def innerLikeJoin(
      streamedIter: Iterator[ColumnarBatch],
      builtTable: Table,
      buildSide: GpuBuildSide,
      boundCondition: Option[GpuExpression],
      outputSchema: Array[DataType],
      joinTime: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    streamedIter.map { cb =>
      val startTime = System.nanoTime()
      val streamTable = withResource(cb) { cb =>
        GpuColumnVector.from(cb)
      }
      val joined =
        withResource(new NvtxWithMetrics("join", NvtxColor.ORANGE, joinTime)) { _ =>
          val joinedTable = withResource(streamTable) { tab =>
            buildSide match {
              case GpuBuildLeft => builtTable.crossJoin(tab)
              case GpuBuildRight => tab.crossJoin(builtTable)
            }
          }
          withResource(joinedTable) { jt =>
            GpuColumnVector.from(jt, outputSchema)
          }
        }
      joinOutputRows += joined.numRows()
      val ret = if (boundCondition.isDefined) {
        GpuFilter(joined, boundCondition.get, numOutputRows, numOutputBatches, filterTime)
      } else {
        numOutputRows += joined.numRows()
        numOutputBatches += 1
        joined
      }
      totalTime += (System.nanoTime() - startTime)
      ret
    }
  }
}

abstract class GpuBroadcastNestedLoopJoinExecBase(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long) extends BinaryExecNode with GpuExec {

  import GpuMetric._

  // Spark BuildSide, BuildRight, BuildLeft changed packages between Spark versions
  // so return a GPU version that is agnostic to the Spark version.
  def getGpuBuildSide: GpuBuildSide

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    FILTER_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_FILTER_TIME))

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = getGpuBuildSide match {
    case GpuBuildRight => (left, right)
    case GpuBuildLeft => (right, left)
  }

  def broadcastExchange: GpuBroadcastExchangeExecBase = broadcast match {
    case BroadcastQueryStageExec(_, gpu: GpuBroadcastExchangeExecBase) => gpu
    case BroadcastQueryStageExec(_, reused: ReusedExchangeExec) =>
      reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
    case gpu: GpuBroadcastExchangeExecBase => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
  }

  override def requiredChildDistribution: Seq[Distribution] = getGpuBuildSide match {
    case GpuBuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case GpuBuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  private[this] def makeBuiltTable(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Table = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      val ret = GpuColumnVector.from(broadcastRelation.value.batch)
      // Don't warn for a leak, because we cannot control when we are done with this
      (0 until ret.getNumberOfColumns).foreach(i => {
        val column = ret.getColumn(i)
        column.noWarnLeakExpected()
        buildDataSize += column.getDeviceMemorySize
      })
      ret
    }
  }

  private[this] def computeBuildRowCount(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      buildDataSize += 0
      broadcastRelation.value.batch.numRows()
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val filterTime = gpuLongMetric(FILTER_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)

    val boundCondition = condition.map(GpuBindReferences.bindGpuReference(_, output))

    val buildTime = gpuLongMetric(BUILD_TIME)
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)

    val outputSchema = output.map(_.dataType).toArray

    joinType match {
      case _: InnerLike => // The only thing we support right now
      case _ => throw new IllegalArgumentException(s"$joinType + $getGpuBuildSide is not" +
          " supported and should be run on the CPU")
    }

    val broadcastRelation =
      broadcastExchange.executeColumnarBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

    if (output.isEmpty) {
      assert(boundCondition.isEmpty)

      lazy val buildCount: Int = computeBuildRowCount(broadcastRelation, buildTime, buildDataSize)

      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val counts = streamed.executeColumnar().map(getRowCountAndClose)
      GpuNoColumnCrossJoin.divideIntoBatches(
        counts.map(s => s * buildCount),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    } else if (broadcast.output.isEmpty) {
      assert(boundCondition.isEmpty)

      lazy val buildCount: Int = computeBuildRowCount(broadcastRelation, buildTime, buildDataSize)

      streamed.executeColumnar().mapPartitions { streamedIter =>
        streamedIter.flatMap { cb =>
          withResource(cb) { cb =>
            withResource(GpuColumnVector.from(cb)) { table =>
              GpuNoColumnCrossJoin.divideIntoBatches(
                table,
                buildCount,
                outputSchema,
                numOutputRows,
                numOutputBatches)
            }
          }
        }
      }
    } else if (streamed.output.isEmpty) {
      assert(boundCondition.isEmpty)

      // streamed is empty, not sure if this ever actually happens though
      lazy val builtTable: Table = makeBuiltTable(broadcastRelation, buildTime, buildDataSize)
      streamed.executeColumnar().flatMap { cb =>
        withResource(cb) { cb =>
          GpuNoColumnCrossJoin.divideIntoBatches(
            builtTable,
            cb.numRows(),
            outputSchema,
            numOutputRows,
            numOutputBatches)
        }
      }
    } else {
      lazy val builtTable: Table = makeBuiltTable(broadcastRelation, buildTime, buildDataSize)
      streamed.executeColumnar().mapPartitions { streamedIter =>
        GpuBroadcastNestedLoopJoinExecBase.innerLikeJoin(streamedIter,
          builtTable, getGpuBuildSide, boundCondition, outputSchema,
          joinTime, joinOutputRows, numOutputRows, numOutputBatches, filterTime, totalTime)
      }
    }
  }
}

