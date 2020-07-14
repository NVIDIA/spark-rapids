/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{NvtxColor, Table}

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetricNames._
import org.apache.spark.sql.rapids.execution._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.internal.Logging


abstract class GpuBroadcastHashJoinExecBase30 extends BinaryExecNode with GpuHashJoin30 with Logging {
  
  def getBuildSide: GpuBuildSide

    protected lazy val (gpuBuildKeys, gpuStreamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    getBuildSide match {
      case GpuBuildLeft => (lkeys, rkeys)
      case GpuBuildRight => (rkeys, lkeys)
    }
  }

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "joinOutputRows" -> SQLMetrics.createMetric(sparkContext, "join output rows"),
    "joinTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "join time"),
    "filterTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "filter time"))

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    getBuildSide match {
      case GpuBuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case GpuBuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }
  def broadcastExchange: GpuBroadcastExchangeExec = buildPlan match {
    case gpu: GpuBroadcastExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExec]
  }

  override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("GpuBroadcastHashJoin does not support row-based processing")

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val totalTime = longMetric(TOTAL_TIME)
    val joinTime = longMetric("joinTime")
    val filterTime = longMetric("filterTime")
    val joinOutputRows = longMetric("joinOutputRows")

    val broadcastRelation = broadcastExchange
      .executeColumnarBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

    val boundCondition = condition.map(GpuBindReferences.bindReference(_, output))

    lazy val builtTable = {
      // TODO clean up intermediate results...
      val keys = GpuProjectExec.project(broadcastRelation.value.batch, gpuBuildKeys)
      val combined = combine(keys, broadcastRelation.value.batch)
      val ret = GpuColumnVector.from(combined)
      // Don't warn for a leak, because we cannot control when we are done with this
      (0 until ret.getNumberOfColumns).foreach(ret.getColumn(_).noWarnLeakExpected())
      ret
    }

    val rdd = streamedPlan.executeColumnar()
    rdd.mapPartitions(it =>
      doJoin(builtTable, it, boundCondition, numOutputRows, joinOutputRows,
        numOutputBatches, joinTime, filterTime, totalTime))
  }

  def doJoinInternal(builtTable: Table,
      streamedBatch: ColumnarBatch,
      boundCondition: Option[Expression],
      numOutputRows: SQLMetric,
      numJoinOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      joinTime: SQLMetric,
      filterTime: SQLMetric): Option[ColumnarBatch] = {

    val streamedTable = try {
      val streamedKeysBatch = GpuProjectExec.project(streamedBatch, gpuStreamedKeys)
      try {
        val combined = combine(streamedKeysBatch, streamedBatch)
        GpuColumnVector.from(combined)
      } finally {
        streamedKeysBatch.close()
      }
    } finally {
      streamedBatch.close()
    }

    val nvtxRange = new NvtxWithMetrics("hash join", NvtxColor.ORANGE, joinTime)
    val joined = try {
      getBuildSide match {
        case GpuBuildLeft => doJoinLeftRight(builtTable, streamedTable)
        case GpuBuildRight => doJoinLeftRight(streamedTable, builtTable)
      }
    } finally {
      streamedTable.close()
      nvtxRange.close()
    }

    numJoinOutputRows += joined.numRows()

    val tmp = if (boundCondition.isDefined) {
      GpuFilter(joined, boundCondition.get, numOutputRows, numOutputBatches, filterTime)
    } else {
      numOutputRows += joined.numRows()
      numOutputBatches += 1
      joined
    }
    if (tmp.numRows() == 0) {
      // Not sure if there is a better way to work around this
      numOutputBatches.set(numOutputBatches.value - 1)
      tmp.close()
      None
    } else {
      Some(tmp)
    }
  } 

}


class GpuBroadcastHashJoinMeta30(
    join: BroadcastHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends GpuHashJoinBaseMeta[BroadcastHashJoinExec](join, conf, parent, rule) with Logging {

  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val condition: Option[BaseExprMeta[_]] = join.condition.map(
    GpuOverrides.wrapExpr(_, conf, Some(this)))

  private def getBuildSide(join: BroadcastHashJoinExec): GpuBuildSide = {
    ShimLoader.getSparkShims.getBuildSide(join)
  }

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin30.tagJoin(this, join.joinType, join.leftKeys, join.rightKeys, join.condition)

    val buildSide = getBuildSide(join) match {
      case GpuBuildLeft => childPlans(0)
      case GpuBuildRight => childPlans(1)
    }

    if (!buildSide.canThisBeReplaced) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu("the BroadcastHashJoin this feeds is not on the GPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val left = childPlans(0).convertIfNeeded()
    val right = childPlans(1).convertIfNeeded()
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSide = getBuildSide(join) match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    if (!buildSide.isInstanceOf[GpuBroadcastExchangeExec]) {
      throw new IllegalStateException("the broadcast must be on the GPU too")
    }
    GpuBroadcastHashJoinExec30(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType, join.buildSide,
      condition.map(_.convertToGpu()),
      left, right)
  }

}
