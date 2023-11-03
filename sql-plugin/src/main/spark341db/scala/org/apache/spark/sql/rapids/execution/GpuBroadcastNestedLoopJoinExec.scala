/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastNestedLoopJoinMetaBase(join, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)

    val condition = conditionMeta.map(_.convertToGpu())
    val isAstCondition = conditionMeta.forall(_.canThisBeAst)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case LeftOuter | RightOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        // Cannot post-filter these types of joins
        assert(isAstCondition, s"Non-AST condition in ${join.joinType}")
      case _ => throw new IllegalStateException(s"Unsupported join type ${join.joinType}")
    }

    val joinExec = GpuBroadcastNestedLoopJoinExec(
      left, right,
      join.joinType, gpuBuildSide,
      if (isAstCondition) condition else None,
      conf.gpuTargetBatchSizeBytes,
      join.isExecutorBroadcast)
    if (isAstCondition) {
      joinExec
    } else {
      // condition cannot be implemented via AST so fallback to a post-filter if necessary
      condition.map {
        // TODO: Restore batch coalescing logic here.
        // Avoid requesting a post-filter-coalesce here, as we've seen poor performance with
        // the cross join microbenchmark. This is a short-term hack for the benchmark, and
        // ultimately this should be solved with the resolution of one or more of the following:
        // https://github.com/NVIDIA/spark-rapids/issues/3749
        // https://github.com/NVIDIA/spark-rapids/issues/3750
        c => GpuFilterExec(c, joinExec)(coalesceAfter = false)
      }.getOrElse(joinExec)
    }
  }
}


case class GpuBroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    gpuBuildSide: GpuBuildSide,
    condition: Option[Expression],
    targetSizeBytes: Long,
    executorBroadcast: Boolean) extends GpuBroadcastNestedLoopJoinExecBase(
      left, right, joinType, gpuBuildSide, condition, targetSizeBytes
    ) {
  import GpuMetric._

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME)
  )

  def isExecutorBroadcast(): Boolean = {
    executorBroadcast
  }

  def shuffleExchange: GpuShuffleExchangeExec = buildPlan match {
    case bqse: ShuffleQueryStageExec if bqse.plan.isInstanceOf[GpuShuffleExchangeExec] =>
      bqse.plan.asInstanceOf[GpuShuffleExchangeExec]
    case bqse: ShuffleQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuShuffleExchangeExec]
    case gpu: GpuShuffleExchangeExec => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuShuffleExchangeExec]
  }

  override def getBroadcastRelation(): Any = {
    if (executorBroadcast) {
      // Get all the broadcast data from the shuffle coalesced into a single partition 
      val partitionSpecs = Seq(CoalescedPartitionSpec(0, shuffleExchange.numPartitions))
      shuffleExchange.getShuffleRDD(partitionSpecs.toArray, lazyFetching = true)
        .asInstanceOf[RDD[ColumnarBatch]]
    } else {
      broadcastExchange.executeColumnarBroadcast[Any]()
    }
  }

  // Ideally we cache the executor batch so we're not reading the shuffle multiple times. 
  // This requires caching the data and making it spillable/etc. This is okay for a smaller 
  // batch of data, but when this batch is bigger, this will make this significantly slower.
  // See https://github.com/NVIDIA/spark-rapids/issues/7599

  private[this] def makeExecutorBuiltBatch(
      rdd: RDD[ColumnarBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    val targetSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val metricsMap = allMetrics
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      val builtBatch = GpuExecutorBroadcastHelper.getExecutorBroadcastBatch(rdd, buildPlan.schema,
          buildPlan.output, metricsMap, targetSize)
      buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(builtBatch)
      builtBatch
    }
  }

  private[this] def computeExecutorBuildRowCount(
      rdd: RDD[ColumnarBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      buildDataSize += 0
      GpuExecutorBroadcastHelper.getExecutorBroadcastBatchNumRows(rdd)
    }
  }

  override def makeBuiltBatch(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    // NOTE: pattern matching doesn't work here because of type-invariance
    if (isExecutorBroadcast) {
      val rdd = relation.asInstanceOf[RDD[ColumnarBatch]]
      makeExecutorBuiltBatch(rdd, buildTime, buildDataSize)
    } else {
      val broadcastRelation = relation.asInstanceOf[Broadcast[Any]]
      makeBroadcastBuiltBatch(broadcastRelation, buildTime, buildDataSize)
    }
  }

  override def computeBuildRowCount(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    if (isExecutorBroadcast) {
      val rdd = relation.asInstanceOf[RDD[ColumnarBatch]]
      computeExecutorBuildRowCount(rdd, buildTime, buildDataSize)
    } else {
      val broadcastRelation = relation.asInstanceOf[Broadcast[Any]]
      computeBroadcastBuildRowCount(broadcastRelation, buildTime, buildDataSize)
    }
  }

}
