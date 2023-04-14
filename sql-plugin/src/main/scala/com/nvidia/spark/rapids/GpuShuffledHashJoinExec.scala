/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, ShimBinaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.GpuOr
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, GpuSubPartitionHashJoin, JoinTypeChecks}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuShuffledHashJoinMeta(
    join: ShuffledHashJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[ShuffledHashJoinExec](join, conf, parent, rule) {
  val leftKeys: Seq[BaseExprMeta[_]] =
    join.leftKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val rightKeys: Seq[BaseExprMeta[_]] =
    join.rightKeys.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val buildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override val childExprs: Seq[BaseExprMeta[_]] = leftKeys ++ rightKeys ++ conditionMeta

  override val namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.equiJoinMeta(leftKeys, rightKeys, conditionMeta)

  override def tagPlanForGpu(): Unit = {
    GpuHashJoin.tagJoin(this, join.joinType, buildSide, join.leftKeys, join.rightKeys,
      conditionMeta)
  }

  override def convertToGpu(): GpuExec = {
    val condition = conditionMeta.map(_.convertToGpu())
    val (joinCondition, filterCondition) = if (conditionMeta.forall(_.canThisBeAst)) {
      (condition, None)
    } else {
      (None, condition)
    }
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    val joinExec = GpuShuffledHashJoinExec(
      leftKeys.map(_.convertToGpu()),
      rightKeys.map(_.convertToGpu()),
      join.joinType,
      buildSide,
      joinCondition,
      left,
      right,
      isSkewJoin = false)(
      join.leftKeys,
      join.rightKeys)
    // For inner joins we can apply a post-join condition for any conditions that cannot be
    // evaluated directly in a mixed join that leverages a cudf AST expression
    filterCondition.map(c => GpuFilterExec(c, joinExec)).getOrElse(joinExec)
  }
}

case class GpuShuffledHashJoinExec(
    override val leftKeys: Seq[Expression],
    override val rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: GpuBuildSide,
    override val condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)(
    cpuLeftKeys: Seq[Expression],
    cpuRightKeys: Seq[Expression]) extends ShimBinaryExecNode with GpuHashJoin
  with GpuSubPartitionHashJoin {

  override def otherCopyArgs: Seq[AnyRef] = cpuLeftKeys :: cpuRightKeys :: Nil

  import GpuMetric._

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    PEAK_DEVICE_MEMORY -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_PEAK_DEVICE_MEMORY),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS))

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(GpuHashPartitioning.getDistribution(cpuLeftKeys),
      GpuHashPartitioning.getDistribution(cpuRightKeys))

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledHashJoin does not support the execute() code path.")
  }

  // Goal to be used for the coalescing the build side. Note that this is internal to
  // the join and not used for planning purposes. The two valid choices are `RequireSingleBatch` or
  // `RequireSingleBatchWithFilter`
  private lazy val buildGoal: CoalesceSizeGoal = joinType match {
    case _: InnerLike | LeftSemi | LeftAnti =>
      val nullFilteringMask = boundBuildKeys.map { bk =>
        // coalesce(key1, false) or coalesce(key2, false) ... or ... coalesce(keyN, false)
        // For any row with a key that is null, this filter mask will remove those rows.
        GpuCoalesce(Seq(GpuCast(bk, BooleanType), GpuLiteral(false)))
      }.reduce(GpuOr)
      RequireSingleBatchWithFilter(nullFilteringMask)
    case _ => RequireSingleBatch
  }

  private def realTargetBatchSize(): Long = {
    val configValue = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    Math.max(configValue, 10 * 1024)
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = {
    val batchedBuildGoal = TargetSize(realTargetBatchSize())
    (joinType, buildSide) match {
      case (_, GpuBuildLeft) => Seq(batchedBuildGoal, null)
      case (_, GpuBuildRight) => Seq(null, batchedBuildGoal)
    }
  }

  override def internalDoExecuteColumnar() : RDD[ColumnarBatch] = {
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
    val numPartitions = RapidsConf.NUM_SUB_PARTITIONS.get(conf)
    val subPartConf = RapidsConf.HASH_SUB_PARTITION_TEST_ENABLED.get(conf)
       .map(_ && RapidsConf.TEST_CONF.get(conf))
    val localBuildOutput = buildPlan.output

    // Create a map of metrics that can be handed down to shuffle and coalesce
    // iterators, setting as noop certain metrics that the coalesce iterators
    // normally update, but that in the case of the join they would produce
    // the wrong statistics (since there are conflicts)
    val coalesceMetrics = allMetrics +
      (GpuMetric.NUM_INPUT_ROWS -> NoopMetric,
       GpuMetric.NUM_INPUT_BATCHES -> NoopMetric,
       GpuMetric.NUM_OUTPUT_BATCHES -> NoopMetric,
       GpuMetric.NUM_OUTPUT_ROWS -> NoopMetric)

    val realTarget = realTargetBatchSize()

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        val (buildData, maybeBufferedStreamIter) =
          GpuShuffledHashJoinExec.prepareBuildBatchesForJoin(buildIter,
            new CollectTimeIterator("shuffled join stream", streamIter, streamTime),
            realTarget, localBuildOutput, buildGoal, subPartConf, coalesceMetrics)

        buildData match {
          case Left(singleBatch) =>
            closeOnExcept(singleBatch) { _ =>
              buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(singleBatch)
            }
            // doJoin will close singleBatch
            doJoin(singleBatch, maybeBufferedStreamIter, realTarget,
              numOutputRows, joinOutputRows, numOutputBatches, opTime, joinTime)
          case Right(builtBatchIter) =>
            // For big joins, when the build data can not fit into a single batch.
            val sizeBuildIter = builtBatchIter.map { cb =>
              closeOnExcept(cb) { _ =>
                buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(cb)
              }
              cb
            }
            doJoinBySubPartition(sizeBuildIter, maybeBufferedStreamIter, realTarget,
              numPartitions, numOutputRows, joinOutputRows, numOutputBatches,
              opTime, joinTime)
        }
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}

object GpuShuffledHashJoinExec {
  /**
   * Return the build data as a single ColumnarBatch when sub-partitioning is not enabled,
   * while as an iterator of ColumnarBatch when sub-partitioning is enabled.
   *
   * sub-partitioning can be activated by specifying its relevant config but this is intended
   * for tests only. In production, whether sub-partitioning will be enabled depends on
   * if all the data in build side can fit into a single batch. If yes, sub-partitioning
   * will not be enabled. Otherwise, it will.
   *
   * This function also takes care of acquiring the GPU semaphore optimally in the scenario
   * where the build side is relatively small (less than `targetSize`).
   *
   * In the optimal case, this function will load the build side on the host up to the
   * goal configuration and if it fits entirely, allow the stream iterator
   * to also pull to host its first batch. After the first stream batch is on the host, the
   * stream iterator acquires the semaphore and then the build side is copied to the GPU.
   *
   * Prior to this we would get a build batch on the GPU, acquiring
   * the semaphore in the process, and then begin pulling from the stream iterator,
   * which could include IO (while holding onto the semaphore).
   *
   * @param buildIter build side iterator
   * @param streamIter stream side iterator
   * @param targetSize target batch size goal
   * @param buildOutput output attributes of the build plan
   * @param buildGoal the build goal to use when coalescing batches
   * @param subPartConf the config whether to enable sub-partitioning algorithm
   * @param coalesceMetrics metrics map with metrics to be used in downstream
   *                        iterators
   * @return a pair of an Either for build and streamed iterator that can be used
   *         for the join.
   */
  private[rapids] def prepareBuildBatchesForJoin(
      buildIter: Iterator[ColumnarBatch],
      streamIter: Iterator[ColumnarBatch],
      targetSize: Long,
      buildOutput: Seq[Attribute],
      buildGoal: CoalesceSizeGoal,
      subPartConf: Option[Boolean],
      coalesceMetrics: Map[String, GpuMetric]):
  (Either[ColumnarBatch, Iterator[ColumnarBatch]], Iterator[ColumnarBatch]) = {
    val buildTime = coalesceMetrics(GpuMetric.BUILD_TIME)
    val buildTypes = buildOutput.map(_.dataType).toArray
    closeOnExcept(new CloseableBufferedIterator(buildIter.buffered)) { bufBuildIter =>
      val startTime = System.nanoTime()
      // Batches type detection
      val isBuildSerialized = bufBuildIter.hasNext && isBatchSerialized(bufBuildIter.head)

      // Let batches coalesce for size overflow check
      val coalesceBuiltIter = if (isBuildSerialized) {
        new HostShuffleCoalesceIterator(bufBuildIter, targetSize, buildTypes, coalesceMetrics)
      } else { // Batches on GPU have already coalesced to the target size by the given goal.
        bufBuildIter
      }

      if (coalesceBuiltIter.hasNext) {
        val firstBuildBatch = coalesceBuiltIter.next()
        // Batches have coalesced to the target size, so size will overflow if there are
        // more than one batch, or the first batch size already exceeds the target.
        val (sizeOverflow, hasMultipleBatches) = closeOnExcept(firstBuildBatch) { _ =>
          val hasSecondBatch = coalesceBuiltIter.hasNext
          (hasSecondBatch || getBatchSize(firstBuildBatch) > targetSize, hasSecondBatch)
        }
        val needSingleBuildBatch = !subPartConf.getOrElse(sizeOverflow)
        if (needSingleBuildBatch && isBuildSerialized && !sizeOverflow) {
          // add the time it took to fetch that first host-side build batch
          buildTime += System.nanoTime() - startTime
          // It can be optimized for grabbing the GPU semaphore when there is only a single
          // serialized host batch and the sub-partitioning is not activated.
          val (singleBuildCb, bufferedStreamIter) = getBuildBatchOptimizedAndClose(
            firstBuildBatch.asInstanceOf[HostConcatResult], streamIter, buildTypes,
            buildTime)
          (Left(singleBuildCb), bufferedStreamIter)

        } else { // Other cases without optimization
          val safeIter = GpuSubPartitionHashJoin.safeIteratorFromSeq(Seq(firstBuildBatch)) ++
            coalesceBuiltIter
          val gpuBuildIter = if (isBuildSerialized) {
            // batches on host, move them to GPU
            new GpuShuffleCoalesceIterator(safeIter.asInstanceOf[Iterator[HostConcatResult]],
              buildTypes, coalesceMetrics)
          } else { // batches already on GPU
            safeIter.asInstanceOf[Iterator[ColumnarBatch]]
          }

          val buildRet = if (needSingleBuildBatch) {
            val singleBuildCb = getAsSingleBatch(gpuBuildIter, buildOutput,
              hasMultipleBatches, buildGoal, coalesceMetrics)
            Left(singleBuildCb)
          } else { // this is for sub-partitioning
            Right(new CollectTimeIterator("hash join build", gpuBuildIter, buildTime))
          }
          // add the time it took to fetch that first build batch
          buildTime += System.nanoTime() - startTime
          (buildRet, streamIter)
        }
      } else {
        // build is empty
        (Left(GpuColumnVector.emptyBatchFromTypes(buildTypes)), streamIter)
      }
    }
  }

  /** Only accepts a HostConcatResult or a ColumnarBatch as input */
  private def getBatchSize(maybeBatch: AnyRef): Long = maybeBatch match {
    case batch: ColumnarBatch => GpuColumnVector.getTotalDeviceMemoryUsed(batch)
    case hostBatch: HostConcatResult => hostBatch.getTableHeader().getDataLen()
    case _ => throw new IllegalStateException(s"Expect a HostConcatResult or a " +
      s"ColumnarBatch, but got a ${maybeBatch.getClass.getSimpleName}")
  }

  private def getBuildBatchOptimizedAndClose(
      hostConcatResult: HostConcatResult,
      streamIter: Iterator[ColumnarBatch],
      buildDataTypes: Array[DataType],
      buildTime: GpuMetric): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    // For the optimal case, the build iterator is already drained and didn't have a
    // prior so it was a single batch, and is entirely on the host.
    // We peek at the stream iterator with `hasNext` on the buffered iterator, which
    // will grab the semaphore when putting the first stream batch on the GPU, and
    // then we bring the build batch to the GPU and return.
    withResource(hostConcatResult) { _ =>
      closeOnExcept(new CloseableBufferedIterator(streamIter.buffered)) { bufStreamIter =>
        withResource(new NvtxRange("first stream batch", NvtxColor.RED)) { _ =>
          if (bufStreamIter.hasNext) {
            bufStreamIter.head
          } else {
            GpuSemaphore.acquireIfNecessary(TaskContext.get())
          }
        }
        // Bring the build batch to the GPU now
        val buildBatch = buildTime.ns {
          cudf_utils.HostConcatResultUtil.getColumnarBatch(hostConcatResult, buildDataTypes)
        }
        (buildBatch, bufStreamIter)
      }
    }
  }

  private def getAsSingleBatch(
      inputIter: Iterator[ColumnarBatch],
      inputAttrs: Seq[Attribute],
      hasMultipleBatches: Boolean,
      goal: CoalesceSizeGoal,
      coalesceMetrics: Map[String, GpuMetric]): ColumnarBatch = {
    val singleBatchIter = if (hasMultipleBatches) {
      new GpuCoalesceIterator(inputIter, inputAttrs.map(_.dataType).toArray, goal,
        NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
        coalesceMetrics(GpuMetric.CONCAT_TIME), coalesceMetrics(GpuMetric.OP_TIME),
        coalesceMetrics(GpuMetric.PEAK_DEVICE_MEMORY), "single build batch")
    } else {
      inputIter
    }
    ConcatAndConsumeAll.getSingleBatchWithVerification(singleBatchIter, inputAttrs)
  }

  def isBatchSerialized(batch: ColumnarBatch): Boolean = {
    batch.numCols() == 1 && batch.column(0).isInstanceOf[SerializedTableColumn]
  }
}

