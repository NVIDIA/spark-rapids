/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{HostConcatResultUtil, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.HostConcatResult
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, GpuJoinUtils, ShimBinaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.rapids.execution.{GpuHashJoin, JoinTypeChecks}
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
    cpuRightKeys: Seq[Expression]) extends ShimBinaryExecNode with GpuHashJoin {

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
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS)) ++ spillMetrics

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(GpuHashPartitioning.getDistribution(cpuLeftKeys),
      GpuHashPartitioning.getDistribution(cpuRightKeys))

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "GpuShuffledHashJoin does not support the execute() code path.")
  }

  override def childrenCoalesceGoal: Seq[CoalesceGoal] = (joinType, buildSide) match {
    case (FullOuter, _) => Seq(RequireSingleBatch, RequireSingleBatch)
    case (_, GpuBuildLeft) => Seq(RequireSingleBatch, null)
    case (_, GpuBuildRight) => Seq(null, RequireSingleBatch)
  }

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
    val batchSizeBytes = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
    val localBuildOutput = buildPlan.output

    // Create a map of metrics that can be handed down to shuffle and coalesce
    // iterators, setting as noop certain metrics that the coalesce iterators
    // normally update, but that in the case of the join they would produce
    // the wrong statistics (since there are conflicts)
    val coalesceMetricsMap = allMetrics +
      (GpuMetric.NUM_INPUT_ROWS -> NoopMetric,
       GpuMetric.NUM_INPUT_BATCHES -> NoopMetric,
       GpuMetric.NUM_OUTPUT_BATCHES -> NoopMetric,
       GpuMetric.NUM_OUTPUT_ROWS -> NoopMetric)

    streamedPlan.executeColumnar().zipPartitions(buildPlan.executeColumnar()) {
      (streamIter, buildIter) => {
        val (builtBatch, maybeBufferedStreamIter) =
          GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
            batchSizeBytes,
            localBuildOutput,
            buildIter,
            new CollectTimeIterator("shuffled join stream", streamIter, streamTime),
            spillCallback,
            coalesceMetricsMap)

        withResource(builtBatch) { _ =>
          // doJoin will increment the reference counts as needed for the builtBatch
          buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(builtBatch)
          doJoin(builtBatch, maybeBufferedStreamIter,
            batchSizeBytes, spillCallback, numOutputRows, joinOutputRows, numOutputBatches,
            opTime, joinTime)
        }
      }
    }
  }

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }
}

object GpuShuffledHashJoinExec extends Arm {
  /**
   * Helper iterator that wraps a BufferedIterator of AutoCloseable subclasses.
   * This iterator also implements AutoCloseable, so it can be closed in case
   * of exceptions.
   *
   * @param wrapped the buffered iterator
   * @tparam T an AutoCloseable subclass
   */
  class CloseableBufferedIterator[T <: AutoCloseable](wrapped: BufferedIterator[T])
    extends BufferedIterator[T] with AutoCloseable {
    // register against task completion to close any leaked buffered items
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

    private[this] var isClosed = false
    override def head: T = wrapped.head
    override def headOption: Option[T] = wrapped.headOption
    override def next: T = wrapped.next
    override def hasNext: Boolean = wrapped.hasNext
    override def close(): Unit = {
      if (!isClosed) {
        headOption.foreach(_.close())
        isClosed = true
      }
    }
  }

  /**
   * Gets a `ColumnarBatch` and stream Iterator[ColumnarBatch] pair by acquiring
   * the GPU semaphore optimally in the scenario where the build side is relatively
   * small (less than `hostTargetBatchSize`).
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
   * The function handles the case where the build side goes above the configured batch
   * goal, in which case it will concat on the host, grab the semaphore, and continue to
   * pull the build iterator to build a bigger batch on the GPU. This is not optimized
   * because we hold onto the semaphore during the entire time after realizing the goal
   * has been hit.
   *
   * @param hostTargetBatchSize target batch size goal on the host
   * @param buildOutput output attributes of the build plan
   * @param buildIter build iterator
   * @param streamIter stream iterator
   * @param spillCallback metric updater in case downstream iterators spill
   * @param coalesceMetricsMap metrics map with metrics to be used in downstream
   *                           iterators
   * @return a pair of `ColumnarBatch` and streamed iterator that can be
   *         used for the join
   */
  def getBuiltBatchAndStreamIter(
      hostTargetBatchSize: Long,
      buildOutput: Seq[Attribute],
      buildIter: Iterator[ColumnarBatch],
      streamIter: Iterator[ColumnarBatch],
      spillCallback: SpillCallback,
      coalesceMetricsMap: Map[String, GpuMetric]): (ColumnarBatch, Iterator[ColumnarBatch]) = {
    val semWait = coalesceMetricsMap(GpuMetric.SEMAPHORE_WAIT_TIME)
    val buildTime = coalesceMetricsMap(GpuMetric.BUILD_TIME)
    var bufferedBuildIterator: CloseableBufferedIterator[ColumnarBatch] = null
    closeOnExcept(bufferedBuildIterator) { _ =>
      val startTime = System.nanoTime()
      // find if the build side is non-empty, and if the first batch is
      // a serialized batch. If neither condition is met, we fallback to the
      // `getSingleBatchWithVerification` method.
      val firstBatchIsSerialized = {
        if (!buildIter.hasNext) {
          false
        } else {
          bufferedBuildIterator = new CloseableBufferedIterator(buildIter.buffered)
          val firstBatch = bufferedBuildIterator.head
          if (firstBatch.numCols() != 1) {
            false
          } else {
            firstBatch.column(0).isInstanceOf[SerializedTableColumn]
          }
        }
      }

      if (!firstBatchIsSerialized) {
        // In this scenario we are getting non host-side batches in the build side
        // given the plan rules we expect this to be a single batch
        val builtBatch =
          ConcatAndConsumeAll.getSingleBatchWithVerification(
            Option(bufferedBuildIterator).getOrElse(buildIter), buildOutput)
        val delta = System.nanoTime() - startTime
        buildTime += delta
        (builtBatch, streamIter)
      } else {
        val dataTypes = buildOutput.map(_.dataType).toArray
        val hostConcatIter = new HostShuffleCoalesceIterator(bufferedBuildIterator,
          hostTargetBatchSize, dataTypes, coalesceMetricsMap)
        withResource(hostConcatIter) { _ =>
          closeOnExcept(hostConcatIter.next()) { hostConcatResult =>
            if (!hostConcatIter.hasNext()) {
              // add the time it took to fetch that first host-side build batch
              buildTime += System.nanoTime() - startTime
              // Optimal case, we drained the build iterator and we didn't have a prior
              // so it was a single batch, and is entirely on the host.
              // We peek at the stream iterator with `hasNext` on the buffered
              // iterator, which will grab the semaphore when putting the first stream
              // batch on the GPU, and then we bring the build batch to the GPU and return.
              val bufferedStreamIter = new CloseableBufferedIterator(streamIter.buffered)
              closeOnExcept(bufferedStreamIter) { _ =>
                withResource(new NvtxRange("first stream batch", NvtxColor.RED)) { _ =>
                  if (bufferedStreamIter.hasNext) {
                    bufferedStreamIter.head
                  } else {
                    GpuSemaphore.acquireIfNecessary(TaskContext.get(), semWait)
                  }
                }
                val buildBatch = getBuildBatchOptimized(hostConcatResult, buildOutput, buildTime)
                (buildBatch, bufferedStreamIter)
              }
            } else {
              val buildBatch = getBuildBatchFromUnfinished(
                Seq(hostConcatResult).iterator ++ hostConcatIter,
                buildOutput, spillCallback, coalesceMetricsMap)
              buildTime += System.nanoTime() - startTime
              (buildBatch, streamIter)
            }
          }
        }
      }
    }
  }

  private def getBuildBatchFromUnfinished(
      iterWithPrior: Iterator[HostConcatResult],
      buildOutput: Seq[Attribute],
      spillCallback: SpillCallback,
      coalesceMetricsMap: Map[String, GpuMetric]): ColumnarBatch = {
    // In the fallback case we build the same iterator chain that the Spark plan
    // would have produced:
    //   GpuCoalesceIterator(GpuShuffleCoalesceIterator(shuffled build side))
    // This allows us to make the shuffle batches spillable in case we have a large,
    // build-side table, as `RequireSingleBatch` is virtually no limit, and we
    // know we are now above `hostTargetBatchSize` (which is 2GB by default)
    val dataTypes = buildOutput.map(_.dataType).toArray
      val shuffleCoalesce = new GpuShuffleCoalesceIterator(
        iterWithPrior,
        dataTypes,
        coalesceMetricsMap)
      val res = ConcatAndConsumeAll.getSingleBatchWithVerification(
        new GpuCoalesceIterator(shuffleCoalesce,
          dataTypes,
          RequireSingleBatch,
          NoopMetric, NoopMetric, NoopMetric, NoopMetric, NoopMetric,
          coalesceMetricsMap(GpuMetric.CONCAT_TIME),
          coalesceMetricsMap(GpuMetric.OP_TIME),
          coalesceMetricsMap(GpuMetric.PEAK_DEVICE_MEMORY),
          spillCallback,
          "build batch"),
        buildOutput)
      res
  }

  private def getBuildBatchOptimized(
      hostConcatResult: HostConcatResult,
      buildOutput: Seq[Attribute],
      buildTime: GpuMetric): ColumnarBatch = {
    val dataTypes = buildOutput.map(_.dataType).toArray
    // we are on the GPU and our build batch is within `targetSizeBytes`.
    // we can bring the build batch to the GPU now
    withResource(hostConcatResult) { _ =>
      buildTime.ns {
        HostConcatResultUtil.getColumnarBatch(hostConcatResult, dataTypes)
      }
    }
  }
}

