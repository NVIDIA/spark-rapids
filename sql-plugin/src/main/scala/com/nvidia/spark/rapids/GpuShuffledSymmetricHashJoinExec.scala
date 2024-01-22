/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import scala.collection.{mutable, BitSet}

import ai.rapids.cudf.{ContiguousTable, HostMemoryBuffer}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.GpuShuffledSymmetricHashJoinExec.JoinInfo
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, ShimBinaryExecNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.rapids.execution.{ConditionalHashJoinIterator, GpuCustomShuffleReaderExec, GpuHashJoin, GpuShuffleExchangeExecBase, HashJoinIterator}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuShuffledSymmetricHashJoinExec {
  /** Utility class to track bound expressions and expression metadata related to a join. */
  case class BoundJoinExprs(
      boundBuildKeys: Seq[GpuExpression],
      buildTypes: Array[DataType],
      boundStreamKeys: Seq[GpuExpression],
      streamTypes: Array[DataType],
      streamOutput: Seq[Attribute],
      boundCondition: Option[GpuExpression],
      numFirstConditionTableColumns: Int,
      compareNullsEqual: Boolean,
      buildSideNeedsNullFilter: Boolean)

  object BoundJoinExprs {
    def bind(
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        condition: Option[Expression],
        buildSide: GpuBuildSide): BoundJoinExprs = {
      val leftTypes = leftOutput.map(_.dataType).toArray
      val rightTypes = rightOutput.map(_.dataType).toArray
      val boundLeftKeys = GpuBindReferences.bindGpuReferences(leftKeys, leftOutput)
      val boundRightKeys = GpuBindReferences.bindGpuReferences(rightKeys, rightOutput)
      val boundCondition = condition.map { c =>
        GpuBindReferences.bindGpuReference(c, leftOutput ++ rightOutput)
      }
      val (boundBuildKeys, buildTypes, boundStreamKeys, streamTypes, streamOutput) =
        buildSide match {
          case GpuBuildRight => (boundRightKeys, rightTypes, boundLeftKeys, leftTypes, leftOutput)
          case GpuBuildLeft => (boundLeftKeys, leftTypes, boundRightKeys, rightTypes, rightOutput)
      }
      val compareNullsEqual = GpuHashJoin.anyNullableStructChild(boundBuildKeys)
      val needNullFilter = compareNullsEqual && boundBuildKeys.exists(_.nullable)
      BoundJoinExprs(boundBuildKeys, buildTypes, boundStreamKeys, streamTypes, streamOutput,
        boundCondition, leftOutput.size, compareNullsEqual, needNullFilter)
    }
  }

  /** Utility class to track information related to a join. */
  class JoinInfo(
      val buildSide: GpuBuildSide,
      val buildIter: Iterator[ColumnarBatch],
      val buildSize: Long,
      val streamIter: Iterator[ColumnarBatch],
      val exprs: BoundJoinExprs)

  /**
   * Trait to house common code for determining the ideal build/stream
   * assignments for symmetric joins.
   */
  trait SymmetricJoinSizer[T <: AutoCloseable] {
    /** Wrap, if necessary, an iterator in preparation for probing the size before a join. */
    def setupForProbe(iter: Iterator[ColumnarBatch]): Iterator[T]

    /**
     * Build an iterator in preparation for using it for sub-joins.
     *
     * @param queue a possibly empty queue of data that has already been fetched from the underlying
     *              iterator as part of probing sizes of the join inputs
     * @param remainingIter the data remaining to be fetched from the original iterator. Iterating
     *                      the queue followed by this iterator reconstructs the iteration order of
     *                      the original input iterator.
     * @param batchTypes the schema of the data
     * @param gpuBatchSizeBytes target GPU batch size in bytes
     * @param metrics metrics to update (e.g.: if coalescing batches)
     * @return iterator of columnar batches to use in sub-joins
     */
    def setupForJoin(
        queue: mutable.Queue[T],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch]

    /** Get the row count of a batch of data */
    def getProbeBatchRowCount(batch: T): Long

    /** Get the data size in bytes of a batch of data */
    def getProbeBatchDataSize(batch: T): Long

    /**
     * Whether to start pulling from the left or right input iterator when probing for data sizes.
     * This helps avoid grabbing the GPU semaphore too early when probing.
     */
    val startWithLeftSide: Boolean

    /**
     * Probe the left and right join inputs to determine which side should be used as the build
     * side and which should be used as the stream side.
     *
     * @param leftKeys join keys for the left table
     * @param leftOutput schema of the left table
     * @param rawLeftIter iterator of batches for the left table
     * @param rightKeys join keys for the right table
     * @param rightOutput schema of the right table
     * @param rawRightIter iterator of batches for the right table
     * @param condition inequality portions of the join condition
     * @param gpuBatchSizeBytes target GPU batch size
     * @param metrics map of metrics to update
     * @return join information including build side, bound expressions, etc.
     */
    def getJoinInfo(
        leftKeys: Seq[Expression],
        leftOutput: Seq[Attribute],
        rawLeftIter: Iterator[ColumnarBatch],
        rightKeys: Seq[Expression],
        rightOutput: Seq[Attribute],
        rawRightIter: Iterator[ColumnarBatch],
        condition: Option[Expression],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): JoinInfo = {
      val leftTime = new LocalGpuMetric
      val rightTime = new LocalGpuMetric
      val buildTime = metrics(BUILD_TIME)
      val streamTime = metrics(STREAM_TIME)
      val leftIter = new CollectTimeIterator("probe left", setupForProbe(rawLeftIter), leftTime)
      val rightIter = new CollectTimeIterator("probe right", setupForProbe(rawRightIter), rightTime)
      closeOnExcept(mutable.Queue.empty[T]) { leftQueue =>
        closeOnExcept(mutable.Queue.empty[T]) { rightQueue =>
          var leftSize = 0L
          var rightSize = 0L
          var buildSide: GpuBuildSide = null
          while (buildSide == null) {
            if (leftSize < rightSize || (startWithLeftSide && leftSize == rightSize)) {
              if (leftIter.hasNext) {
                val leftBatch = leftIter.next()
                if (getProbeBatchRowCount(leftBatch) > 0) {
                  leftQueue += leftBatch
                  leftSize += getProbeBatchDataSize(leftBatch)
                }
              } else {
                buildSide = GpuBuildLeft
              }
            } else {
              if (rightIter.hasNext) {
                val rightBatch = rightIter.next()
                if (getProbeBatchRowCount(rightBatch) > 0) {
                  rightQueue += rightBatch
                  rightSize += getProbeBatchDataSize(rightBatch)
                }
              } else {
                buildSide = GpuBuildRight
              }
            }
          }
          val exprs = BoundJoinExprs.bind(leftKeys, leftOutput, rightKeys, rightOutput,
            condition, buildSide)
          val (buildQueue, buildSize, streamQueue, rawStreamIter) = buildSide match {
            case GpuBuildRight =>
              buildTime += rightTime.value
              streamTime += leftTime.value
              (rightQueue, rightSize, leftQueue, rawLeftIter)
            case GpuBuildLeft =>
              buildTime += leftTime.value
              streamTime += rightTime.value
              (leftQueue, leftSize, rightQueue, rawRightIter)
          }
          metrics(BUILD_DATA_SIZE).set(buildSize)
          val baseBuildIter = setupForJoin(buildQueue, Iterator.empty, exprs.buildTypes,
            gpuBatchSizeBytes, metrics)
          val buildIter = if (exprs.buildSideNeedsNullFilter) {
            new NullFilteredBatchIterator(baseBuildIter, exprs.boundBuildKeys, metrics(OP_TIME))
          } else {
            baseBuildIter
          }
          val streamIter = new CollectTimeIterator("fetch join stream",
            setupForJoin(streamQueue, rawStreamIter, exprs.streamTypes, gpuBatchSizeBytes, metrics),
            streamTime)
          new JoinInfo(buildSide, buildIter, buildSize, streamIter, exprs)
        }
      }
    }
  }

  /**
   * Join sizer to use when both the left and right table are coming directly from a shuffle and
   * the data will be on the host. Caches shuffle batches in host memory while probing without
   * grabbing the GPU semaphore.
   */
  class HostHostJoinSizer extends SymmetricJoinSizer[SpillableHostConcatResult] {

    override def setupForProbe(
        iter: Iterator[ColumnarBatch]): Iterator[SpillableHostConcatResult] = {
      new SpillableHostConcatResultFromColumnarBatchIterator(iter)
    }

    override def setupForJoin(
        queue: mutable.Queue[SpillableHostConcatResult],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      val concatMetrics = getConcatMetrics(metrics)
      val bufferedCoalesceIter = new CloseableBufferedIterator(
        new HostShuffleCoalesceIterator(
          new HostQueueBatchIterator(queue, remainingIter),
          gpuBatchSizeBytes,
          concatMetrics))
      // Force a coalesce of the first batch before we grab the GPU semaphore
      bufferedCoalesceIter.headOption
      new GpuShuffleCoalesceIterator(bufferedCoalesceIter, batchTypes, concatMetrics)
    }

    override def getProbeBatchRowCount(batch: SpillableHostConcatResult): Long = {
      batch.header.getNumRows
    }

    override def getProbeBatchDataSize(batch: SpillableHostConcatResult): Long = {
      batch.header.getDataLen
    }

    override val startWithLeftSide: Boolean = true
  }

  /**
   * Join sizer to use when at least one side of the join is coming from another GPU exec node
   * such that the GPU semaphore is already held. Caches input batches on the GPU.
   *
   * @param startWithLeftSide whether to prefer fetching from the left or right side first
   *                          when probing for table sizes.
   */
  class SpillableColumnarBatchJoinSizer(
      override val startWithLeftSide: Boolean) extends SymmetricJoinSizer[SpillableColumnarBatch] {

    override def setupForProbe(iter: Iterator[ColumnarBatch]): Iterator[SpillableColumnarBatch] = {
      iter.map(batch => SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_BATCHING_PRIORITY))
    }

    override def setupForJoin(
        queue: mutable.Queue[SpillableColumnarBatch],
        remainingIter: Iterator[ColumnarBatch],
        batchTypes: Array[DataType],
        gpuBatchSizeBytes: Long,
        metrics: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
      new SpillableColumnarBatchQueueIterator(queue, remainingIter)
    }

    override def getProbeBatchRowCount(batch: SpillableColumnarBatch): Long = batch.numRows()

    override def getProbeBatchDataSize(batch: SpillableColumnarBatch): Long = batch.sizeInBytes
  }

  def getConcatMetrics(metrics: Map[String, GpuMetric]): Map[String, GpuMetric] = {
    // Use a filtered metrics map to avoid output batch counts and other unrelated metric updates
    Map(
      OP_TIME -> metrics(OP_TIME),
      CONCAT_TIME -> metrics(CONCAT_TIME)).withDefaultValue(NoopMetric)
  }

  def createJoinIterator(
      info: JoinInfo,
      spillableBuiltBatch: LazySpillableColumnarBatch,
      lazyStream: Iterator[LazySpillableColumnarBatch],
      gpuBatchSizeBytes: Long,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
    if (info.exprs.boundCondition.isDefined) {
      // ConditionalHashJoinIterator will close the compiled condition
      val compiledCondition = info.exprs.boundCondition.get.convertToAst(
        info.exprs.numFirstConditionTableColumns).compile()
      new ConditionalHashJoinIterator(spillableBuiltBatch, info.exprs.boundBuildKeys,
        lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput, compiledCondition,
        gpuBatchSizeBytes, Inner, info.buildSide, info.exprs.compareNullsEqual,
        opTime, joinTime)
    } else {
      new HashJoinIterator(spillableBuiltBatch, info.exprs.boundBuildKeys,
        lazyStream, info.exprs.boundStreamKeys, info.exprs.streamOutput,
        gpuBatchSizeBytes, Inner, info.buildSide, info.exprs.compareNullsEqual,
        opTime, joinTime)
    }
  }
}

/**
 * A GPU shuffled hash join optimized to handle inner joins. Probes the sizes of the input tables
 * before performing the join to determine which to use as the build side.
 *
 * @param leftKeys join keys for the left table
 * @param rightKeys join keys for the right table
 * @param condition inequality portions of the join condition
 * @param left plan for the left table
 * @param right plan for the right table
 * @param isGpuShuffle whether the shuffle is GPU-centric (e.g.: UCX-based)
 * @param gpuBatchSizeBytes target GPU batch size
 * @param cpuLeftKeys original CPU expressions for the left join keys
 * @param cpuRightKeys original CPU expressions for the right join keys
 */
case class GpuShuffledSymmetricHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isGpuShuffle: Boolean,
    gpuBatchSizeBytes: Long)(
    cpuLeftKeys: Seq[Expression],
    cpuRightKeys: Seq[Expression]) extends ShimBinaryExecNode with GpuExec {
  import GpuShuffledSymmetricHashJoinExec._

  override def otherCopyArgs: Seq[AnyRef] = Seq(cpuLeftKeys, cpuRightKeys)

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    CONCAT_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_CONCAT_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_BUILD_TIME),
    STREAM_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_STREAM_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME))

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(GpuHashPartitioning.getDistribution(cpuLeftKeys),
      GpuHashPartitioning.getDistribution(cpuRightKeys))

  override def output: Seq[Attribute] = left.output ++ right.output

  override def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException(s"${this.getClass} does not support row-based execution")
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val localLeftKeys = leftKeys
    val leftOutput = left.output
    val localRightKeys = rightKeys
    val isLeftHost = isHostBatchProducer(left)
    val rightOutput = right.output
    val isRightHost = isHostBatchProducer(right)
    val localCondition = condition
    val localGpuBatchSizeBytes = gpuBatchSizeBytes
    val localMetrics = allMetrics.withDefaultValue(NoopMetric)
    left.executeColumnar().zipPartitions(right.executeColumnar()) { case (leftIter, rightIter) =>
      val joinInfo = (isLeftHost, isRightHost) match {
        case (true, true) =>
          getHostHostJoinInfo(localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter,
            localCondition, localGpuBatchSizeBytes, localMetrics)
        case (true, false) =>
          getHostGpuJoinInfo(localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter,
            localCondition, localGpuBatchSizeBytes, localMetrics)
        case (false, true) =>
          getGpuHostJoinInfo(localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter,
            localCondition, localGpuBatchSizeBytes, localMetrics)
        case (false, false) =>
          getGpuGpuJoinInfo(localLeftKeys, leftOutput, leftIter,
            localRightKeys, rightOutput, rightIter,
            localCondition, localGpuBatchSizeBytes, localMetrics)
      }
      val joinIterator = if (joinInfo.buildSize <= localGpuBatchSizeBytes) {
        if (joinInfo.buildSize == 0) {
          Iterator.empty
        } else {
          doSmallBuildJoin(joinInfo, localGpuBatchSizeBytes, localMetrics)
        }
      } else {
        doBigBuildJoin(joinInfo, localGpuBatchSizeBytes, localMetrics)
      }
      val numOutputRows = localMetrics(NUM_OUTPUT_ROWS)
      val numOutputBatches = localMetrics(NUM_OUTPUT_BATCHES)
      joinIterator.map { cb =>
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }

  /**
   * Perform a join where the build side fits in a single GPU batch.
   *
   * @param info join information from the probing phase
   * @param gpuBatchSizeBytes target GPU batch size
   * @param metricsMap metrics to update
   * @return iterator to produce the results of the join
   */
  private def doSmallBuildJoin(
      info: JoinInfo,
      gpuBatchSizeBytes: Long,
      metricsMap: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    val opTime = metricsMap(OP_TIME)
    val lazyStream = new Iterator[LazySpillableColumnarBatch]() {
      override def hasNext: Boolean = info.streamIter.hasNext

      override def next(): LazySpillableColumnarBatch = {
        withResource(info.streamIter.next()) { batch =>
          LazySpillableColumnarBatch(batch, "stream_batch")
        }
      }
    }
    val buildIter = new GpuCoalesceIterator(
      info.buildIter,
      info.exprs.buildTypes,
      RequireSingleBatch,
      numInputRows = NoopMetric,
      numInputBatches = NoopMetric,
      numOutputRows = NoopMetric,
      numOutputBatches = NoopMetric,
      collectTime = NoopMetric,
      concatTime = metricsMap(CONCAT_TIME),
      opTime = opTime,
      opName = "build batch")
    assert(buildIter.hasNext, "build side should not be empty")
    val spillableBuiltBatch = withResource(buildIter.next()) { batch =>
      assert(!buildIter.hasNext, "build side should have a single batch")
      LazySpillableColumnarBatch(batch, "built")
    }
    createJoinIterator(info, spillableBuiltBatch, lazyStream, gpuBatchSizeBytes, opTime,
      metricsMap(JOIN_TIME))
  }

  /**
   * Perform a join where the build side does not fit in a single GPU batch.
   *
   * @param info join information from the probing phase
   * @param gpuBatchSizeBytes target GPU batch size
   * @param metricsMap metrics to update
   * @return iterator to produce the results of the join
   */
  private def doBigBuildJoin(
      info: JoinInfo,
      gpuBatchSizeBytes: Long,
      metricsMap: Map[String, GpuMetric]): Iterator[ColumnarBatch] = {
    new BigInnerJoinIterator(info, gpuBatchSizeBytes, metricsMap)
  }

  /**
   * Probe for join information when both inputs are coming from host memory (i.e.: both
   * inputs are coming from a shuffle when not using a GPU-centered shuffle manager).
   */
  private def getHostHostJoinInfo(
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = new HostHostJoinSizer()
    sizer.getJoinInfo(leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for join information when the left input is coming from host memory and the
   * right table is coming from GPU memory.
   */
  private def getHostGpuJoinInfo(
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      rawLeftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = new SpillableColumnarBatchJoinSizer(startWithLeftSide = true)
    val concatMetrics = getConcatMetrics(metrics)
    val leftIter = new GpuShuffleCoalesceIterator(
      new HostShuffleCoalesceIterator(rawLeftIter, gpuBatchSizeBytes, concatMetrics),
      leftOutput.map(_.dataType).toArray,
      concatMetrics)
    sizer.getJoinInfo(leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for the join information when the left input is coming from GPU memory and the
   * left table is coming from host memory.
   */
  private def getGpuHostJoinInfo(
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rawRightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = new SpillableColumnarBatchJoinSizer(startWithLeftSide = false)
    val concatMetrics = getConcatMetrics(metrics)
    val rightIter = new GpuShuffleCoalesceIterator(
      new HostShuffleCoalesceIterator(rawRightIter, gpuBatchSizeBytes, concatMetrics),
      rightOutput.map(_.dataType).toArray,
      concatMetrics)
    sizer.getJoinInfo(leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Probe for the join information when both inputs are coming from GPU memory.
   */
  private def getGpuGpuJoinInfo(
      leftKeys: Seq[Expression],
      leftOutput: Seq[Attribute],
      leftIter: Iterator[ColumnarBatch],
      rightKeys: Seq[Expression],
      rightOutput: Seq[Attribute],
      rightIter: Iterator[ColumnarBatch],
      condition: Option[Expression],
      gpuBatchSizeBytes: Long,
      metrics: Map[String, GpuMetric]): JoinInfo = {
    val sizer = new SpillableColumnarBatchJoinSizer(startWithLeftSide = true)
    sizer.getJoinInfo(leftKeys, leftOutput, leftIter, rightKeys, rightOutput, rightIter,
      condition, gpuBatchSizeBytes, metrics)
  }

  /**
   * Determines if a plan produces data in host memory.
   *
   * @param plan the plan to check
   * @return true if the plan produces batches in host memory, false otherwise
   */
  private def isHostBatchProducer(plan: SparkPlan): Boolean = {
    if (isGpuShuffle) {
      false
    } else {
      plan match {
        case _: GpuShuffleCoalesceExec =>
          throw new IllegalStateException("Should not have shuffle coalesce before this node")
        case _: GpuShuffleExchangeExecBase | _: GpuCustomShuffleReaderExec => true
        case _: ShuffleQueryStageExec => true
        case _ => false
      }
    }
  }
}

/**
 * A spillable form of a HostConcatResult. Takes ownership of the specified host buffer.
 */
class SpillableHostConcatResult(
    val header: SerializedTableHeader,
    hmb: HostMemoryBuffer) extends AutoCloseable {
  private var buffer = {
    SpillableHostBuffer(hmb, hmb.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
  }

  def getHostMemoryBufferAndClose(): HostMemoryBuffer = {
    val hostBuffer = buffer.getHostBuffer()
    closeOnExcept(hostBuffer) { _ =>
      close()
    }
    hostBuffer
  }

  override def close(): Unit = {
    buffer.close()
    buffer = null
  }
}

/**
 * Converts an iterator of shuffle batches in host memory into an iterator of spillable
 * host memory batches.
 */
class SpillableHostConcatResultFromColumnarBatchIterator(
    iter: Iterator[ColumnarBatch]) extends Iterator[SpillableHostConcatResult] {
  override def hasNext: Boolean = iter.hasNext

  override def next(): SpillableHostConcatResult = {
    withResource(iter.next()) { batch =>
      require(batch.numCols() > 0, "Batch must have at least 1 column")
      batch.column(0) match {
        case col: SerializedTableColumn =>
          val buffer = col.hostBuffer
          buffer.incRefCount()
          new SpillableHostConcatResult(col.header, buffer)
        case c =>
          throw new IllegalStateException(s"Expected SerializedTableColumn, got ${c.getClass}")
      }
    }
  }
}

/**
 * Iterator that produces SerializedTableColumn batches from a queue of spillable host memory
 * batches that were fetched first during probing and the (possibly empty) remaining iterator of
 * un-probed host memory batches. The iterator returns the queue elements first, followed by
 * the elements of the remaining iterator.
 *
 * @param spillableQueue queue of spillable host memory batches
 * @param batchIter iterator of remaining host memory batches
 */
class HostQueueBatchIterator(
    spillableQueue: mutable.Queue[SpillableHostConcatResult],
    batchIter: Iterator[ColumnarBatch]) extends GpuColumnarBatchIterator(true) {
  override def hasNext: Boolean = spillableQueue.nonEmpty || batchIter.hasNext

  override def next(): ColumnarBatch = {
    if (spillableQueue.nonEmpty) {
      val shcr = spillableQueue.dequeue()
      closeOnExcept(shcr.getHostMemoryBufferAndClose()) { hostBuffer =>
        SerializedTableColumn.from(shcr.header, hostBuffer)
      }
    } else {
      batchIter.next()
    }
  }

  override def doClose(): Unit = {
    spillableQueue.safeClose()
  }
}

/**
 * Iterator that produces columnar batches from a queue of spillable batches that were fetched
 * first during probing and the (possibly empty) remaining iterator fo un-probed batches. The
 * iterator returns the queue elements first, followed by the elements of the remaining iterator.
 */
class SpillableColumnarBatchQueueIterator(
    queue: mutable.Queue[SpillableColumnarBatch],
    batchIter: Iterator[ColumnarBatch]) extends GpuColumnarBatchIterator(true) {

  override def hasNext: Boolean = queue.nonEmpty || batchIter.hasNext

  override def next(): ColumnarBatch = {
    if (queue.nonEmpty) {
      withResource(queue.dequeue()) { spillable =>
        spillable.getColumnarBatch()
      }
    } else {
      batchIter.next()
    }
  }

  override def doClose(): Unit = {
    queue.safeClose()
  }
}

/**
 * Iterator that filters out rows with null keys.
 *
 * @param iter iterator of batches to filter
 * @param boundKeys expressions to produce the keys
 * @param opTime metric to update for time taken during the filter operation
 */
class NullFilteredBatchIterator(
    iter: Iterator[ColumnarBatch],
    boundKeys: Seq[Expression],
    opTime: GpuMetric) extends Iterator[ColumnarBatch] with AutoCloseable {
  private var onDeck: Option[ColumnarBatch] = None

  onTaskCompletion(close())

  override def hasNext: Boolean = {
    while (onDeck.isEmpty && iter.hasNext) {
      val batch = withResource(iter.next()) { batch =>
        opTime.ns {
          val spillable = SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
          GpuHashJoin.filterNullsWithRetryAndClose(spillable, boundKeys)
        }
      }
      if (batch.numRows > 0) {
        onDeck = Some(batch)
      } else {
        batch.close()
      }
    }
    onDeck.nonEmpty
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("no more batches")
    }
    val batch = onDeck.get
    onDeck = None
    batch
  }

  override def close(): Unit = {
    onDeck.foreach(_.close())
    onDeck = None
  }
}

/** Tracks a collection of batches associated with a partition in a large join */
class JoinPartition extends AutoCloseable {
  private var totalSize = 0L
  private val batches = new mutable.ArrayBuffer[SpillableColumnarBatch]()

  def addPartitionBatch(part: SpillableColumnarBatch, dataSize: Long): Unit = {
    batches.append(part)
    totalSize += dataSize
  }

  def getTotalSize: Long = totalSize

  def releaseBatches(): Array[SpillableColumnarBatch] = {
    val result = batches.toArray
    batches.clear()
    totalSize = 0
    result
  }

  override def close(): Unit = {
    batches.safeClose()
    batches.clear()
    totalSize = 0L
  }
}

/**
 * Base class for a partitioner in a large join.
 *
 * @param numPartitions number of partitions being used in the join
 * @param batchTypes schema of the batches
 * @param boundJoinKeys bound keys used in the join
 * @param metrics metrics to update
 */
abstract class JoinPartitioner(
    numPartitions: Int,
    batchTypes: Array[DataType],
    boundJoinKeys: Seq[Expression],
    metrics: Map[String, GpuMetric]) extends AutoCloseable {
  protected val partitions: Array[JoinPartition] =
    (0 until numPartitions).map(_ => new JoinPartition).toArray
  protected val opTime = metrics(OP_TIME)

  /**
   * Hash partitions a batch in preparation for performing a sub-join. The input batch will
   * be closed by this method.
   */
  protected def partitionBatch(inputBatch: ColumnarBatch): Unit = {
    val spillableBatch = SpillableColumnarBatch(inputBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    withRetryNoSplit(spillableBatch) { _ =>
      opTime.ns {
        val partsTable = GpuHashPartitioningBase.hashPartitionAndClose(
          spillableBatch.getColumnarBatch(), boundJoinKeys, numPartitions, "partition for join",
          JoinPartitioner.HASH_SEED)
        val contigTables = withResource(partsTable) { _ =>
          partsTable.getTable.contiguousSplit(partsTable.getPartitions.tail: _*)
        }
        withResource(contigTables) { _ =>
          contigTables.zipWithIndex.foreach { case (ct, i) =>
            if (ct.getRowCount > 0) {
              contigTables(i) = null
              addPartition(i, ct)
            }
          }
        }
      }
    }
  }

  /**
   * Adds a batch associated with a partition.
   *
   * @param partIndex index of the partition
   * @param ct contiguous table to add to the specified partition
   */
  protected def addPartition(partIndex: Int, ct: ContiguousTable): Unit = {
    val dataSize = ct.getBuffer.getLength
    partitions(partIndex).addPartitionBatch(SpillableColumnarBatch(ct, batchTypes,
      SpillPriorities.ACTIVE_BATCHING_PRIORITY), dataSize)
  }

  override def close(): Unit = {
    partitions.safeClose()
  }
}

object JoinPartitioner {
  /**
   * A seed to use for the hash partitioner when sub-partitioning. Needs to be different than
   * the seed used by Spark for partitioning a hash join (i.e.: 42)
   */
  val HASH_SEED: Int = 100
}

/**
 * Join partitioner for the build side of a large join where the build side of the join does not
 * fit in a single GPU batch.
 *
 * @param numPartitions number of partitions being used in the join
 * @param buildSideIter iterator of build side batches
 * @param buildSideTypes schema of the build side batches
 * @param boundBuildKeys bound join key expressions for the build side
 * @param gpuBatchSizeBytes target GPU batch size
 * @param metrics metrics to update
 */
class BuildSidePartitioner(
    val numPartitions: Int,
    buildSideIter: Iterator[ColumnarBatch],
    buildSideTypes: Array[DataType],
    boundBuildKeys: Seq[Expression],
    gpuBatchSizeBytes: Long,
    metrics: Map[String, GpuMetric])
  extends JoinPartitioner(numPartitions, buildSideTypes, boundBuildKeys, metrics) {

  // partition all of the build-side batches
  closeOnExcept(partitions) { _ =>
    while (buildSideIter.hasNext) {
      partitionBatch(buildSideIter.next())
    }
  }

  private val (emptyPartitions, joinGroups) = findEmptyAndJoinGroups()
  private val partitionBatches = new Array[LazySpillableColumnarBatch](joinGroups.length)
  private val concatTime = metrics(CONCAT_TIME)

  /** Returns a BitSet where a set bit corresponds to an empty partition at that index. */
  def getEmptyPartitions: BitSet = emptyPartitions

  /**
   * Returns a BitSet array where each BitSet corresponds to a "join group," a group of partitions
   * that can together be used as a single build batch for a sub-join. A set bit in a join group's
   * BitSet corresponds to a partition index that is part of the join group.
   */
  def getJoinGroups: Array[BitSet] = joinGroups

  /**
   * Returns the batch of data for the specified join group index. All of the data across the
   * partitions comprising the join group are concatenated together to produce the batch.
   * The concatenated batches are lazily cached, so the cost of concatenation is only incurred
   * by the first caller for a particular join group.
   *
   * @param partitionGroupIndex the index of the join group for which to produce the batch
   * @return the batch of data for the join group
   */
  def getBuildBatch(partitionGroupIndex: Int): LazySpillableColumnarBatch = {
    var batch = partitionBatches(partitionGroupIndex)
    if (batch == null) {
      val spillBatchesBuffer = new mutable.ArrayBuffer[SpillableColumnarBatch]()
      closeOnExcept(spillBatchesBuffer) { _ =>
        joinGroups(partitionGroupIndex).foreach { i =>
          val batches = partitions(i).releaseBatches()
          assert(batches.nonEmpty)
          spillBatchesBuffer ++= batches
        }
      }
      val concatBatch = withRetryNoSplit(spillBatchesBuffer.toSeq) { spillBatches =>
        val batchesToConcat = spillBatches.safeMap(_.getColumnarBatch()).toArray
        opTime.ns {
          concatTime.ns {
            ConcatAndConsumeAll.buildNonEmptyBatchFromTypes(batchesToConcat, buildSideTypes)
          }
        }
      }
      withResource(concatBatch) { _ =>
        batch = LazySpillableColumnarBatch(concatBatch, "build subtable")
        partitionBatches(partitionGroupIndex) = batch
      }
    }
    LazySpillableColumnarBatch.spillOnly(batch)
  }

  override def close(): Unit = {
    partitions.safeClose()
    partitionBatches.safeClose()
  }

  /**
   * After partitioning the build-side table, find the set of partition indices
   * that are empty partitions along with an iterator of partition index sets that,
   * for each set of indices, produces a build-side sub-table that is ideally
   * within the GPU batch size limits. If there is a partition that is larger than
   * the target size, it will be in its own partition index set.
   *
   * @return the set of empty partition indices and an iterator of partition index
   *         sets where each set identifies partitions that can be processed together
   *         in a join pass.
   */
  private def findEmptyAndJoinGroups(): (BitSet, Array[BitSet]) = {
    val emptyPartitions = new mutable.BitSet(numPartitions)
    val joinGroups = new mutable.ArrayBuffer[BitSet]()
    val sortedIndices = (0 until numPartitions).sortBy(i => partitions(i).getTotalSize)
    val (emptyIndices, nonEmptyIndices) = sortedIndices.partition { i =>
      partitions(i).getTotalSize == 0
    }
    emptyPartitions ++= emptyIndices
    var group = new mutable.BitSet(numPartitions)
    var groupSize = 0L
    nonEmptyIndices.foreach { i =>
      val newSize = groupSize + partitions(i).getTotalSize
      if (newSize > gpuBatchSizeBytes) {
        if (group.nonEmpty) {
          joinGroups.append(group)
        }
        group = new mutable.BitSet(numPartitions)
        groupSize = partitions(i).getTotalSize
      } else {
        groupSize = newSize
      }
      group.add(i)
    }
    if (group.nonEmpty) {
      joinGroups.append(group)
    }
    (emptyPartitions, joinGroups.toArray)
  }
}

/**
 * Join partitioner for the stream side of a large join.
 *
 * @param numPartitions number of partitions being used in the join
 * @param emptyBuildPartitions BitSet indicating which build side partitions are empty
 * @param iter iterator of stream side batches
 * @param streamTypes schema of the stream side batches
 * @param boundStreamKeys bound join key expressions for the stream side
 * @param metrics metrics to update
 */
class StreamSidePartitioner(
    numPartitions: Int,
    emptyBuildPartitions: BitSet,
    iter: Iterator[ColumnarBatch],
    streamTypes: Array[DataType],
    boundStreamKeys: Seq[Expression],
    metrics: Map[String, GpuMetric])
  extends JoinPartitioner(numPartitions, streamTypes, boundStreamKeys, metrics) {

  override protected def addPartition(partIndex: Int, ct: ContiguousTable): Unit = {
    // Ignore partitions that correspond to empty build-side partitions, since
    // no stream-side keys in this partition will match anything on the build-side.
    if (emptyBuildPartitions.contains(partIndex)) {
      ct.close()
    } else {
      super.addPartition(partIndex, ct)
    }
  }

  def hasInputBatches: Boolean = iter.hasNext

  def partitionNextBatch(): Unit = {
    assert(partitions.forall(_.getTotalSize == 0), "leftover partitions from previous batch")
    partitionBatch(iter.next)
  }

  def releasePartitions(partIndices: BitSet): Array[SpillableColumnarBatch] = {
    partIndices.iterator.flatMap(i => partitions(i).releaseBatches().toSeq).toArray
  }

  override def close(): Unit = {
    partitions.safeClose()
  }
}

/**
 * Iterator that produces the result of a large inner join where the build side of the join is
 * too large for a single GPU batch. The prior join input probing phase has sized the build side
 * of the join, so this partitions both the build side and stream side into N+1 partitions, where
 * N is the size of the build side divided by the target GPU batch size.
 *
 * Once the build side is partitioned completely, the partitions are placed into "join groups"
 * where all the build side data of a join group fits in the GPU target batch size. If the input
 * data is skewed, a single build partition could be larger than the target GPU batch size.
 * Currently such oversized partitions are placed in separate join groups consisting just of one
 * partition each in the hopes that there will be enough GPU memory to proceed with the join
 * despite the skew. We will need to revisit this for very large, skewed build side data arriving
 * at a single task.
 *
 * Once the build side join groups are identified, each stream batch is partitioned into the same
 * number of partitions as the build side with the same hash key used for the build side. The
 * partitions from the batch are grouped into join groups matching the partition grouping from
 * the build side, and each join group is processed as a sub-join. Once all the join groups for
 * a stream batch have been processed, the next stream batch is fetched, partitioned, and sub-joins
 * are processed against the build side join groups. Repeat until the stream side is exhausted.
 *
 * @param info join information from input probing phase
 * @param gpuBatchSizeBytes target GPU batch size
 * @param metrics metrics to update
 */
class BigInnerJoinIterator(
    info: JoinInfo,
    gpuBatchSizeBytes: Long,
    metrics: Map[String, GpuMetric])
  extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  private val buildPartitioner = {
    val numPartitions = (info.buildSize / gpuBatchSizeBytes) + 1
    require(numPartitions <= Int.MaxValue, "too many build partitions")
    new BuildSidePartitioner(numPartitions.toInt, info.buildIter, info.exprs.buildTypes,
      info.exprs.boundBuildKeys, gpuBatchSizeBytes, metrics)
  }
  use(buildPartitioner)

  private val joinGroups = buildPartitioner.getJoinGroups
  private var nextJoinGroupIndex = joinGroups.length

  private val streamPartitioner = new StreamSidePartitioner(buildPartitioner.numPartitions,
    buildPartitioner.getEmptyPartitions, info.streamIter, info.exprs.streamTypes,
    info.exprs.boundStreamKeys, metrics)
  use(streamPartitioner)

  private var subIter: Option[Iterator[ColumnarBatch]] = None
  private var isExhausted = joinGroups.isEmpty

  override def hasNext: Boolean = {
    if (isExhausted) {
      false
    } else if (subIter.exists(_.hasNext)) {
      true
    } else {
      setupNextJoinIterator()
      val result = subIter.exists(_.hasNext)
      if (!result) {
        isExhausted = true
        close()
      }
      result
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("join batches exhausted")
    }
    subIter.get.next()
  }

  private def setupNextJoinIterator(): Unit = {
    while (!isExhausted && !subIter.exists(_.hasNext)) {
      if (nextJoinGroupIndex >= joinGroups.length) {
        // try to pull in the next stream batch
        if (streamPartitioner.hasInputBatches) {
          streamPartitioner.partitionNextBatch()
          nextJoinGroupIndex = 0
          subIter = Some(moveToNextBuildGroup())
        } else {
          isExhausted = true
          subIter = None
        }
      } else {
        subIter = Some(moveToNextBuildGroup())
      }
    }
  }

  private def moveToNextBuildGroup(): Iterator[ColumnarBatch] = {
    val builtBatch = buildPartitioner.getBuildBatch(nextJoinGroupIndex)
    val group = joinGroups(nextJoinGroupIndex)
    nextJoinGroupIndex += 1
    val streamBatches = streamPartitioner.releasePartitions(group)
    val lazyStream = new Iterator[LazySpillableColumnarBatch] {
      onTaskCompletion(streamBatches.safeClose())

      private var i = 0

      override def hasNext: Boolean = i < streamBatches.length

      override def next(): LazySpillableColumnarBatch = {
        withResource(streamBatches(i)) { spillBatch =>
          streamBatches(i) = null
          i += 1
          withResource(spillBatch.getColumnarBatch()) { batch =>
            LazySpillableColumnarBatch(batch, "stream_batch")
          }
        }
      }
    }
    GpuShuffledSymmetricHashJoinExec.createJoinIterator(info, builtBatch, lazyStream,
      gpuBatchSizeBytes, metrics(OP_TIME), metrics(JOIN_TIME))
  }
}
