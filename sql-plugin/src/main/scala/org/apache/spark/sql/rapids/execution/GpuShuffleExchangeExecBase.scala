/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import scala.collection.AbstractIterator
import scala.concurrent.Future

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{GpuHashPartitioning, GpuRangePartitioning, ShimUnaryExecNode, ShuffleOriginUtil, SparkShimImpl}

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuShuffleDependency
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

abstract class GpuShuffleMetaBase(
    shuffle: ShuffleExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[ShuffleExchangeExec](shuffle, conf, parent, rule) {
  // Some kinds of Partitioning are a type of expression, but Partitioning itself is not
  // so don't let them leak through as expressions
  override val childExprs: scala.Seq[ExprMeta[_]] = Seq.empty
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(shuffle.outputPartitioning, conf, Some(this)))

  // Propagate possible type conversions on the output attributes of map-side plans to
  // reduce-side counterparts. We can pass through the outputs of child because Shuffle will
  // not change the data schema. And we need to pass through because Shuffle itself and
  // reduce-side plans may failed to pass the type check for tagging CPU data types rather
  // than their GPU counterparts.
  //
  // Taking AggregateExec with TypedImperativeAggregate function as example:
  //    Assume I have a query: SELECT a, COLLECT_LIST(b) FROM table GROUP BY a, which physical plan
  //    looks like:
  //    ObjectHashAggregate(keys=[a#10], functions=[collect_list(b#11, 0, 0)],
  //                        output=[a#10, collect_list(b)#17])
  //      +- Exchange hashpartitioning(a#10, 200), true, [id=#13]
  //        +- ObjectHashAggregate(keys=[a#10], functions=[partial_collect_list(b#11, 0, 0)],
  //                               output=[a#10, buf#21])
  //          +- LocalTableScan [a#10, b#11]
  //
  //    We will override the data type of buf#21 in GpuNoHashAggregateMeta. Otherwise, the partial
  //    Aggregate will fall back to CPU because buf#21 produce a GPU-unsupported type: BinaryType.
  //    Just like the partial Aggregate, the ShuffleExchange will also fall back to CPU unless we
  //    apply the same type overriding as its child plan: the partial Aggregate.
  override protected val useOutputAttributesOfChild: Boolean = true

  // For transparent plan like ShuffleExchange, the accessibility of runtime data transition is
  // depended on the next non-transparent plan. So, we need to trace back.
  override val availableRuntimeDataTransition: Boolean =
    childPlans.head.availableRuntimeDataTransition

  override def tagPlanForGpu(): Unit = {

    if (!ShuffleOriginUtil.isSupported(shuffle.shuffleOrigin)) {
      willNotWorkOnGpu(s"${shuffle.shuffleOrigin} not supported on GPU")
    }

    shuffle.outputPartitioning match {
      case _: RoundRobinPartitioning
        if SparkShimImpl.sessionFromPlan(shuffle).sessionState.conf
            .sortBeforeRepartition =>
        val orderableTypes = GpuOverrides.pluginSupportedOrderableSig +
            TypeSig.ARRAY.nested(GpuOverrides.gpuCommonTypes)

        shuffle.output.map(_.dataType)
            .filterNot(orderableTypes.isSupportedByPlugin)
            .foreach { dataType =>
              willNotWorkOnGpu(s"round-robin partitioning cannot sort $dataType to run " +
                  s"this on the GPU set ${SQLConf.SORT_BEFORE_REPARTITION.key} to false")
            }
      case _ =>
    }

    // When AQE is enabled, we need to preserve meta data as outputAttributes and
    // availableRuntimeDataTransition to the spark plan for the subsequent query stages.
    // These meta data will be fetched in the SparkPlanMeta of CustomShuffleReaderExec.
    if (wrapped.getTagValue(GpuShuffleMetaBase.shuffleExOutputAttributes).isEmpty) {
      wrapped.setTagValue(GpuShuffleMetaBase.shuffleExOutputAttributes, outputAttributes)
    }
    if (wrapped.getTagValue(GpuShuffleMetaBase.availableRuntimeDataTransition).isEmpty) {
      wrapped.setTagValue(GpuShuffleMetaBase.availableRuntimeDataTransition,
        availableRuntimeDataTransition)
    }
  }

  override final def convertToGpu(): GpuExec = {
    // Any AQE child node should be marked columnar if we're columnar.
    val newChild = childPlans.head.wrapped match {
      case adaptive: AdaptiveSparkPlanExec =>
        val goal = TargetSize(conf.gpuTargetBatchSizeBytes)
        SparkShimImpl.columnarAdaptivePlan(adaptive, goal)
      case _ => childPlans.head.convertIfNeeded()
    }
    convertShuffleToGpu(newChild)
  }

  protected def convertShuffleToGpu(newChild: SparkPlan): GpuExec = {
    GpuShuffleExchangeExec(
      childParts.head.convertToGpu(),
      newChild,
      shuffle.shuffleOrigin
    )(shuffle.outputPartitioning)
  }
}

object GpuShuffleMetaBase {

  val shuffleExOutputAttributes = TreeNodeTag[Seq[Attribute]](
    "rapids.gpu.shuffleExOutputAttributes")

  val availableRuntimeDataTransition = TreeNodeTag[Boolean](
    "rapids.gpu.availableRuntimeDataTransition")

}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
abstract class GpuShuffleExchangeExecBaseWithMetrics(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan) extends GpuShuffleExchangeExecBase(gpuOutputPartitioning, child) {

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputBatchRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependencyColumnar)
    }
  }
}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
abstract class GpuShuffleExchangeExecBase(
    gpuOutputPartitioning: GpuPartitioning,
    child: SparkPlan) extends Exchange with ShimUnaryExecNode with GpuExec {
  import GpuMetric._

  private lazy val useKudo = RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.get(child.conf)

  private lazy val useGPUShuffle = {
    gpuOutputPartitioning match {
      case gpuPartitioning: GpuPartitioning => gpuPartitioning.usesGPUShuffle
      case _ => false
    }
  }

  private lazy val useMultiThreadedShuffle = {
    gpuOutputPartitioning match {
      case gpuPartitioning: GpuPartitioning => gpuPartitioning.usesMultiThreadedShuffle
      case _ => false
    }
  }

  // Shuffle produces a lot of small output batches that should be coalesced together.
  // This coalesce occurs on the GPU and should always be done when using RAPIDS shuffle,
  // when it is under UCX or CACHE_ONLY modes.
  // Normal shuffle performs the coalesce on the CPU to optimize the transfers to the GPU.
  override def coalesceAfter: Boolean = useGPUShuffle

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val additionalMetrics : Map[String, GpuMetric] = Map(
    // dataSize and dataReadSize are uncompressed, one is on write and the 
    // other on read
    "dataSize" -> createSizeMetric(ESSENTIAL_LEVEL,"data size"),
    "dataReadSize" -> createSizeMetric(MODERATE_LEVEL, "data read size"),
    "rapidsShuffleSerializationTime" ->
        createNanoTimingMetric(DEBUG_LEVEL,"rs. serialization time"),
    "rapidsShuffleDeserializationTime" ->
        createNanoTimingMetric(DEBUG_LEVEL,"rs. deserialization time"),
    "rapidsShuffleWriteTime" ->
        createNanoTimingMetric(ESSENTIAL_LEVEL,"rs. shuffle write time"),
    "rapidsShuffleCombineTime" ->
        createNanoTimingMetric(DEBUG_LEVEL,"rs. shuffle combine time"),
    "rapidsShuffleWriteIoTime" ->
        createNanoTimingMetric(DEBUG_LEVEL,"rs. shuffle write io time"),
    "rapidsShuffleReadTime" ->
        createNanoTimingMetric(ESSENTIAL_LEVEL,"rs. shuffle read time")
  ) ++ GpuMetric.wrap(readMetrics) ++ GpuMetric.wrap(writeMetrics)

  // Spark doesn't report totalTime for this operator so we override metrics
  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    PARTITION_SIZE -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_PARTITION_SIZE),
    NUM_PARTITIONS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_PARTITIONS),
    NUM_OUTPUT_ROWS -> createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES)
  ) ++ additionalMetrics

  override def nodeName: String = "GpuColumnarExchange"

  def shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    throw new IllegalStateException()
  }

  private lazy val sparkTypes: Array[DataType] = child.output.map(_.dataType).toArray

  // This value must be lazy because the child's output may not have been resolved
  // yet in all cases.
  private lazy val serializer: Serializer = new GpuColumnarBatchSerializer(
    gpuLongMetric("dataSize"), sparkTypes, useKudo)

  @transient lazy val inputBatchRDD: RDD[ColumnarBatch] = child.executeColumnar()

  /**
   * A `ShuffleDependency` that will partition columnar batches of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependencyColumnar : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    GpuShuffleExchangeExecBase.prepareBatchShuffleDependency(
      inputBatchRDD,
      child.output,
      gpuOutputPartitioning,
      sparkTypes,
      serializer,
      useGPUShuffle,
      useMultiThreadedShuffle,
      allMetrics,
      writeMetrics,
      additionalMetrics)
  }

  /**
   * Caches the created ShuffleBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledBatchRDD = null

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = SparkShimImpl
    .attachTreeIfSupported(this, "execute") {
      // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
      if (cachedShuffleRDD == null) {
        cachedShuffleRDD = new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics)
      }
      cachedShuffleRDD
    }
}

object GpuShuffleExchangeExecBase {
  def prepareBatchShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: GpuPartitioning,
      sparkTypes: Array[DataType],
      serializer: Serializer,
      useGPUShuffle: Boolean,
      useMultiThreadedShuffle: Boolean,
      metrics: Map[String, GpuMetric],
      writeMetrics: Map[String, SQLMetric],
      additionalMetrics: Map[String, GpuMetric])
  : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val isRoundRobin = newPartitioning match {
      case _: GpuRoundRobinPartitioning => true
      case _ => false
    }

    /**
     * Analogous to how spark config for sorting before repartition works, the GPU implementation
     * here sorts on the GPU over all columns of the table in ascending order and nulls as smallest.
     * This essentially means the results are not one to one w.r.t. the row hashcode based sort.
     * As long as we don't mix and match repartition() between CPU and GPU this should not be a
     * concern. Latest spark behavior does not even require this sort as it fails the upstream
     * task when indeterminate tasks re-run.
     */
    val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
      val boundReferences = outputAttributes.zipWithIndex.map { case (attr, index) =>
        SortOrder(GpuBoundReference(index, attr.dataType,
          attr.nullable)(attr.exprId, attr.name), Ascending)
        // Force the sequence to materialize so we don't have issues with serializing too much
      }.toArray.toSeq
      val sorter = new GpuSorter(boundReferences, outputAttributes)
      rdd.mapPartitions { cbIter =>
        GpuSortEachBatchIterator(cbIter, sorter, false)
      }
    } else {
      rdd
    }
    val partitioner: GpuExpression = getPartitioner(newRdd, outputAttributes, newPartitioning)
    def getPartitioned: ColumnarBatch => Any = {
      batch => partitioner.columnarEvalAny(batch)
    }
    val rddWithPartitionIds: RDD[Product2[Int, ColumnarBatch]] = {
      newRdd.mapPartitions { iter =>
        val getParts = getPartitioned
        new AbstractIterator[Product2[Int, ColumnarBatch]] {
          private var partitioned : Array[(ColumnarBatch, Int)] = _
          private var at = 0
          private val mutablePair = new MutablePair[Int, ColumnarBatch]()
          private def partNextBatch(): Unit = {
            if (partitioned != null) {
              partitioned.map(_._1).safeClose()
              partitioned = null
              at = 0
            }
            if (iter.hasNext) {
              var batch = iter.next()
              while (batch.numRows == 0 && iter.hasNext) {
                batch.close()
                batch = iter.next()
              }
              // Get a non-empty batch or the last batch. So still need to
              // check if it is empty for the later case.
              if (batch.numRows > 0) {
                partitioned = getParts(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
                partitioned.foreach(batches => {
                  metrics(GpuMetric.NUM_OUTPUT_ROWS) += batches._1.numRows()
                })
                metrics(GpuMetric.NUM_OUTPUT_BATCHES) += partitioned.length
                at = 0
              } else {
                batch.close()
              }
            }
          }

          override def hasNext: Boolean = {
            if (partitioned == null || at >= partitioned.length) {
              partNextBatch()
            }

            partitioned != null && at < partitioned.length
          }

          override def next(): Product2[Int, ColumnarBatch] = {
            if (partitioned == null || at >= partitioned.length) {
              partNextBatch()
            }
            if (partitioned == null || at >= partitioned.length) {
              throw new NoSuchElementException("Walked off of the end...")
            }
            val tup = partitioned(at)
            mutablePair.update(tup._2, tup._1)
            at += 1
            mutablePair
          }
        }
      }
    }

    // Now, we manually create a GpuShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    // We do a GPU version because it allows us to know that the data is on the GPU so we can
    // detect it and do further processing if needed.
    val dependency =
    new GpuShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rddWithPartitionIds,
      new BatchPartitionIdPassthrough(newPartitioning.numPartitions),
      sparkTypes,
      serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
      useGPUShuffle = useGPUShuffle,
      useMultiThreadedShuffle = useMultiThreadedShuffle,
      metrics = GpuMetric.unwrap(additionalMetrics))

    dependency
  }

  private def getPartitioner(
    rdd: RDD[ColumnarBatch],
    outputAttributes: Seq[Attribute],
    newPartitioning: GpuPartitioning): GpuExpression with GpuPartitioning = {
    newPartitioning match {
      case h: GpuHashPartitioning =>
        GpuBindReferences.bindReference(h, outputAttributes)
      case r: GpuRangePartitioning =>
        val sorter = new GpuSorter(r.gpuOrdering, outputAttributes)
        val bounds = GpuRangePartitioner.createRangeBounds(r.numPartitions, sorter,
          rdd, SQLConf.get.rangeExchangeSampleSizePerPartition)
        // No need to bind arguments for the GpuRangePartitioner. The Sorter has already done it
        new GpuRangePartitioner(bounds, sorter)
      case GpuSinglePartitioning =>
        GpuSinglePartitioning
      case rrp: GpuRoundRobinPartitioning =>
        GpuBindReferences.bindReference(rrp, outputAttributes)
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
  }
}
