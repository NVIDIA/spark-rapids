/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.GpuMetricNames.{DESCRIPTION_NUM_OUTPUT_BATCHES, DESCRIPTION_NUM_OUTPUT_ROWS, DESCRIPTION_NUM_PARTITIONS, DESCRIPTION_PARTITION_SIZE, NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, NUM_PARTITIONS, PARTITION_SIZE}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuShuffleDependency, GpuShuffleEnv}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

class GpuShuffleMeta(
    shuffle: ShuffleExchangeExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[ShuffleExchangeExec](shuffle, conf, parent, rule) {
  // Some kinds of Partitioning are a type of expression, but Partitioning itself is not
  // so don't let them leak through as expressions
  override val childExprs: scala.Seq[ExprMeta[_]] = Seq.empty
  override val childParts: scala.Seq[PartMeta[_]] =
    Seq(GpuOverrides.wrapPart(shuffle.outputPartitioning, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    // when AQE is enabled and we are planning a new query stage, we need to look at meta-data
    // previously stored on the spark plan to determine whether this exchange can run on GPU
    wrapped.getTagValue(gpuSupportedTag).foreach(_.foreach(willNotWorkOnGpu))
  }

  override def isSupportedType(t: DataType): Boolean =
    GpuOverrides.isSupportedType(t,
      allowNull = true)

  override def convertToGpu(): GpuExec =
    ShimLoader.getSparkShims.getGpuShuffleExchangeExec(
      childParts(0).convertToGpu(),
      childPlans(0).convertIfNeeded(),
      Some(shuffle))
}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
abstract class GpuShuffleExchangeExecBase(
    override val outputPartitioning: Partitioning,
    child: SparkPlan) extends Exchange with GpuExec {

  // Shuffle produces a lot of small output batches that should be coalesced together.
  // This coalesce occurs on the GPU and should always be done when using RAPIDS shuffle.
  // Normal shuffle performs the coalesce on the CPU to optimize the transfers to the GPU.
  override def coalesceAfter: Boolean = GpuShuffleEnv.isRapidsShuffleEnabled

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val additionalMetrics : Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  // Spark doesn't report totalTime for this operator so we override metrics
  override lazy val metrics: Map[String, SQLMetric] = Map(
    PARTITION_SIZE ->
        SQLMetrics.createMetric(sparkContext, DESCRIPTION_PARTITION_SIZE),
    NUM_PARTITIONS ->
        SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_PARTITIONS),
    NUM_OUTPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_OUTPUT_BATCHES)
  ) ++ additionalMetrics

  override def nodeName: String = "GpuColumnarExchange"

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputBatchRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependencyColumnar)
    }
  }

  def shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    throw new IllegalStateException()
  }

  private lazy val sparkTypes: Array[DataType] = child.output.map(_.dataType).toArray

  // This value must be lazy because the child's output may not have been resolved
  // yet in all cases.
  private lazy val serializer: Serializer = new GpuColumnarBatchSerializer(
    longMetric("dataSize"))

  @transient lazy val inputBatchRDD: RDD[ColumnarBatch] = child.executeColumnar()

  /**
   * A `ShuffleDependency` that will partition columnar batches of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependencyColumnar : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    GpuShuffleExchangeExec.prepareBatchShuffleDependency(
      inputBatchRDD,
      child.output,
      outputPartitioning,
      sparkTypes,
      serializer,
      metrics,
      writeMetrics,
      additionalMetrics)
  }

  /**
   * Caches the created ShuffleBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledBatchRDD = null

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = attachTree(this, "execute") {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledBatchRDD(shuffleDependencyColumnar, metrics ++ readMetrics)
    }
    cachedShuffleRDD
  }
}

object GpuShuffleExchangeExec {
  def prepareBatchShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      sparkTypes: Array[DataType],
      serializer: Serializer,
      metrics: Map[String, SQLMetric],
      writeMetrics: Map[String, SQLMetric],
      additionalMetrics: Map[String, SQLMetric])
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
      val sorter = new GpuColumnarBatchSorter(Seq.empty[SortOrder],
        null, false, false)
      rdd.mapPartitions { cbIter =>
        val sortedIterator = sorter.sort(cbIter)
        sortedIterator
      }
    } else {
      rdd
    }
    val partitioner: GpuExpression = getPartitioner(newRdd, outputAttributes, newPartitioning)
    def getPartitioned: ColumnarBatch => Any = {
      batch => partitioner.columnarEval(batch)
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
              partitioned = getParts(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
              partitioned.foreach(batches => {
                metrics(GpuMetricNames.NUM_OUTPUT_ROWS) += batches._1.numRows()
              })
              metrics(GpuMetricNames.NUM_OUTPUT_BATCHES) += partitioned.length
              at = 0
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
      metrics = additionalMetrics)

    dependency
  }

  private def getPartitioner(
    rdd: RDD[ColumnarBatch],
    outputAttributes: Seq[Attribute],
    newPartitioning: Partitioning): GpuExpression with GpuPartitioning = {
    newPartitioning match {
      case h: GpuHashPartitioning =>
        GpuBindReferences.bindReference(h, outputAttributes)
      case r: GpuRangePartitioning =>
        r.part.createRangeBounds(r.numPartitions, r.gpuOrdering, rdd, outputAttributes,
          SQLConf.get.rangeExchangeSampleSizePerPartition)
        GpuBindReferences.bindReference(r, outputAttributes)
      case s: GpuSinglePartitioning =>
        GpuBindReferences.bindReference(s, outputAttributes)
      case rrp: GpuRoundRobinPartitioning =>
        GpuBindReferences.bindReference(rrp, outputAttributes)
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
  }
}
