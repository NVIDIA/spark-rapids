/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, HashFunction, NvtxColor, NvtxRange, Table}
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution, Partitioning}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{BatchPartitionIdPassthrough, ShuffledBatchRDD, SparkPlan}
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
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

  override def convertToGpu(): GpuExec =
    GpuShuffleExchangeExec(childParts(0).convertToGpu(),
      childPlans(0).convertIfNeeded(),
      shuffle.canChangeNumPartitions)
}

/**
 * Performs a shuffle that will result in the desired partitioning.
 */
case class GpuShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    child: SparkPlan,
    canChangeNumPartitions: Boolean = true) extends Exchange with GpuExec {

  /**
   * Lots of small output batches we want to group together.
   */
  override def coalesceAfter: Boolean = true

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size")
  ) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "GpuColumnarExchange"

  private val serializer: Serializer =
    new GpuColumnarBatchSerializer(child.output.size, longMetric("dataSize"))

  @transient lazy val inputBatchRDD: RDD[ColumnarBatch] = child.executeColumnar()

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleBatchDependency : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    GpuShuffleExchangeExec.prepareBatchShuffleDependency(
      inputBatchRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
  }

  def createShuffledBatchRDD(partitionStartIndices: Option[Array[Int]]): ShuffledBatchRDD = {
    new ShuffledBatchRDD(shuffleBatchDependency, readMetrics, partitionStartIndices)
  }

  /**
   * Caches the created ShuffleBatchRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledBatchRDD = null

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = attachTree(this, "execute") {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = createShuffledBatchRDD(None)
    }
    cachedShuffleRDD
  }
}

object GpuShuffleExchangeExec {


  def prepareBatchShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric])
  : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    def getPartitioned: ColumnarBatch => Any = newPartitioning match {
      case h: GpuHashPartitioning =>
        val boundH = GpuBindReferences.bindReferences(h :: Nil, outputAttributes).head
        batch => boundH.columnarEval(batch)

      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    // We already know it is always going to be hash
    val rddWithPartitionIds: RDD[Product2[Int, ColumnarBatch]] = {
      rdd.mapPartitions { iter =>
        val getParts = getPartitioned
        new AbstractIterator[Product2[Int, ColumnarBatch]] {
          private var partitioned : Array[(ColumnarBatch, Int)] = null
          private var at = 0
          private val mutablePair = new MutablePair[Int, ColumnarBatch]()
          private def partNextBatch(): Unit = {
            if (partitioned != null) {
              partitioned.map(_._1).safeClose()
              partitioned = null
              at = 0
            }
            if (iter.hasNext) {
              val batch = iter.next()
              partitioned = getParts(batch).asInstanceOf[Array[(ColumnarBatch, Int)]]
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

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
    new ShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rddWithPartitionIds,
      new BatchPartitionIdPassthrough(newPartitioning.numPartitions),
      serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics))

    dependency
  }
}

case class GpuHashPartitioning(expressions: Seq[GpuExpression], numPartitions: Int)
  extends GpuExpression with GpuPartitioning {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: HashClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  def getGpuKeyColumns(batch: ColumnarBatch) : Array[GpuColumnVector] =
    expressions.map {
      expr => {
        val vec = expr.columnarEval(batch).asInstanceOf[GpuColumnVector]
        try {
          if (vec.dataType() == StringType) {
            // TODO GPU partitioning has a bug when working with strings so hash them directly here instead...
            GpuColumnVector.from(vec.getBase().hash())
          } else {
            vec.incRefCount()
          }
        } finally {
          vec.close()
        }
      }
    }.toArray

  def getGpuDataColumns(batch: ColumnarBatch) : Array[GpuColumnVector] =
    GpuColumnVector.convertToStringCategoriesArrayIfNeeded(batch)

  def insertDedupe(indexesOut: Array[Int], colsIn: Array[GpuColumnVector], dedupedData: ArrayBuffer[ColumnVector]): Unit = {
    indexesOut.indices.foreach { i =>
      val b = colsIn(i).getBase
      val idx = dedupedData.indexOf(b)
      if (idx < 0) {
        indexesOut(i) = dedupedData.size
        dedupedData += b
      } else {
        indexesOut(i) = idx
      }
    }
  }

  def dedupe(keyCols: Array[GpuColumnVector], dataCols: Array[GpuColumnVector]):
  (Array[Int], Array[Int], Table) = {
    val base = new ArrayBuffer[ColumnVector](keyCols.length + dataCols.length)
    val keys = new Array[Int](keyCols.length)
    val data = new Array[Int](dataCols.length)

    insertDedupe(keys, keyCols, base)
    insertDedupe(data, dataCols, base)

    (keys, data, new Table(base: _*))
  }

  def partitionInternal(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    var gpuKeyColumns : Array[GpuColumnVector] = null
    var gpuDataColumns : Array[GpuColumnVector] = null
    try {
      gpuKeyColumns = getGpuKeyColumns(batch)
      gpuDataColumns = getGpuDataColumns(batch)

      val (keys, dataIndexes, table) = dedupe(gpuKeyColumns, gpuDataColumns)
      // Don't need the batch any more table has all we need in it.
      gpuDataColumns.foreach(_.close())
      gpuDataColumns = null
      gpuKeyColumns.foreach(_.close())
      gpuKeyColumns = null
      batch.close()

      val partedTable = table.onColumns(keys: _*).partition(numPartitions, HashFunction.MURMUR3)
      table.close()
      val parts = partedTable.getPartitions
      val columns = dataIndexes.map(idx => GpuColumnVector.from(partedTable.getColumn(idx).incRefCount()))
      partedTable.close()
      (parts, columns)
    } finally {
      if (gpuDataColumns != null) {
        gpuDataColumns.safeClose()
      }
      if (gpuKeyColumns != null) {
        gpuKeyColumns.safeClose()
      }
    }
  }

  def sliceBatch(vectors: Array[GpuColumnVector], start: Int, end: Int): ColumnarBatch = {
    var ret: ColumnarBatch = null
    val count = end - start
    if (count > 0) {
      ret = new ColumnarBatch(vectors.map(vec => new SlicedGpuColumnVector(vec, start, end)))
      ret.setNumRows(count)
    }
    ret
  }

  def sliceInternal(batch: ColumnarBatch, partitionIndexes: Array[Int],
      partitionColumns: Array[GpuColumnVector]): Array[ColumnarBatch] = {
    // We are slicing the data but keeping the old in place, so copy to the CPU now
    partitionColumns.foreach(_.getBase.ensureOnHost())
    val ret = new Array[ColumnarBatch](numPartitions)
    var start = 0
    for (i <- 1 until numPartitions) {
      val idx = partitionIndexes(i)
      ret(i - 1) = sliceBatch(partitionColumns, start, idx)
      start = idx
    }
    ret(numPartitions - 1) = sliceBatch(partitionColumns, start, batch.numRows())
    ret
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level

    val totalRange = new NvtxRange("Hash partition", NvtxColor.PURPLE)
    try {
      val (partitionIndexes, partitionColumns) = {
        val partitionRange = new NvtxRange("partition", NvtxColor.BLUE)
        try {
          partitionInternal(batch)
        } finally {
          partitionRange.close()
        }
      }
      val ret = {
        val sliceRange = new NvtxRange("slice", NvtxColor.CYAN)
        try {
          sliceInternal(batch, partitionIndexes, partitionColumns)
        } finally {
          sliceRange.close()
        }
      }
      partitionColumns.safeClose()
      // Close the partition columns we copied them as a part of the slice
      ret.zipWithIndex.filter(_._1 != null)
    } finally {
      totalRange.close()
    }
  }
}

