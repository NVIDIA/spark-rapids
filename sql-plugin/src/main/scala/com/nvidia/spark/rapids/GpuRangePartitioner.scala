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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuRangePartitioner {
  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd                    the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  private[this] def sketch(
      rdd: RDD[ColumnarBatch],
      sampleSizePerPartition: Int,
      sorter: GpuSorter): (Long, Array[(Int, Long, Array[InternalRow])]) = {
    val shift = rdd.id
    val toRowConverter = GpuColumnarToRowExecParent.makeIteratorFunc(sorter.projectedBatchSchema,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, sorter, toRowConverter, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  private[this] def randomResample(
      rdd: RDD[ColumnarBatch],
      fraction: Double,
      seed: Int,
      sorter: GpuSorter): Array[InternalRow] = {
    val toRowConverter = GpuColumnarToRowExecParent.makeIteratorFunc(sorter.projectedBatchSchema,
      NoopMetric, NoopMetric, NoopMetric, NoopMetric)
    rdd.mapPartitions { iter =>
      val sample = SamplingUtils.randomResample(
        iter, fraction, sorter, toRowConverter, seed)
      Iterator(sample)
    }.collect().flatten
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  private[this] def determineBounds(
      candidates: ArrayBuffer[(InternalRow, Float)],
      partitions: Int,
      ordering: Ordering[InternalRow]): Array[InternalRow] = {
    val ordered = candidates.sortBy(_._1)(ordering)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[InternalRow]
    var i = 0
    var j = 0
    var previousBound = Option.empty[InternalRow]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  def createRangeBounds(partitions: Int,
      sorter: GpuSorter,
      rdd: RDD[ColumnarBatch],
      samplePointsPerPartitionHint: Int): Array[InternalRow] = {
    // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
    require(partitions >= 0,
      s"Number of partitions cannot be negative but found $partitions.")
    require(samplePointsPerPartitionHint > 0,
      s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

    implicit val ordering: LazilyGeneratedOrdering = new LazilyGeneratedOrdering(sorter.cpuOrdering)

    // An array of upper bounds for the first (partitions - 1) partitions
    val rangeBounds : Array[InternalRow] = {
      if (partitions < 1) {
        Array.empty
      } else {
        // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
        // Cast to double to avoid overflowing ints or longs
        val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
        // Assume the input partitions are roughly balanced and over-sample a little bit.
        val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
        val (numItems, sketched) = sketch(rdd, sampleSizePerPartition, sorter)
        if (numItems == 0L) {
          Array.empty
        } else {
          // If a partition contains much more than the average number of items,
          // we re-sample from it
          // to ensure that enough items are collected from that partition.
          val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
          val candidates = ArrayBuffer.empty[(InternalRow, Float)]
          var imbalancedPartitions = Set.empty[Int]
          sketched.foreach { case (idx, n, sample) =>
            if (fraction * n > sampleSizePerPartition) {
              imbalancedPartitions += idx
            } else {
              // The weight is 1 over the sampling probability.
              val weight = (n.toDouble / sample.length).toFloat
              for (key <- sample) {
                candidates += ((key, weight))
              }
            }
          }
          if (imbalancedPartitions.nonEmpty) {
            // Re-sample imbalanced partitions with the desired sampling probability.
            val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
            val seed = byteswap32(-rdd.id - 1)
            val reSampled = randomResample(imbalanced, fraction, seed, sorter)
            val weight = (1.0 / fraction).toFloat
            candidates ++= reSampled.map(x => (x, weight))
          }
          determineBounds(candidates, math.min(partitions, candidates.size), ordering)
        }
      }
    }
    rangeBounds.asInstanceOf[Array[InternalRow]]
  }
}

case class GpuRangePartitioner(
    rangeBounds: Array[InternalRow],
    sorter: GpuSorter) extends GpuExpression with GpuPartitioning {

  private lazy val converters = new GpuRowToColumnConverter(
    TrampolineUtil.fromAttributes(sorter.projectedBatchSchema))

  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = Seq.empty
  override val numPartitions: Int = rangeBounds.length + 1

  private[this] def computeBoundsAndClose(cb: ColumnarBatch): (Array[Int], ColumnarBatch) = {
    withResource(cb) { cb =>
      withResource(
        sorter.appendProjectedAndSort(cb, NoopMetric)) { sortedTbl =>
        val parts = withResource(
          GpuColumnVector.from(sortedTbl, sorter.projectedBatchTypes)) { sorted =>
          val retCv = withResource(converters.convertBatch(rangeBounds,
            TrampolineUtil.fromAttributes(sorter.projectedBatchSchema))) { ranges =>
            sorter.upperBound(sorted, ranges)
          }
          withResource(retCv) { retCv =>
            // The first entry must always be 0, which upper bound is not doing
            Array(0) ++ GpuColumnVector.toIntArray(retCv)
          }
        }
        (parts, sorter.removeProjectedColumns(sortedTbl))
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    if (rangeBounds.nonEmpty) {
      val (parts, sortedBatch) = computeBoundsAndClose(batch)
      withResource(sortedBatch) { sortedBatch =>
        val partitionColumns = GpuColumnVector.extractColumns(sortedBatch)
        val slicedCb = sliceInternalGpuOrCpu(sortedBatch.numRows(), parts, partitionColumns)
        slicedCb.zipWithIndex.filter(_._1 != null)
      }
    } else {
      withResource(batch) { cb =>
        // Nothing needs to be sliced but a contiguous table is needed for GPU shuffle which
        // slice will produce.
        val sliced = sliceInternalGpuOrCpu(cb.numRows, Array(0),
          GpuColumnVector.extractColumns(cb))
        sliced.zipWithIndex.filter(_._1 != null)
      }
    }
  }
}
