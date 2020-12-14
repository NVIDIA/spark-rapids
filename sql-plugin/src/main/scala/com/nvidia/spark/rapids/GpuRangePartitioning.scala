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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, OrderedDistribution}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU accelerated `org.apache.spark.sql.catalyst.plans.physical.Partitioning` that partitions
 * sortable records by range into roughly equal ranges. The ranges are determined by sampling
 * the content of the RDD passed in.
 *
 * @note The actual number of partitions created might not be the same
 * as the `numPartitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 */

case class GpuRangePartitioning(
    gpuOrdering: Seq[SortOrder],
    numPartitions: Int,
    schema: StructType)(val part: GpuRangePartitioner)
  extends GpuExpression with GpuPartitioning {

  override def otherCopyArgs: Seq[AnyRef] = Seq(part)

  var rangeBounds: Array[InternalRow] = _

  override def children: Seq[SortOrder] = gpuOrdering
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case OrderedDistribution(requiredOrdering) =>
          // If `ordering` is a prefix of `requiredOrdering`:
          //   Let's say `ordering` is [a, b] and `requiredOrdering` is [a, b, c]. According to the
          //   RangePartitioning definition, any [a, b] in a previous partition must be smaller
          //   than any [a, b] in the following partition. This also means any [a, b, c] in a
          //   previous partition must be smaller than any [a, b, c] in the following partition.
          //   Thus `RangePartitioning(a, b)` satisfies `OrderedDistribution(a, b, c)`.
          //
          // If `requiredOrdering` is a prefix of `ordering`:
          //   Let's say `ordering` is [a, b, c] and `requiredOrdering` is [a, b]. According to the
          //   RangePartitioning definition, any [a, b, c] in a previous partition must be smaller
          //   than any [a, b, c] in the following partition. If there is a [a1, b1] from a
          //   previous partition which is larger than a [a2, b2] from the following partition,
          //   then there must be a [a1, b1 c1] larger than [a2, b2, c2], which violates
          //   RangePartitioning definition. So it's guaranteed that, any [a, b] in a previous
          //   partition must not be greater(i.e. smaller or equal to) than any [a, b] in the
          //   following partition. Thus `RangePartitioning(a, b, c)` satisfies
          //   `OrderedDistribution(a, b)`.
          val minSize = Seq(requiredOrdering.size, gpuOrdering.size).min
          requiredOrdering.take(minSize) == gpuOrdering.take(minSize)
        case ClusteredDistribution(requiredClustering, _) =>
          gpuOrdering.map(_.child).forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    var rangesBatch: ColumnarBatch = null
    var rangesTbl: Table = null
    var sortedTbl: Table = null
    var slicedSortedTbl: Table = null
    var finalSortedCb: ColumnarBatch = null
    var retCv: ColumnVector = null
    var inputCvs: Seq[GpuColumnVector] = null
    var inputTbl: Table = null
    var partitionColumns: Array[GpuColumnVector] = null
    var parts: Array[Int] = Array(0)
    var slicedCb: Array[ColumnarBatch] = null
    val descFlags = new ArrayBuffer[Boolean]()
    val nullFlags = new ArrayBuffer[Boolean]()
    val numSortCols = gpuOrdering.length

    val orderByArgs: Seq[Table.OrderByArg] = gpuOrdering.zipWithIndex.map { case (order, index) =>
      val nullsSmallest = SortUtils.areNullsSmallest(order)
      if (order.isAscending) {
        descFlags += false
        nullFlags += nullsSmallest
        Table.asc(index, nullsSmallest)
      } else {
        descFlags += true
        nullFlags += nullsSmallest
        Table.desc(index, nullsSmallest)
      }
    }

    try {
      //get Inputs table bound
      inputCvs = SortUtils.getGpuColVectorsAndBindReferences(batch, gpuOrdering)
      inputTbl = new Table(inputCvs.map(_.getBase): _*)
      //sort incoming batch to compare with ranges
      sortedTbl = inputTbl.orderBy(orderByArgs: _*)
      val sortColumns = (0 until numSortCols).map(sortedTbl.getColumn(_))
      //get the table for upper bound calculation
      slicedSortedTbl = new Table(sortColumns: _*)
      //get the final column batch, remove the sort order sortColumns
      val outputTypes = gpuOrdering.map(_.child.dataType) ++
          GpuColumnVector.extractTypes(batch)
      finalSortedCb = GpuColumnVector.from(sortedTbl, outputTypes.toArray,
        numSortCols, sortedTbl.getNumberOfColumns)
      val numRows = finalSortedCb.numRows
      partitionColumns = GpuColumnVector.extractColumns(finalSortedCb)
      // get the ranges table and get upper bounds if possible
      // rangeBounds can be empty or of length < numPartitions in cases where the samples are less
      // than numPartitions. The way Spark handles it is by allowing the returned partitions to be
      // rangeBounds.length + 1 which is essentially what happens here when we do upperBound on the
      // ranges table, or return one partition.
      if (part.rangeBounds.nonEmpty) {
        rangesBatch = part.getRangesBatch(schema, part.rangeBounds)
        rangesTbl = GpuColumnVector.from(rangesBatch)
        retCv = slicedSortedTbl.upperBound(nullFlags.toArray, rangesTbl, descFlags.toArray)
        parts = parts ++ GpuColumnVector.toIntArray(retCv)
      }
      slicedCb = sliceInternalGpuOrCpu(numRows, parts, partitionColumns)
    } finally {
      batch.close()
      if (inputCvs != null) {
        inputCvs.safeClose()
      }
      if (inputTbl != null) {
        inputTbl.close()
      }
      if (sortedTbl != null) {
        sortedTbl.close()
      }
      if (slicedSortedTbl != null) {
        slicedSortedTbl.close()
      }
      if (rangesBatch != null) {
        rangesBatch.close()
      }
      if (rangesTbl != null) {
        rangesTbl.close()
      }
      if (retCv != null) {
        retCv.close()
      }
      if (partitionColumns != null) {
        partitionColumns.safeClose()
      }
    }
    slicedCb.zipWithIndex.filter(_._1 != null)
  }
}
