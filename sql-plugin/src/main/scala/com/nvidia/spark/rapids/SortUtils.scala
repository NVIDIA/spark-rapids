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

package com.nvidia.spark.rapids

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, NvtxColor, Table}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, Expression, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object SortUtils extends Arm {
  private [this] def evaluateBoundExpressions[A <: GpuExpression](cb: ColumnarBatch,
      boundExprs: Seq[A]): Seq[GpuColumnVector] = {
    withResource(GpuProjectExec.project(cb, boundExprs)) { cb =>
      (0 until cb.numCols()).map(cb.column(_).asInstanceOf[GpuColumnVector].incRefCount())
          .toArray.toSeq
    }
  }

  /*
  * This function takes the input batch and the bound sort order references and
  * evaluates each column in case its an expression. It then appends the original columns
  * after the sort key columns. The sort key columns will be dropped after sorting.
  */
  def evaluateForSort(batch: ColumnarBatch,
      boundInputReferences: Seq[SortOrder]): Seq[GpuColumnVector] = {
    val sortCvs = new ArrayBuffer[GpuColumnVector](boundInputReferences.length)
    val childExprs = boundInputReferences.map(_.child.asInstanceOf[GpuExpression])
    sortCvs ++= evaluateBoundExpressions(batch, childExprs)
    val originalColumns = GpuColumnVector.extractColumns(batch)
    originalColumns.foreach(_.incRefCount())
    sortCvs ++ originalColumns
  }

  /*
  * Return true if nulls are needed first and ordering is ascending and vice versa
   */
  def areNullsSmallest(order: SortOrder): Boolean = {
    (order.isAscending && order.nullOrdering == NullsFirst) ||
      (!order.isAscending && order.nullOrdering == NullsLast)
  }

  @scala.annotation.tailrec
  def extractReference(exp: Expression): Option[GpuBoundReference] = exp match {
    case r: GpuBoundReference => Some(r)
    case a: Alias => extractReference(a.child)
    case _ => None
  }

  def getOrder(order: SortOrder, index: Int): Table.OrderByArg =
    if (order.isAscending) {
      Table.asc(index, order.nullOrdering == NullsFirst)
    } else {
      Table.desc(index, order.nullOrdering == NullsLast)
    }
}

class GpuSorter(
    sortOrder: Seq[SortOrder],
    inputSchema: Array[Attribute]) extends Arm with Serializable {

  def this(sortOrder: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(sortOrder, inputSchema.toArray)

  private[this] val boundSortOrder = GpuBindReferences.bindReferences(sortOrder, inputSchema.toSeq)

  private[this] val numInputColumns = inputSchema.length

  def cpuOrdering: Seq[SortOrder] = cpuOrderingInternal.toSeq

  private[this] lazy val (needsComputation, cudfOrdering, cpuOrderingInternal) = {
    val needsComputation = mutable.ArrayBuffer[SortOrder]()
    val cpuOrdering = mutable.ArrayBuffer[SortOrder]()
    val cudfOrdering = mutable.ArrayBuffer[Table.OrderByArg]()
    var newColumnIndex = numInputColumns
    // Remove duplicates in the ordering itself because
    // There is no need to do it twice
    boundSortOrder.distinct.foreach { so =>
      SortUtils.extractReference(so.child) match {
        case Some(ref) =>
          cudfOrdering += SortUtils.getOrder(so, ref.ordinal)
          // It is a bound GPU reference so we have to translate it to the CPU
          cpuOrdering += ShimLoader.getSparkShims.sortOrder(
            BoundReference(ref.ordinal, ref.dataType, ref.nullable),
            so.direction, so.nullOrdering)
        case None =>
          val index = newColumnIndex
          newColumnIndex += 1
          cudfOrdering += SortUtils.getOrder(so, index)
          needsComputation += so
          // We already did the computation so instead of trying to translate
          // the computation back to the CPU too, just use the existing columns.
          cpuOrdering += ShimLoader.getSparkShims.sortOrder(
            BoundReference(index, so.dataType, so.nullable),
            so.direction, so.nullOrdering)
      }
    }
    (needsComputation.toArray, cudfOrdering.toArray, cpuOrdering.toArray)
  }

  lazy val computationSchema: Seq[Attribute] =
    needsComputation.map { so =>
      AttributeReference(s"SORT_TMP", so.dataType, so.nullable)()
    }

  lazy val projectedBatchSchema: Seq[Attribute] = inputSchema ++ computationSchema
  lazy val projectedBatchTypes: Array[DataType] = projectedBatchSchema.map(_.dataType).toArray
  lazy val originalTypes: Array[DataType] = inputSchema.map(_.dataType)

  /**
   * Append any columns to the batch that need to be materialized
   * @param inputBatch the batch to add columns to
   * @return the batch with columns added
   */
  final def appendProjectedColumns(inputBatch: ColumnarBatch): ColumnarBatch = {
    if (needsComputation.isEmpty) {
      GpuColumnVector.incRefCounts(inputBatch)
    } else {
      withResource(GpuProjectExec.project(inputBatch, needsComputation.map(_.child))) { extra =>
        GpuColumnVector.combineColumns(inputBatch, extra)
      }
    }
  }

  /**
   * Find the upper bounds on data that is the output of `appendProjectedColumns`. Be careful
   * because a batch with no columns/only rows will cause errors and should be special cased.
   * @param findIn the data to look in for upper bounds
   * @param find the data to look for and get the upper bound for
   * @return the rows where the insertions would happen.
   */
  def upperBound(findIn: ColumnarBatch, find: ColumnarBatch): ColumnVector = {
    withResource(GpuColumnVector.from(findIn)) { findInTbl =>
      withResource(GpuColumnVector.from(find)) { findTbl =>
        findInTbl.upperBound(findTbl, cudfOrdering: _*)
      }
    }
  }

  /**
   * Sort a batch of data that is the output of `appendProjectedColumns`. Be careful because
   * a batch with no columns/only rows will cause errors and should be special cased.
   * @param inputBatch the batch to sort
   * @param sortTime metric for the sort time
   * @return a sorted table.
   */
  final def sort(inputBatch: ColumnarBatch, sortTime: GpuMetric): Table = {
    withResource(new NvtxWithMetrics("sort", NvtxColor.DARK_GREEN, sortTime)) { _ =>
      withResource(GpuColumnVector.from(inputBatch)) { toSortTbl =>
        toSortTbl.orderBy(cudfOrdering: _*)
      }
    }
  }

  final def mergeSort(batches: Array[ColumnarBatch], sortTime: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("merge sort", NvtxColor.DARK_GREEN, sortTime)) { _ =>
      if (batches.length == 1) {
        GpuColumnVector.incRefCounts(batches.head)
      } else {
        val merged = withResource(ArrayBuffer[Table]()) { tabs =>
          batches.foreach { cb =>
            tabs += GpuColumnVector.from(cb)
          }
          Table.merge(tabs.toArray, cudfOrdering: _*)
        }
        withResource(merged) { merged =>
          GpuColumnVector.from(merged, projectedBatchTypes)
        }
      }
    }
  }

  /**
   * Get the sort order for a batch of data that is the output of `appendProjectedColumns`.
   * Be careful because a batch with no columns/only rows will cause errors and should be special
   * cased.
   * @param inputBatch the batch to sort
   * @param sortTime metric for the sort time (really the sort order time here)
   * @return a gather map column
   */
  final def computeSortOrder(inputBatch: ColumnarBatch, sortTime: GpuMetric): ColumnVector = {
    withResource(new NvtxWithMetrics("sort_order", NvtxColor.DARK_GREEN, sortTime)) { _ =>
      withResource(GpuColumnVector.from(inputBatch)) { toSortTbl =>
        toSortTbl.sortOrder(cudfOrdering: _*)
      }
    }
  }

  /**
   * Convert a sorted table into a ColumnarBatch and drop any columns added by
   * appendProjectedColumns
   * @param input the table to convert
   * @return the ColumnarBatch
   */
  final def removeProjectedColumns(input: Table): ColumnarBatch =
    GpuColumnVector.from(input, originalTypes,0, numInputColumns)

  /**
   * Append any columns needed for sorting the batch and sort it. Be careful because
   * a batch with no columns/only rows will cause errors and should be special cased.
   * @param inputBatch the batch to sort
   * @param sortTime metric for the sort time
   * @return a sorted table.
   */
  final def appendProjectedAndSort(inputBatch: ColumnarBatch, sortTime: GpuMetric): Table = {
    withResource(appendProjectedColumns(inputBatch)) { toSort =>
      sort(toSort, sortTime)
    }
  }

  /**
   * Sort a batch start to finish. Add any projected columns that are needed to sort, sort the
   * data, and drop the added columns.
   * @param inputBatch the batch to sort
   * @param sortTime metric for the amount of time taken to sort.
   * @param peakDevMemory metric for the peak memory usage
   * @return the sorted batch
   */
  final def fullySortBatch(
      inputBatch: ColumnarBatch,
      sortTime: GpuMetric,
      peakDevMemory: GpuMetric): ColumnarBatch = {
    if (inputBatch.numCols() == 0) {
      // Special case
      return new ColumnarBatch(Array.empty, inputBatch.numRows())
    }

    var peakMem = 0L
    val sortedTbl = withResource(appendProjectedColumns(inputBatch)) { toSort =>
      // inputBatch is completely contained in toSort, so don't need to add it too
      peakMem += GpuColumnVector.getTotalDeviceMemoryUsed(toSort)
      // We are going to skip gathering the computed columns
      // In cases where we don't need the computed columns again this can save some time
      withResource(computeSortOrder(toSort, sortTime)) { gatherMap =>
        withResource(GpuColumnVector.from(inputBatch)) { inputTable =>
          withResource(new NvtxWithMetrics("gather", NvtxColor.DARK_GREEN, sortTime)) { _ =>
            inputTable.gather(gatherMap)
          }
        }
      }
    }
    withResource(sortedTbl) { sortedTbl =>
      peakMem += GpuColumnVector.getTotalDeviceMemoryUsed(sortedTbl)
      peakDevMemory.set(Math.max(peakDevMemory.value, peakMem))
      // We don't need to remove any projected columns, because they were never gathered
      GpuColumnVector.from(sortedTbl, originalTypes)
    }
  }
}
