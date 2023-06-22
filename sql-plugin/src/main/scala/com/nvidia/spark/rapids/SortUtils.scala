/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, NvtxColor, OrderByArg, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BoundReference, Expression, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, MapType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object SortUtils {
  @scala.annotation.tailrec
  def extractReference(exp: Expression): Option[GpuBoundReference] = exp match {
    case r: GpuBoundReference => Some(r)
    case a: Alias => extractReference(a.child)
    case _ => None
  }

  def getOrder(order: SortOrder, index: Int): OrderByArg =
    if (order.isAscending) {
      OrderByArg.asc(index, order.nullOrdering == NullsFirst)
    } else {
      OrderByArg.desc(index, order.nullOrdering == NullsLast)
    }
}

/**
 * A class that provides convenience methods for sorting batches of data. A Spark SortOrder
 * typically will just reference a single column using an AttributeReference. This is the simplest
 * situation so we just need to bind the attribute references to where they go, but it is possible
 * that some computation can be done in the SortOrder.  This would be a situation like sorting
 * strings by their length instead of in lexicographical order. Because cudf does not support this
 * directly we instead go through the SortOrder instances that are a part of this sorter and find
 * the ones that require computation. We then do the sort in a few stages first we compute any
 * needed columns from the SortOrder instances that require some computation, and add them to the
 * original batch.  The method `appendProjectedColumns` does this. This then provides a number of
 * methods that can be used to operate on a batch that has these new columns added to it. These
 * include sorting, merge sorting, and finding bounds. These can be combined in various ways to
 * do different algorithms. When you are done with these different operations you can drop the
 * temporary columns that were added, just for computation, using `removeProjectedColumns`.
 * Some times you may want to pull data back to the CPU and sort rows there too. We provide
 * `cpuOrders` that lets you do this on rows that have had the extra ordering columns added to them.
 * This also provides `fullySortBatch` as an optimization. If all you want to do is sort a batch
 * you don't want to have to sort the temp columns too, and this provide that.
 * @param sortOrder The unbound sorting order requested (Should be converted to the GPU)
 * @param inputSchema The schema of the input data
 */
class GpuSorter(
    val sortOrder: Seq[SortOrder],
    inputSchema: Array[Attribute]) extends Serializable {

  /**
   * A class that provides convenience methods for sorting batches of data
   * @param sortOrder The unbound sorting order requested (Should be converted to the GPU)
   * @param inputSchema The schema of the input data
   */
  def this(sortOrder: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(sortOrder, inputSchema.toArray)

  private[this] val boundSortOrder = GpuBindReferences.bindReferences(sortOrder, inputSchema.toSeq)

  private[this] val numInputColumns = inputSchema.length

  /**
   * A sort order that the CPU can use to sort data that is the output of `appendProjectedColumns`.
   * You cannot use the regular sort order directly because it has been translated to the GPU when
   * computation is needed.
   */
  def cpuOrdering: Seq[SortOrder] = cpuOrderingInternal.toSeq

  private[this] lazy val (sortOrdersThatNeedComputation, cudfOrdering, cpuOrderingInternal) = {
    val sortOrdersThatNeedsComputation = mutable.ArrayBuffer[SortOrder]()
    val cpuOrdering = mutable.ArrayBuffer[SortOrder]()
    val cudfOrdering = mutable.ArrayBuffer[OrderByArg]()
    var newColumnIndex = numInputColumns
    // Remove duplicates in the ordering itself because
    // there is no need to do it twice.
    boundSortOrder.distinct.foreach { so =>
      SortUtils.extractReference(so.child) match {
        case Some(ref) =>
          cudfOrdering += SortUtils.getOrder(so, ref.ordinal)
          // It is a bound GPU reference so we have to translate it to the CPU
          cpuOrdering += SortOrder(
            BoundReference(ref.ordinal, ref.dataType, ref.nullable),
            so.direction, so.nullOrdering, Seq.empty)
        case None =>
          val index = newColumnIndex
          newColumnIndex += 1
          cudfOrdering += SortUtils.getOrder(so, index)
          sortOrdersThatNeedsComputation += so
          // We already did the computation so instead of trying to translate
          // the computation back to the CPU too, just use the existing columns.
          cpuOrdering += SortOrder(
            BoundReference(index, so.dataType, so.nullable),
            so.direction, so.nullOrdering, Seq.empty)
      }
    }
    (sortOrdersThatNeedsComputation.toArray, cudfOrdering.toArray, cpuOrdering.toArray)
  }

  /**
   * A schema for columns that are computed as a part of sorting. The name of this field should be
   * ignored as it is a temporary implementation detail. The order and types of them are what
   * matter.
   */
  private[this] lazy val computationSchema: Seq[Attribute] =
    sortOrdersThatNeedComputation.map { so =>
      AttributeReference("SORT_TMP", so.dataType, so.nullable)()
    }

  /**
   * Some SortOrder instances require adding temporary columns which is done as a part of
   * the `appendProjectedColumns` method. This is the schema for the result of that method.
   */
  lazy val projectedBatchSchema: Seq[Attribute] = inputSchema ++ computationSchema
  /**
   * The types and order for the columns returned by `appendProjectedColumns`
   */
  lazy val projectedBatchTypes: Array[DataType] = projectedBatchSchema.map(_.dataType).toArray
  /**
   * The original input types without any temporary columns added to them needed for sorting.
   */
  lazy val originalTypes: Array[DataType] = inputSchema.map(_.dataType)

  /**
   * Append any columns to the batch that need to be materialized for sorting to work.
   * @param inputBatch the batch to add columns to
   * @return the batch with columns added
   */
  final def appendProjectedColumns(inputBatch: ColumnarBatch): ColumnarBatch = {
    if (sortOrdersThatNeedComputation.isEmpty) {
      GpuColumnVector.incRefCounts(inputBatch)
    } else {
      withResource(GpuProjectExec.project(inputBatch,
        sortOrdersThatNeedComputation.map(_.child))) { extra =>
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
   * Find the lower bounds on data that is the output of `appendProjectedColumns`. Be careful
   * because a batch with no columns/only rows will cause errors and should be special cased.
   * @param findIn the data to look in for lower bounds
   * @param find the data to look for and get the lower bound for
   * @return the rows where the insertions would happen.
   */
  def lowerBound(findIn: ColumnarBatch, find: ColumnarBatch): ColumnVector = {
    withResource(GpuColumnVector.from(findIn)) { findInTbl =>
      withResource(GpuColumnVector.from(find)) { findTbl =>
        findInTbl.lowerBound(findTbl, cudfOrdering: _*)
      }
    }
  }

  /**
   * Find the lower bounds on data that is the output of `appendProjectedColumns`. Be careful
   * because a batch with no columns/only rows will cause errors and should be special cased.
   * @param findIn the data to look in for lower bounds
   * @param find the data to look for and get the lower bound for
   * @return the rows where the insertions would happen.
   */
  def lowerBound(findIn: Table, find: Table): ColumnVector =
    findIn.lowerBound(find, cudfOrdering: _*)

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

  private[this] lazy val hasNestedInKeyColumns = cpuOrderingInternal.exists { order =>
    order.child.dataType match {
      case _: BinaryType =>
        // binary is represented in cudf as a LIST column of UINT8
        true
      case t => DataTypeUtils.isNestedType(t)
    }
  }

  /** (This can be removed once https://github.com/rapidsai/cudf/issues/8050 is addressed) */
  private[this] lazy val hasUnsupportedNestedInRideColumns = {
    val keyColumnIndices = cpuOrderingInternal.map(_.child.asInstanceOf[BoundReference].ordinal)
    val rideColumnIndices = projectedBatchTypes.indices.toSet -- keyColumnIndices
    rideColumnIndices.exists { idx =>
      TrampolineUtil.dataTypeExistsRecursively(projectedBatchTypes(idx),
        t => t.isInstanceOf[ArrayType] || t.isInstanceOf[MapType] || t.isInstanceOf[BinaryType])
    }
  }

  /**
   * Merge multiple batches together. All of these batches should be the output of
   * `appendProjectedColumns` and the output of this will also be in that same format.
   *
   * After this function is called, the argument `spillableBatches` should not be used.
   *
   * @param spillableBatches the spillable batches to sort
   * @param sortTime metric for the time spent doing the merge sort
   * @return the sorted data.
   */
  final def mergeSortAndClose(
      spillableBatches: mutable.ArrayStack[SpillableColumnarBatch],
      sortTime: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("merge sort", NvtxColor.DARK_GREEN, sortTime)) { _ =>
      if (spillableBatches.size == 1) {
        // Single batch no need for a merge sort
        withResource(spillableBatches.pop()) { sb =>
          sb.getColumnarBatch()
        }
      } else {
        val tablesToMerge = new mutable.ArrayStack[Table]()
        closeOnExcept(spillableBatches) { _ =>
          val merged: Table = {
            // In the current version of cudf merge does not work for lists and maps.
            // This should be fixed by https://github.com/rapidsai/cudf/issues/8050
            // Nested types in sort key columns is not supported either.
            if (hasNestedInKeyColumns || hasUnsupportedNestedInRideColumns) {
              // so as a work around we concatenate all of the data together and then sort it.
              // It is slower, but it works
              val concatenated = closeOnExcept(tablesToMerge) { _ =>
                while (spillableBatches.nonEmpty) {
                  withResource(spillableBatches.pop()) { sb =>
                    withResource(sb.getColumnarBatch()) { cb =>
                      tablesToMerge.push(GpuColumnVector.from(cb))
                    }
                  }
                }
                val concatenated = withResource(tablesToMerge) { _ =>
                  Table.concatenate(tablesToMerge: _*)
                }
                // we no longer care about the old tables, we closed them
                tablesToMerge.clear()
                concatenated
              }
              withResource(concatenated) { _ =>
                concatenated.orderBy(cudfOrdering: _*)
              }
            } else {
              while (spillableBatches.nonEmpty || tablesToMerge.size > 1) {
                // pop a spillable batch if there is one to pop
                var spillableCandidate: Option[SpillableColumnarBatch] = None
                if (spillableBatches.nonEmpty) {
                  spillableCandidate = Some(spillableBatches.pop())
                }

                // optionally add a table to `tablesToMerge`
                closeOnExcept(tablesToMerge) { _ =>
                  withResource(spillableCandidate) {
                    _.foreach { sb =>
                      // materialize a potentially spilled batch
                      withResource(sb.getColumnarBatch()) { batch =>
                        tablesToMerge.push(GpuColumnVector.from(batch))
                      }
                    }
                  }
                }

                if (tablesToMerge.size > 1) {
                  val merged = withResource(tablesToMerge) { _ =>
                    Table.merge(tablesToMerge.toArray, cudfOrdering: _*)
                  }
                  // we no longer care about the old tables, we closed them
                  tablesToMerge.clear()

                  // add the result to be merged with the next spillable batch
                  tablesToMerge.push(merged)
                }
              }
              tablesToMerge.pop()
            }
          }
          withResource(merged) { merged =>
            GpuColumnVector.from(merged, projectedBatchTypes)
          }
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
    GpuColumnVector.from(input, originalTypes, 0, numInputColumns)

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
   * @return the sorted batch
   */
  final def fullySortBatch(
      inputBatch: ColumnarBatch,
      sortTime: GpuMetric): ColumnarBatch = {
    if (inputBatch.numCols() == 0) {
      // Special case
      return new ColumnarBatch(Array.empty, inputBatch.numRows())
    }

    val sortedTbl = withResource(appendProjectedColumns(inputBatch)) { toSort =>
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
      // We don't need to remove any projected columns, because they were never gathered
      GpuColumnVector.from(sortedTbl, originalTypes)
    }
  }
}
