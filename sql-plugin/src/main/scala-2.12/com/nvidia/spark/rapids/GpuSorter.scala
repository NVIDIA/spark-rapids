/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableProducingSeq, AutoCloseableSeq}

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}

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
class GpuSorter(sortOrder: Seq[SortOrder], inputSchema: Array[Attribute])
    extends GpuSorterBase(sortOrder, inputSchema) {

  /**
   * A class that provides convenience methods for sorting batches of data
   * @param sortOrder The unbound sorting order requested (Should be converted to the GPU)
   * @param inputSchema The schema of the input data
   */
  def this(sortOrder: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(sortOrder, inputSchema.toArray)
  
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
  final def mergeSortAndCloseWithRetry(
      spillableBatches: mutable.ArrayStack[SpillableColumnarBatch],
      sortTime: GpuMetric): SpillableColumnarBatch = {
    closeOnExcept(spillableBatches) { _ =>
      assert(spillableBatches.nonEmpty)
    }
    withResource(new NvtxWithMetrics("merge sort", NvtxColor.DARK_GREEN, sortTime)) { _ =>
      if (spillableBatches.size == 1) {
        // Single batch no need for a merge sort
        spillableBatches.pop()
      } else { // spillableBatches.size > 1
        // In the current version of cudf merge does not work for lists and maps.
        // This should be fixed by https://github.com/rapidsai/cudf/issues/8050
        // Nested types in sort key columns is not supported either.
        if (hasNestedInKeyColumns || hasUnsupportedNestedInRideColumns) {
          // so as a work around we concatenate all of the data together and then sort it.
          // It is slower, but it works
          val merged = RmmRapidsRetryIterator.withRetryNoSplit(spillableBatches) { _ =>
            val tablesToMerge = spillableBatches.safeMap { sb =>
              withResource(sb.getColumnarBatch()) { cb =>
                GpuColumnVector.from(cb)
              }
            }
            val concatenated = withResource(tablesToMerge) { _ =>
              Table.concatenate(tablesToMerge: _*)
            }
            withResource(concatenated) { _ =>
              concatenated.orderBy(cudfOrdering: _*)
            }
          }
          withResource(merged) { _ =>
            closeOnExcept(GpuColumnVector.from(merged, projectedBatchTypes)) { b =>
              SpillableColumnarBatch(b, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
            }
          }
        } else {
          closeOnExcept(spillableBatches) { _ =>
            val batchesToMerge = new mutable.ArrayStack[SpillableColumnarBatch]()
            closeOnExcept(batchesToMerge) { _ =>
              while (spillableBatches.nonEmpty || batchesToMerge.size > 1) {
                // pop a spillable batch if there is one, and add it to `batchesToMerge`.
                if (spillableBatches.nonEmpty) {
                  batchesToMerge.push(spillableBatches.pop())
                }
                if (batchesToMerge.size > 1) {
                  val merged = RmmRapidsRetryIterator.withRetryNoSplit[Table] {
                    val tablesToMerge = batchesToMerge.safeMap { sb =>
                      withResource(sb.getColumnarBatch()) { cb =>
                        GpuColumnVector.from(cb)
                      }
                    }
                    withResource(tablesToMerge) { _ =>
                      Table.merge(tablesToMerge.toArray, cudfOrdering: _*)
                    }
                  }

                  // we no longer care about the old batches, we closed them
                  closeOnExcept(merged) { _ =>
                    batchesToMerge.safeClose()
                    batchesToMerge.clear()
                  }

                  // add the result to be merged with the next spillable batch
                  withResource(merged) { _ =>
                    closeOnExcept(GpuColumnVector.from(merged, projectedBatchTypes)) { b =>
                      batchesToMerge.push(
                        SpillableColumnarBatch(b, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
                    }
                  }
                }
              }
              batchesToMerge.pop()
            }
          }
        }
      }
    }
  }
}
