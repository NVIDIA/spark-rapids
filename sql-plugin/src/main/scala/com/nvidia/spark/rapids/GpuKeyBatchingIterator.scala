/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, NvtxColor, Table}
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Given a stream of data that is sorted by a set of keys, split the data so each batch output
 * contains all of the keys for a given key set.
 */
class GpuKeyBatchingIterator private (
    iter: Iterator[ColumnarBatch],
    sorter: GpuSorter,
    types: Array[DataType],
    numInputRows: GpuMetric,
    numInputBatches: GpuMetric,
    numOutputRows: GpuMetric,
    numOutputBatches: GpuMetric,
    collectTime: GpuMetric,
    concatTime: GpuMetric,
    totalTime: GpuMetric,
    peakDevMemory: GpuMetric,
    spillCallback: SpillCallback)
    extends Iterator[ColumnarBatch] with Arm {
  private val pending = mutable.Queue[SpillableColumnarBatch]()

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  def close(): Unit = {
    pending.foreach(_.close())
    pending.clear()
  }

  override def hasNext: Boolean = pending.nonEmpty || iter.hasNext

  private def getKeyCutoff(cb: ColumnarBatch): Int = {
    withResource(sorter.appendProjectedAndSort(cb, NoopMetric)) { table =>
      val searchTab = withResource(ColumnVector.fromInts(cb.numRows() - 1)) { gatherMap =>
        table.gather(gatherMap)
      }
      val cutoffVec = withResource(searchTab) { searchTab =>
        sorter.lowerBound(table, searchTab)
      }
      withResource(cutoffVec) { cutoffVec =>
        withResource(cutoffVec.copyToHost()) { vecHost =>
          assert(vecHost.getRowCount == 1)
          vecHost.getInt(0)
        }
      }
    }
  }

  private def concatPending(last: Option[Table] = None): ColumnarBatch = {
    var peak = 0L
    try {
      withResource(new NvtxWithMetrics("concat pending", NvtxColor.CYAN, concatTime)) { _ =>
        withResource(mutable.ArrayBuffer[Table]()) { toConcat =>
          while (pending.nonEmpty) {
            withResource(pending.dequeue()) { spillable =>
              withResource(spillable.getColumnarBatch()) { cb =>
                peak += GpuColumnVector.getTotalDeviceMemoryUsed(cb)
                toConcat.append(GpuColumnVector.from(cb))
              }
            }
          }
          last.foreach { lastTab =>
            peak += GpuColumnVector.getTotalDeviceMemoryUsed(lastTab)
            toConcat.append(lastTab)
          }
          if (toConcat.length > 1) {
            withResource(Table.concatenate(toConcat: _*)) { concated =>
              peak += GpuColumnVector.getTotalDeviceMemoryUsed(concated)
              GpuColumnVector.from(concated, types)
            }
          } else {
            GpuColumnVector.from(toConcat.head, types)
          }
        }
      }
    } finally {
      peakDevMemory.set(Math.max(peakDevMemory.value, peak))
    }
  }

  override def next(): ColumnarBatch = {
    var startHasNext = System.nanoTime()
    while (iter.hasNext) {
      val hasNextTime = System.nanoTime() - startHasNext
      totalTime += hasNextTime
      collectTime += hasNextTime
      val startNextTime = System.nanoTime()
      withResource(iter.next()) { cb =>
        val nextTime = System.nanoTime() - startNextTime
        totalTime += nextTime
        collectTime += nextTime
        numInputRows += cb.numRows()
        numInputBatches += 1
        withResource(new MetricRange(totalTime)) { _ =>
          if (GpuColumnVector.isTaggedAsFinalBatch(cb)) {
            // No need to do a split on the final row and create extra work this is the last batch
            withResource(GpuColumnVector.from(cb)) { table =>
              val ret = concatPending(Some(table))
              numOutputRows += ret.numRows()
              numOutputBatches += 1
              return ret
            }
          }
          // else not the last batch split so the final key group is not in this batch...
          val cutoff = getKeyCutoff(cb)
          if (cutoff <= 0) {
            peakDevMemory.set(
              Math.max(peakDevMemory.value, GpuColumnVector.getTotalDeviceMemoryUsed(cb)))
            // Everything is for a single key, so save it away and try the next batch...
            pending +=
                SpillableColumnarBatch(GpuColumnVector.incRefCounts(cb),
                  SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
          } else {
            var peak = GpuColumnVector.getTotalDeviceMemoryUsed(cb)
            withResource(GpuColumnVector.from(cb)) { table =>
              withResource(table.contiguousSplit(cutoff)) { tables =>
                assert(tables.length == 2)
                val ret = concatPending(Some(tables(0).getTable))
                peak += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
                peak += tables(1).getBuffer.getLength
                pending +=
                    SpillableColumnarBatch(tables(1), types,
                      SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
                numOutputRows += ret.numRows()
                numOutputBatches += 1
                peakDevMemory.set(Math.max(peakDevMemory.value, peak))
                return ret
              }
            }
          }
        }
      }
      startHasNext = System.nanoTime()
    }
    val ret = withResource(new MetricRange(totalTime)) { _ =>
      // At the end of the iterator, nothing more to process
      concatPending()
    }
    numOutputRows += ret.numRows()
    numOutputBatches += 1
    ret
  }
}

object GpuKeyBatchingIterator {
  def makeFunc(
      unboundOrderSpec: Seq[SortOrder],
      schema: Array[Attribute],
      numInputRows: GpuMetric,
      numInputBatches: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      collectTime: GpuMetric,
      concatTime: GpuMetric,
      totalTime: GpuMetric,
      peakDevMemory: GpuMetric,
      spillCallback: SpillCallback): Iterator[ColumnarBatch] => GpuKeyBatchingIterator = {
    val sorter = new GpuSorter(unboundOrderSpec, schema)
    val types = schema.map(_.dataType)
    def makeIter(iter: Iterator[ColumnarBatch]): GpuKeyBatchingIterator = {
      new GpuKeyBatchingIterator(iter, sorter, types,
        numInputRows, numInputBatches, numOutputRows, numOutputBatches,
        collectTime, concatTime, totalTime, peakDevMemory, spillCallback)
    }
    makeIter
  }
}