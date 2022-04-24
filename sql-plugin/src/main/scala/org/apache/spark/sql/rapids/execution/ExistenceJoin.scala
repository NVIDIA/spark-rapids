/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ColumnVector, GatherMap, NvtxColor, Scalar, Table}
import com.nvidia.spark.rapids.{Arm, GpuColumnVector, GpuMetric, LazySpillableColumnarBatch, NvtxWithMetrics, TaskAutoCloseableResource}

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Existence join generates an `exists` boolean column with `true` or `false` in it,
 * then appends it to the `output` columns. The true in `exists` column indicates left table should
 * retain that row, the row number of `exists` equals to the row number of left table.
 *
 * e.g.:
 * <code>
 * select * from left_table where
 *   left_table.column_0 >= 3
 *   or
 *   exists (select * from right_table where left_table.column_1 < right_table.column_1)
 *
 * Explanation of this sql is:
 *
 * Filter(left_table.column_0 >= 3 or `exists`)
 *   Existence_join (left + `exists`) // `exists` do not shrink or expand the rows of left table
 *     left_table
 *     right_table
 * </code>
 */
abstract class ExistenceJoinIterator(
    spillableBuiltBatch: LazySpillableColumnarBatch,
    lazyStream: Iterator[LazySpillableColumnarBatch],
    opTime: GpuMetric,
    joinTime: GpuMetric
) extends Iterator[ColumnarBatch]()
    with TaskAutoCloseableResource with Arm {

  use(spillableBuiltBatch)

  def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap

  override def hasNext: Boolean = {
    val streamHasNext = lazyStream.hasNext
    if (!streamHasNext) {
      close()
    }
    streamHasNext
  }

  override def next(): ColumnarBatch = {
    withResource(lazyStream.next()) { lazyBatch =>
      withResource(new NvtxWithMetrics("existence join batch", NvtxColor.ORANGE, joinTime)) { _ =>
        opTime.ns {
          val ret = existenceJoinNextBatch(lazyBatch.getBatch)
          spillableBuiltBatch.allowSpilling()
          ret
        }
      }
    }
  }

  override def close(): Unit = {
    opTime.ns {
      super.close()
    }
  }

  private def existenceJoinNextBatch(leftColumnarBatch: ColumnarBatch): ColumnarBatch = {
    // left columns with exists
    withResource(existsScatterMap(leftColumnarBatch)) { gatherMap =>
      existenceJoinResult(leftColumnarBatch, gatherMap)
    }
  }

  /**
   * Generate existence join result according to `gatherMap`: left columns with `exists` column
   */
  def existenceJoinResult(leftColumnarBatch: ColumnarBatch, gatherMap: GatherMap): ColumnarBatch = {
    // left columns with exists
    withResource(existsColumn(leftColumnarBatch, gatherMap)) { existsColumn =>
      val resCols = GpuColumnVector.extractBases(leftColumnarBatch) :+ existsColumn
      val resTypes = GpuColumnVector.extractTypes(leftColumnarBatch) :+ BooleanType
      withResource(new Table(resCols: _*)) { resTab =>
        GpuColumnVector.from(resTab, resTypes)
      }
    }
  }

  private def existsColumn(leftColumnarBatch: ColumnarBatch,
      existsScatterMap: GatherMap): ColumnVector = {
    val numLeftRows = leftColumnarBatch.numRows
    withResource(falseColumnTable(numLeftRows)) { allFalseTable =>
      val numExistsTrueRows = existsScatterMap.getRowCount.toInt
      withResource(existsScatterMap.toColumnView(0, numExistsTrueRows)) { existsView =>
        withResource(Scalar.fromBool(true)) { trueScalar =>
          withResource(Table.scatter(Array(trueScalar), existsView, allFalseTable, false)) {
            _.getColumn(0).incRefCount()
          }
        }
      }
    }
  }

  private def falseColumnTable(numLeftRows: Int): Table = {
    withResource(Scalar.fromBool(false)) { falseScalar =>
      withResource(ai.rapids.cudf.ColumnVector.fromScalar(falseScalar, numLeftRows)) {
        new Table(_)
      }
    }
  }
}
