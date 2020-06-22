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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.GpuExpressionsUtils.evaluateBoundExpressions

import org.apache.spark.sql.catalyst.expressions.{NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.vectorized.ColumnarBatch

object SortUtils {
  /*
  * This function takes the input batch and the bound sort order references and
  * evaluates each column in case its an expression. It then appends the original columns
  * after the sort key columns. The sort key columns will be dropped after sorting.
  */
  def getGpuColVectorsAndBindReferences(batch: ColumnarBatch,
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
}
