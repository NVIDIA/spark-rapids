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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{ColumnVector, ColumnView}
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuJsonTuple(children: Seq[Expression]) extends GpuExpression 
  with ShimExpression with ExpectsInputTypes {
  override def dataType: DataType = StringType
  override def inputTypes: Seq[DataType] = Seq.fill(children.length)(StringType)
  override def nullable: Boolean = true
  override def prettyName: String = "json_tuple"

  override def columnarEval(batch: ColumnarBatch): Any = {
    val json = children.head
    val paths = children.tail
    def resolveScalar(any: Any): GpuScalar = {
      withResourceIfAllowed(any) {
        case s: GpuScalar => s
        case _ => throw new UnsupportedOperationException("JSON path must be a scalar value")
      }
    }
    withResource(columnarEvalToColumn(json, batch).getBase) { json =>
      withResource(paths.safeMap(p => resolveScalar(p.columnarEval(batch)).getBase)) { scalars =>
        withResource(scalars.safeMap(json.getJSONObject(_))) { cols =>
          ColumnVector.stringConcatenate(cols.toArray[ColumnView])
        }
      }
    }
  }
}
