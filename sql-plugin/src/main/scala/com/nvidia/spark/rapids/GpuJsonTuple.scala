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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.GpuExpressionsUtils.columnarEvalToColumn
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuJsonTuple(children: Seq[Expression]) extends GpuGenerator {
  // override def dataType: DataType = StringType
  // override def nullable: Boolean = false
  
  // @transient private lazy val json: Expression = children.head
  // @transient private lazy val fields: Seq[Expression] = children.tail

  @transient private lazy val jsonExpr: Expression = children.head
  @transient private lazy val fieldExpressions: Seq[Expression] = children.tail
  override def prettyName: String = "json_tuple"

  override def elementSchema: StructType = StructType(fieldExpressions.zipWithIndex.map {
    case (_, idx) => StructField(s"c$idx", StringType, nullable = true)
  })

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length < 2) {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName has wrong number of auguments: requires > 1, but found ${children.length}"
      )
    } else if (children.forall(child => child.dataType == StringType)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports string type input")
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    def resolveScalar(any: Any): GpuScalar = {
      withResourceIfAllowed(any) {
        case s: GpuScalar => s
        case _ => throw new UnsupportedOperationException("JSON path must be a scalar value")
      }
    }
    withResource(columnarEvalToColumn(json, batch).getBase) { json =>
      withResource(fields.safeMap(p => resolveScalar(p.columnarEval(batch)).getBase)) { scalars =>
        withResource(scalars.safeMap(json.getJSONObject(_))) { cols =>
          cols.foreach(GpuColumnVector.debug("Get JSON res:", _))
          ColumnVector.makeList(cols: _*)
          // ColumnVector.concatenate(cols: _*)
          // cols
        }
      }
    }
  }
}
