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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.rapids.GpuTimeUnaryExpression
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuToPrettyString(child: Expression, timeZoneId: Option[String] = None)
  extends GpuTimeUnaryExpression {

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override lazy val resolved: Boolean = childrenResolved

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    val c = child.columnarEvalAny(batch)
    if (c.isInstanceOf[GpuColumnVector]) {
      processColumnVector(c.asInstanceOf[GpuColumnVector])
    } else {
      if (!c.asInstanceOf[Scalar].isValid) {
        withResource(c.asInstanceOf[Scalar]) { sc =>
          Scalar.fromString("NULL")
        }
      } else {
        c
      }
    }
  }

  private def processColumnVector(cv: GpuColumnVector) = {
    if (cv.hasNull) {
      withResource(cv.asInstanceOf[GpuColumnVector]) { columnVector =>
        withResource(columnVector.getBase.isNull) { isNull =>
          withResource(Scalar.fromString("NULL")) { nullString =>
            GpuColumnVector.from(isNull.ifElse(nullString, cv.getBase), StringType)
          }
        }
      }
    } else {
      cv
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val c = child.columnarEval(batch)
    processColumnVector(c)
  }

  override def doColumnar(input: GpuColumnVector): ColumnVector = {
    GpuCast.doCast(input.getBase, input.dataType(), StringType, false,
      legacyCastToString = false, stringToDateAnsiModeEnabled = false)
  }
}