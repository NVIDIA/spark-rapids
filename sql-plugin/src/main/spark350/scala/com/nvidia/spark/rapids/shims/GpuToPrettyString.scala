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
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuToPrettyString(child: Expression, timeZoneId: Option[String] = None)
  extends ShimUnaryExpression with GpuExpression with ToStringBase with TimeZoneAwareExpression {

  override lazy val resolved: Boolean = childrenResolved

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def withTimeZone(timeZoneId: String): GpuToPrettyString =
    copy(timeZoneId = Some(timeZoneId))

  override def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override protected def leftBracket: String = "{"

  override protected def rightBracket: String = "}"

  override protected def nullString: String = "NULL"

  override protected def useDecimalPlainString: Boolean = true

  override protected def useHexFormatForBinary: Boolean = true

  private def processColumnVector(cv: GpuColumnVector) = {
    val stringCol = withResource(cv) { originalCol =>
      castToString(originalCol.getBase, originalCol.dataType(), false, false, false)
    }

    if (stringCol.hasNulls()) {
      withResource(stringCol.asInstanceOf[GpuColumnVector]) { columnVector =>
        withResource(columnVector.getBase.isNull()) { isNull =>
          withResource(Scalar.fromString(nullString)) { nullString =>
            GpuColumnVector.from(isNull.ifElse(nullString, stringCol), StringType)
          }
        }
      }
    } else {
      GpuColumnVector.from(stringCol, StringType)
    }
  }

  override def columnarEvalAny(batch: ColumnarBatch): Any = {
    val c = child.columnarEvalAny(batch)
    if (c.isInstanceOf[GpuColumnVector]) {
      processColumnVector(c.asInstanceOf[GpuColumnVector])
    } else {
      withResource(c.asInstanceOf[Scalar]) { scalar =>
        if (!scalar.isValid) {
          withResource(c.asInstanceOf[Scalar]) { sc =>
            Scalar.fromString(nullString)
          }
        } else {
          if (scalar.getType == DType.STRING) {
            // TODO: unsure if we should cast it or just return as is
            scalar.incRefCount()
          } else {
            GpuScalar.from(GpuScalar.extract(scalar).toString, StringType)
          }
        }
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val c = child.columnarEval(batch)
    processColumnVector(c)
  }

}