/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims

import java.util.concurrent.TimeUnit

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

case class GpuTimeAdd(start: Expression,
                      interval: Expression,
                      timeZoneId: Option[String] = None)
  extends ShimBinaryExpression
    with GpuExpression
    with TimeZoneAwareExpression
    with ExpectsInputTypes
    with Serializable {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  val microSecondsInOneDay: Long = TimeUnit.DAYS.toMicros(1)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def inputTypes: Seq[AbstractDataType] =
    Seq(AnyTimestampType, TypeCollection(CalendarIntervalType, DayTimeIntervalType))

  override def dataType: DataType = start.dataType

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEval(batch)) { rhs =>
        (lhs, rhs) match {
          case (l: GpuColumnVector, intvlS: GpuScalar) =>
            val interval = intvlS.dataType match {
              case CalendarIntervalType =>
                // Scalar does not support 'CalendarInterval' now, so use
                // the Scala value instead.
                // Skip the null check because it wll be detected by the following calls.
                val intvl = intvlS.getValue.asInstanceOf[CalendarInterval]
                if (intvl.months != 0) {
                  throw new UnsupportedOperationException("Months aren't supported at the moment")
                }
                intvl.days * microSecondsInOneDay + intvl.microseconds
              case _: DayTimeIntervalType =>
                // Scalar does not support 'DayTimeIntervalType' now, so use
                // the Scala value instead.
                intvlS.getValue.asInstanceOf[Long]
              case _ =>
                throw new UnsupportedOperationException("GpuTimeAdd unsupported data type: " +
                  intvlS.dataType)
            }

            if (interval != 0) {
              withResource(Scalar.fromLong(interval)) { us_s =>
                withResource(l.getBase.bitCastTo(DType.INT64)) { us =>
                  withResource(intervalMath(us_s, us)) { longResult =>
                    GpuColumnVector.from(longResult.castTo(DType.TIMESTAMP_MICROSECONDS),
                      dataType)
                  }
                }
              }
            } else {
              l.incRefCount()
            }
          case _ =>
            throw new UnsupportedOperationException("GpuTimeAdd takes column and interval as an " +
              "argument only")
        }
      }
    }
  }

  private def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.add(us_s)
  }
}
