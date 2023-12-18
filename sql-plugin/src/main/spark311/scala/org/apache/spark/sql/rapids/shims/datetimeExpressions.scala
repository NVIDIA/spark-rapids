/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import java.util.concurrent.TimeUnit

import ai.rapids.cudf.{BinaryOp, BinaryOperable, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression, GpuScalar}
import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import com.nvidia.spark.rapids.GpuOverrides
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.jni.GpuTimeZoneDB
import com.nvidia.spark.rapids.shims.ShimBinaryExpression

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.CalendarInterval

case class GpuTimeAdd(
    start: Expression,
    interval: Expression,
    timeZoneId: Option[String] = None)
   extends ShimBinaryExpression
       with GpuExpression
       with TimeZoneAwareExpression
       with ExpectsInputTypes
       with Serializable {

  def this(start: Expression, interval: Expression) = this(start, interval, None)

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left - $right"
  override def sql: String = s"${left.sql} - ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  val microSecondsInOneDay: Long = TimeUnit.DAYS.toMicros(1)

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    withResourceIfAllowed(left.columnarEval(batch)) { lhs =>
      withResourceIfAllowed(right.columnarEvalAny(batch)) { rhs =>
        // lhs is start, rhs is interval
        (lhs, rhs) match {
          case (l, intvlS: GpuScalar) if intvlS.dataType.isInstanceOf[CalendarIntervalType] =>
            // Scalar does not support 'CalendarInterval' now, so use
            // the Scala value instead.
            // Skip the null check because it wll be detected by the following calls.
            val intvl = intvlS.getValue.asInstanceOf[CalendarInterval]
            if (intvl.months != 0) {
              throw new UnsupportedOperationException("Months aren't supported at the moment")
            }
            val usToSub = intvl.days * microSecondsInOneDay + intvl.microseconds
            if (usToSub != 0) {
              val res = if (GpuOverrides.isUTCTimezone(zoneId)) {
                withResource(Scalar.durationFromLong(DType.DURATION_MICROSECONDS, usToSub)) { d =>
                  timestampAddDuration(l.getBase, d)
                }
              } else {
                val utcRes = withResource(GpuTimeZoneDB.fromUtcTimestampToTimestamp(l.getBase,
                  zoneId)) { utcTimestamp =>
                  withResource(Scalar.durationFromLong(DType.DURATION_MICROSECONDS, usToSub)) { 
                    d => timestampAddDuration(utcTimestamp, d)
                  }
                }
                withResource(utcRes) { _ =>
                  GpuTimeZoneDB.fromTimestampToUtcTimestamp(utcRes, zoneId)
                }
              }
              GpuColumnVector.from(res, dataType)
            } else {
              l.incRefCount()
            }
          case _ =>
            throw new UnsupportedOperationException("only column and interval arguments " +
              s"are supported, got left: ${lhs.getClass} right: ${rhs.getClass}")
        }
      }
    }
  }

  private def timestampAddDuration(cv: ColumnView, duration: BinaryOperable): ColumnVector = {
    // Not use cv.add(duration), because of it invoke BinaryOperable.implicitConversion,
    // and currently BinaryOperable.implicitConversion return Long
    // Directly specify the return type is TIMESTAMP_MICROSECONDS
    cv.binaryOp(BinaryOp.ADD, duration, DType.TIMESTAMP_MICROSECONDS)
  }
}
