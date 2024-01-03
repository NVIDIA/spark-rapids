/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import ai.rapids.cudf.{BinaryOp, BinaryOperable, ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

object datetimeExpressionsUtils {
  def timestampAddDuration(cv: ColumnView, duration: BinaryOperable): ColumnVector = {
    // Not use cv.add(duration), because of it invoke BinaryOperable.implicitConversion,
    // and currently BinaryOperable.implicitConversion return Long
    // Directly specify the return type is TIMESTAMP_MICROSECONDS
    val resWithOverflow = cv.binaryOp(BinaryOp.ADD, duration, DType.TIMESTAMP_MICROSECONDS)
    closeOnExcept(resWithOverflow) { _ =>
      withResource(resWithOverflow.castTo(DType.INT64)) { resWithOverflowLong =>
        withResource(cv.bitCastTo(DType.INT64)) { cvLong =>
          duration match {
          case dur: Scalar =>
            val durLong = Scalar.fromLong(dur.getLong)
            withResource(durLong) { _ =>
            AddOverflowChecks.basicOpOverflowCheck(
                cvLong, durLong, resWithOverflowLong, "long overflow")
            }
          case dur: ColumnView =>
            withResource(dur.bitCastTo(DType.INT64)) { durationLong =>
            AddOverflowChecks.basicOpOverflowCheck(
                cvLong, durationLong, resWithOverflowLong, "long overflow")
            }
          case _ =>
            throw new UnsupportedOperationException("only scalar and column arguments " +
                s"are supported, got ${duration.getClass}")
          }
        }
      }
    }
    resWithOverflow
  }
}