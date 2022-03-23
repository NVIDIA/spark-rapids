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

import ai.rapids.cudf.{ColumnVector, ColumnView, Scalar}

import org.apache.spark.sql.catalyst.expressions.{Expression, TimeZoneAwareExpression}
import org.apache.spark.sql.rapids.GpuTimeMath

case class GpuTimeAdd(start: Expression,
                      interval: Expression,
                      timeZoneId: Option[String] = None)
  extends GpuTimeMath(start, interval, timeZoneId) {

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def intervalMath(us_s: Scalar, us: ColumnView): ColumnVector = {
    us.add(us_s)
  }
}
