/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, DateType, DayTimeIntervalType, IntegerType, TimestampNTZType, TimestampType, YearMonthIntervalType}

object Spark32XShimsUtils {

  def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    ss.leafNodeDefaultParallelism
  }

  def isValidRangeFrameType(orderSpecType: DataType, ft: DataType): Boolean = {
    (orderSpecType, ft) match {
      case (DateType, IntegerType) => true
      case (DateType, _: YearMonthIntervalType) => true
      case (TimestampType | TimestampNTZType, CalendarIntervalType) => true
      case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) => true
      case (TimestampType | TimestampNTZType, _: DayTimeIntervalType) => true
      case (a, b) => a == b
    }
  }

}
