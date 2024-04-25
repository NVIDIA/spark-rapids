/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION.
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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

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
