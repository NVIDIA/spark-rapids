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

package com.nvidia.spark.rapids

import org.apache.spark.sql.types._

/**
 * Class for helper functions for Date and Timestamp
 */
object DateTimeUtils {

  /**
   * If `t` is date/timestamp type or its children have a date/timestamp type.
   *
   * @param t input date type.
   * @return if contains date type.
   */
  def hasDateOrTimestampType(t: DataType): Boolean = {
    hasType(t, t => t.isInstanceOf[DateType] || t.isInstanceOf[TimestampType])
  }

  /**
   * If the specified date type or its children have a true predicate
   *
   * @param t         input data type.
   * @param predicate predicate for a date type.
   * @return true if date type or its children have a true predicate.
   */
  def hasType(t: DataType, predicate: DataType => Boolean): Boolean = {
    t match {
      case _ if predicate(t) => true
      case MapType(keyType, valueType, _) =>
        hasType(keyType, predicate) || hasType(valueType, predicate)
      case ArrayType(elementType, _) => hasType(elementType, predicate)
      case StructType(fields) => fields.exists(f => hasType(f.dataType, predicate))
      case _ => false
    }
  }
}
