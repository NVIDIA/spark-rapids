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

package org.apache.spark.sql.rapids.shims.spark331plus

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

object CastOverflowUtil {
  // To support Spark-3.3.1+ CAST_OVERFLOW_IN_TABLE_INSERT
  def castCausingOverflowInTableInsertError(from: DataType,
      to: DataType,
      columnName: String): ArithmeticException = {
    QueryExecutionErrors.castingCauseOverflowErrorInTableInsert(from, to, columnName)
  }
}
