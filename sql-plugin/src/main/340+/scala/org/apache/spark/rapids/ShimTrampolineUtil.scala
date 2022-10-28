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

package org.apache.spark.rapids

import org.apache.spark.SparkDateTimeException
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.internal.SQLConf

object ShimTrampolineUtil {
  def dateTimeException(infOrNan: String) = {
    // These are the arguments required by SparkDateTimeException class to create error message.
    val errorClass = "CAST_INVALID_INPUT"
    val messageParameters = Map("expression" -> infOrNan, "sourceType" -> "DOUBLE",
      "targetType" -> "TIMESTAMP", "ansiConfig" -> SQLConf.ANSI_ENABLED.key)
    val context: SQLQueryContext = null
    val summary: String = ""
    new SparkDateTimeException(errorClass, messageParameters, null, "")
  }
}
