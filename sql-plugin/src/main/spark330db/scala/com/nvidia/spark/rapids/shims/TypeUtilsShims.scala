/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.{DataType, NullType, NumericType}

/**
 * Reimplement the function `checkForNumericExpr` which has been removed since
 * Spark 3.4.0
 */
object TypeUtilsShims {
  def checkForNumericExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (dt.isInstanceOf[NumericType] || dt == NullType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"$caller requires numeric types, not ${dt.catalogString}")
    }
  }
}
