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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

object ArrowUtilsShim {
  def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] =
    ArrowPythonRunner.getPythonRunnerConfMap(conf)

  def toArrowSchema(schema: StructType, timeZoneId: String,
      errorOnDuplicatedFieldNames: Boolean = true, largeVarTypes: Boolean = false): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  }
}