/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "350db"}
{"spark": "351"}
{"spark": "352"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils

private[spark] object SchemaUtilsShims {
  def checkSchemaColumnNameDuplication(
      schema: DataType,
      colType: String,
      caseSensitiveAnalysis: Boolean = false): Unit = {
    SchemaUtils.checkSchemaColumnNameDuplication(schema, caseSensitiveAnalysis)
  }

  def checkSchemaColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    SchemaUtils.checkSchemaColumnNameDuplication(schema, resolver)
  }

  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    SchemaUtils.checkColumnNameDuplication(columnNames, resolver)
  }

  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    SchemaUtils.checkColumnNameDuplication(columnNames, caseSensitiveAnalysis)
  }
}
