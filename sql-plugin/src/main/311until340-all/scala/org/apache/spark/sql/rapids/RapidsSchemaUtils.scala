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

package org.apache.spark.sql.rapids

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils

object RapidsSchemaUtils {
  def checkColumnNameDuplication(columnNames: Seq[String],
      colType: String, resolver: Resolver): Unit = {
    SchemaUtils.checkColumnNameDuplication(columnNames, colType, resolver)
  }

  def checkColumnNameDuplication(columnNames: Seq[String],
      colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    SchemaUtils.checkColumnNameDuplication(columnNames, colType, caseSensitiveAnalysis)
  }

  def checkSchemaColumnNameDuplication(
      schema: StructType,
      colType: String,
      resolver: Resolver): Unit = {
    SchemaUtils.checkSchemaColumnNameDuplication(schema, colType, resolver)
  }
}
