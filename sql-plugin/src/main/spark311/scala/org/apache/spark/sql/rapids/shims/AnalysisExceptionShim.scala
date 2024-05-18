/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
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
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.AnalysisException

object AnalysisExceptionShim {

  private def getMessage(errorClass: String, params: Map[String, String]): String = {
    errorClass match {
      case "_LEGACY_ERROR_TEMP_1137" => s"Unable to resolve ${params("name")} given " +
        s"[${params("outputStr")}]"
      case "_LEGACY_ERROR_TEMP_1128" =>
        s"Failed to resolve the schema for ${params("format")} for the partition column: " +
          s"${params("partitionColumn")}. It must be specified manually."
      case "UNABLE_TO_INFER_SCHEMA" =>
        s"Unable to infer schema for ${params("format")}. It must be specified manually."
      case "_LEGACY_ERROR_TEMP_1132" =>
        s"A schema needs to be specified when using ${params("className")}."
      case "_LEGACY_ERROR_TEMP_1133" =>
        "The user-specified schema doesn't match the actual schema: " +
          s"user-specified: ${params("schema")}, actual: ${params("actualSchema")}. If " +
          "you're using DataFrameReader.schema API or creating a table, please do not " +
          "specify the schema. Or if you're scanning an existed table, please drop " +
          "it and re-create it."
      case "_LEGACY_ERROR_TEMP_1134" =>
        s"Unable to infer schema for ${params("format")} at ${params("fileCatalog")}. " +
          "It must be specified manually"
      case "_LEGACY_ERROR_TEMP_1135" =>
        s"${params("className")} is not a valid Spark SQL Data Source."
      case "_LEGACY_ERROR_TEMP_1138" =>
        s"Hive built-in ORC data source must be used with Hive support enabled. " +
          s"Please use the native ORC data source by setting 'spark.sql.orc.impl' to 'native'."
      case "_LEGACY_ERROR_TEMP_1139" =>
        s"Failed to find data source: ${params("provider")}. Avro is built-in but external data " +
          "source module since Spark 2.4. Please deploy the application as per " +
          "the deployment section of \"Apache Avro Data Source Guide\"."
      case "_LEGACY_ERROR_TEMP_1140" =>
        s"Failed to find data source: ${params("provider")}. Please deploy the application as " +
          "per the deployment section of " +
          "\"Structured Streaming + Kafka Integration Guide\"."
      case "_LEGACY_ERROR_TEMP_1141" =>
        s"Multiple sources found for ${params("provider")} " +
          s"(${params("sourceNames")}), please specify the fully qualified class name."
      case "PATH_NOT_FOUND" =>
        s"Path does not exist: ${params("path")}"
      case "_LEGACY_ERROR_TEMP_3079" =>
        "Dynamic partition cannot be the parent of a static partition."
      case "_LEGACY_ERROR_TEMP_1288" =>
        s"Table ${params("tableName")} already exists. You need to drop it first."
      case "_LEGACY_ERROR_TEMP_1241" =>
        s"CREATE-TABLE-AS-SELECT cannot create table with location to a non-empty directory " +
        s"${params("tablePath")}. To allow overwriting the existing non-empty directory, " +
        s"set '${params("config")}' to true."
      case "_LEGACY_ERROR_TEMP_1172" =>
        s"Parquet type not yet supported: ${params("parquetType")}."
      case "_LEGACY_ERROR_TEMP_1173" =>
        s"Illegal Parquet type: ${params("parquetType")}."
      case _ =>
        throw new IllegalStateException(s"Invalid errorClass in ${this.getClass.getSimpleName}")
    }
  }

  def throwException(
    errorClass: String,
    messageParameters: Map[String, String]) =
      throw new AnalysisException(getMessage(errorClass, messageParameters))


  def throwException(msg: String) = throw new AnalysisException(msg)

}