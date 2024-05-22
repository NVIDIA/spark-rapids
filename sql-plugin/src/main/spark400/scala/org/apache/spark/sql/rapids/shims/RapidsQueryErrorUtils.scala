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
{"spark": "400"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType

trait RapidsQueryErrorUtils {

  def createTableAsSelectWithNonEmptyDirectoryError(tablePath: String, conf: String): Throwable = {
    QueryCompilationErrors.createTableAsSelectWithNonEmptyDirectoryError(tablePath)
  }

  def cannotResolveAttributeError(name: String, outputStr: String): Throwable = {
    QueryCompilationErrors.cannotResolveAttributeError(name, outputStr)
  }

  def partitionColumnNotSpecifiedError(format: String, partitionColumn: String): Throwable = {
    QueryCompilationErrors.partitionColumnNotSpecifiedError(format, partitionColumn)
  }

  def dataSchemaNotSpecifiedError(format: String): Throwable = {
    QueryCompilationErrors.dataSchemaNotSpecifiedError(format)
  }

  def schemaNotSpecifiedForSchemaRelationProviderError(className: String): Throwable = {
    QueryCompilationErrors.schemaNotSpecifiedForSchemaRelationProviderError(className)
  }

  def userSpecifiedSchemaMismatchActualSchemaError(
    schema: StructType,
    actualSchema: StructType): Throwable = {
    QueryCompilationErrors.userSpecifiedSchemaMismatchActualSchemaError(schema, actualSchema)
  }

  def dataSchemaNotSpecifiedError(format: String, fileCatalog: String): Throwable = {
    QueryCompilationErrors.dataSchemaNotSpecifiedError(format, fileCatalog)
  }

  def invalidDataSourceError(className: String): Throwable = {
    QueryCompilationErrors.invalidDataSourceError(className)
  }

  def orcNotUsedWithHiveEnabledError(): Throwable = {
    QueryCompilationErrors.orcNotUsedWithHiveEnabledError()
  }

  def failedToFindAvroDataSourceError(provider: String): Throwable = {
    QueryCompilationErrors.failedToFindAvroDataSourceError(provider)
  }

  def failedToFindKafkaDataSourceError(provider: String): Throwable = {
    QueryCompilationErrors.failedToFindKafkaDataSourceError(provider)
  }

  def findMultipleDataSourceError(provider: String, sourceNames: Seq[String]): Throwable = {
    QueryCompilationErrors.findMultipleDataSourceError(provider, sourceNames)
  }

  def dataPathNotExistError(path: String): Throwable = {
    QueryCompilationErrors.dataPathNotExistError(path)
  }

  def dynamicPartitionParentError: Throwable = {
  /** This exception doesn't have a helper method so the errorClass has to be hardcoded */
    throw new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_3079",
      messageParameters = Map.empty)
  }

  def tableOrViewAlreadyExistsError(tableName: String): Throwable = {
    QueryCompilationErrors.tableOrViewAlreadyExistsError(tableName)
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    QueryCompilationErrors.parquetTypeUnsupportedYetError(parquetType)
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    QueryCompilationErrors.illegalParquetTypeError(parquetType)
  }
}