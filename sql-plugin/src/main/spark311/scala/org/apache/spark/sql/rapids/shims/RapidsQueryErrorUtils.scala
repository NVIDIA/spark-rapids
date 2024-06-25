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
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.rapids.shims

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.ErrorMsg

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.rapids.execution.RapidsAnalysisException
import org.apache.spark.sql.types.StructType

trait RapidsQueryErrorUtils {

  def outputPathAlreadyExistsError(qualifiedOutputPath: Path): Throwable = {
    new AnalysisException(s"path $qualifiedOutputPath already exists.")
  }

  def createTableAsSelectWithNonEmptyDirectoryError(tablePath: String, conf: String): Throwable = {
    new AnalysisException(s"CREATE-TABLE-AS-SELECT cannot create table with location to a " +
      s"non-empty directory $tablePath. To allow overwriting the existing non-empty directory, " +
      s"set '$conf' to true.")
  }

  def cannotResolveAttributeError(name: String, outputStr: String): Throwable = {
    new AnalysisException(s"Unable to resolve $name given [$outputStr]")
  }

  def partitionColumnNotSpecifiedError(format: String, partitionColumn: String): Throwable = {
    new AnalysisException(s"Failed to resolve the schema for $format for the partition column: " +
      s"$partitionColumn. It must be specified manually.")
  }

  def dataSchemaNotSpecifiedError(format: String): Throwable = {
    new AnalysisException(s"Unable to infer schema for $format. It must be specified manually.")
  }

  def schemaNotSpecifiedForSchemaRelationProviderError(className: String): Throwable = {
    new AnalysisException(s"A schema needs to be specified when using $className.")
  }

  def userSpecifiedSchemaMismatchActualSchemaError(
    schema: StructType,
    actualSchema: StructType): Throwable = {
    new AnalysisException("The user-specified schema doesn't match the actual schema: " +
      s"user-specified: ${schema.toDDL}, actual: ${actualSchema.toDDL}. If " +
      "you're using DataFrameReader.schema API or creating a table, please do not " +
      "specify the schema. Or if you're scanning an existed table, please drop " +
      "it and re-create it.")
  }

  def dataSchemaNotSpecifiedError(format: String, fileCatalog: String): Throwable = {
    new AnalysisException(s"Unable to infer schema for $format at $fileCatalog. " +
      "It must be specified manually")
  }

  def invalidDataSourceError(className: String): Throwable = {
    new AnalysisException(s"$className is not a valid Spark SQL Data Source.")
  }

  def orcNotUsedWithHiveEnabledError(): Throwable = {
    new AnalysisException(
      s"Hive built-in ORC data source must be used with Hive support enabled. " +
        s"Please use the native ORC data source by setting 'spark.sql.orc.impl' to 'native'.")
  }

  def failedToFindAvroDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      s"Failed to find data source: $provider. Avro is built-in but external data " +
        "source module since Spark 2.4. Please deploy the application as per " +
        "the deployment section of \"Apache Avro Data Source Guide\".")
  }

  def failedToFindKafkaDataSourceError(provider: String): Throwable = {
    new AnalysisException(
      s"Failed to find data source: $provider. Please deploy the application as " +
        "per the deployment section of " +
        "\"Structured Streaming + Kafka Integration Guide\".")
  }

  def findMultipleDataSourceError(provider: String, sourceNames: Seq[String]): Throwable = {
    new AnalysisException(
      s"Multiple sources found for $provider " +
        s"(${sourceNames.mkString(", ")}), please specify the fully qualified class name.")
  }

  def dataPathNotExistError(path: String): Throwable = {
    new AnalysisException(s"Path does not exist: $path")
  }

  def dynamicPartitionParentError: Throwable = {
    throw new RapidsAnalysisException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
  }

  def tableOrViewAlreadyExistsError(tableName: String): Throwable = {
    new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
  }

  def parquetTypeUnsupportedYetError(parquetType: String): Throwable = {
    new AnalysisException(s"Parquet type not yet supported: $parquetType.")
  }

  def illegalParquetTypeError(parquetType: String): Throwable = {
    new AnalysisException(s"Illegal Parquet type: $parquetType.")
  }
}