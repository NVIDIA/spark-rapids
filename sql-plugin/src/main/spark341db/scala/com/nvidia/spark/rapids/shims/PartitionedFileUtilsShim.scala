/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}

object PartitionedFileUtilsShim {
  // Wrapper for case class constructor so Java code can access
  // the default values across Spark versions.
  def newPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      start: Long,
      length: Long): PartitionedFile = {
    PartitionedFile(partitionValues, SparkPath.fromPathString(filePath), start, length)
  }

  def withNewLocations(pf: PartitionedFile, locations: Seq[String]): PartitionedFile = {
    pf.copy(locations = locations)
  }

  // In Spark 4.0, PartitionedFileUtil.splitFiles lost its `sparkSession` parameter.
  // This pre-Spark-4.0 shim keeps the `sparkSession` parameter.
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatusWithMetadata,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(sparkSession, file, isSplitable, maxSplitBytes, partitionValues)
  }
}
