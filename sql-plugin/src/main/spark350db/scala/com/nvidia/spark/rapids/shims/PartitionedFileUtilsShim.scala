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
{"spark": "350db"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionedFile}

object PartitionedFileUtilsShim extends PartitionedFileUtilsShimBase {

  // In Spark 4.0, PartitionedFileUtil.splitFiles lost its `sparkSession` parameter.
  // This Spark-4.0+ shim ignores the `sparkSession` parameter.
  def splitFiles(sparkSession: SparkSession,
                 file: FileStatusWithMetadata,
                 isSplitable: Boolean,
                 maxSplitBytes: Long,
                 partitionValues: InternalRow): Seq[PartitionedFile] = {
    PartitionedFileUtil.splitFiles(file, isSplitable, maxSplitBytes, partitionValues)
  }

} // object PartitionFileUtilsShim;
