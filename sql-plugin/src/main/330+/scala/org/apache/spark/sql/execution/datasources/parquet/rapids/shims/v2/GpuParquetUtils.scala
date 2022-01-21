/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution.datasources.parquet.rapids.shims.v2

import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.StructType

object GpuParquetUtils {
  def createAggInternalRowFromFooter(
    footer: ParquetMetadata,
    filePath: String,
    dataSchema: StructType,
    partitionSchema: StructType,
    aggregation: Aggregation,
    aggSchema: StructType,
    partitionValues: InternalRow,
    datetimeRebaseSpec: RebaseSpec): InternalRow = {
    ParquetUtils.createAggInternalRowFromFooter(
      footer,
      filePath,
      dataSchema,
      partitionSchema,
      aggregation,
      aggSchema,
      partitionValues,
      datetimeRebaseSpec
  )}
}
