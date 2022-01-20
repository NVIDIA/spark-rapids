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
