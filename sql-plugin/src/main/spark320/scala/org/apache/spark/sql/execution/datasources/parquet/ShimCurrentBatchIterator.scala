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
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.util

import com.nvidia.spark.CurrentBatchIterator
import com.nvidia.spark.rapids.ParquetCachedBatch
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.Type

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.parquet.rapids.shims.ShimVectorizedColumnReader
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy

object ParquetVectorizedReader {
  lazy val readBatchMethod = {
    val method = classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
        classOf[WritableColumnVector])
    method.setAccessible(true)
    method
  }
}

/**
 * This class takes a lot of the logic from
 * org.apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java
 */
class ShimCurrentBatchIterator(
    parquetCachedBatch: ParquetCachedBatch,
    conf: SQLConf,
    selectedAttributes: Seq[Attribute],
    options: ParquetReadOptions,
    hadoopConf: Configuration) extends CurrentBatchIterator(
      parquetCachedBatch,
      conf,
      selectedAttributes,
      options,
      hadoopConf) {

  var columnReaders: Array[VectorizedColumnReader] = _
  val missingColumns = new Array[Boolean](reqParquetSchemaInCacheOrder.getFieldCount)
  val typesInCache: util.List[Type] = reqParquetSchemaInCacheOrder.asGroupType.getFields
  val columnsInCache: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
  val columnsRequested: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns

  // initialize missingColumns to cover the case where requested column isn't present in the
  // cache, which should never happen but just in case it does
  val paths: util.List[Array[String]] = reqParquetSchemaInCacheOrder.getPaths

  for (i <- 0 until reqParquetSchemaInCacheOrder.getFieldCount) {
    val t = reqParquetSchemaInCacheOrder.getFields.get(i)
    if (!t.isPrimitive || t.isRepetition(Type.Repetition.REPEATED)) {
      throw new UnsupportedOperationException("Complex types not supported.")
    }
    val colPath = paths.get(i)
    if (inMemCacheParquetSchema.containsPath(colPath)) {
      val fd = inMemCacheParquetSchema.getColumnDescription(colPath)
      if (!(fd == columnsRequested.get(i))) {
        throw new UnsupportedOperationException("Schema evolution not supported.")
      }
      missingColumns(i) = false
    } else {
      if (columnsRequested.get(i).getMaxDefinitionLevel == 0) {
        // Column is missing in data but the required data is non-nullable.
        // This file is invalid.
        throw new IOException(s"Required column is missing in data file: ${colPath.toList}")
      }
      missingColumns(i) = true
      vectors(i).putNulls(0, capacity)
      vectors(i).setIsConstant()
    }
  }

  @throws[IOException]
  def checkEndOfRowGroup(): Unit = {
    if (rowsReturned != totalCountLoadedSoFar) return
    val pages = parquetFileReader.readNextRowGroup
    if (pages == null) {
      throw new IOException("expecting more rows but reached last" +
          " block. Read " + rowsReturned + " out of " + totalRowCount)
    }
    columnReaders = new Array[VectorizedColumnReader](columnsRequested.size)
    for (i <- 0 until columnsRequested.size) {
      if (!missingColumns(i)) {
        columnReaders(i) =
          new ShimVectorizedColumnReader(
            i,
            columnsInCache,
            typesInCache,
            pages,
            convertTz = null,
            LegacyBehaviorPolicy.CORRECTED.toString,
            LegacyBehaviorPolicy.EXCEPTION.toString,
            int96CDPHive3Compatibility = false,
            writerVersion)
      }
    }
    totalCountLoadedSoFar += pages.getRowCount
  }

  /**
   * Read the next RowGroup and read each column and return the columnarBatch
   */
  def nextBatch: Boolean = {
    for (vector <- vectors) {
      vector.reset()
    }
    columnarBatch.setNumRows(0)
    if (rowsReturned >= totalRowCount) return false
    checkEndOfRowGroup()
    val num = Math.min(capacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
    for (i <- columnReaders.indices) {
      if (columnReaders(i) != null) {
        ParquetVectorizedReader.readBatchMethod
            .invoke(columnReaders(i), num.asInstanceOf[AnyRef],
          vectors(cacheSchemaToReqSchemaMap(i)).asInstanceOf[AnyRef])
      }
    }
    rowsReturned += num
    columnarBatch.setNumRows(num)
    true
  }

  override def hasNext: Boolean = rowsReturned < totalRowCount

  onTaskCompletion {
    close()
  }

}
