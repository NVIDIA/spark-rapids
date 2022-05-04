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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.lang.reflect.Method

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{ByteArrayInputFile, ParquetCachedBatch}
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.{ParquetReadOptions, VersionParser}
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{GroupType, Type}

import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized
import org.apache.spark.sql.vectorized.ColumnarBatch

object ParquetVectorizedReader {
  /**
   * We are getting this method using reflection because its a package-private
   */
  private var readBatchMethod: Method = null

  def getReadBatchMethod(): Method = {
    if (readBatchMethod == null) {
      readBatchMethod =
        classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
          classOf[WritableColumnVector],
          classOf[WritableColumnVector],
          classOf[WritableColumnVector])
      readBatchMethod.setAccessible(true)
    }
    readBatchMethod
  }
}

/**
 * This class takes a lot of the logic from
 * org.apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java
 */
class ShimCurrentBatchIterator(
    val parquetCachedBatch: ParquetCachedBatch,
    conf: SQLConf,
    selectedAttributes: Seq[Attribute],
    options: ParquetReadOptions,
    hadoopConf: Configuration) extends Iterator[ColumnarBatch] with AutoCloseable {

  var reqParquetSchemaInCacheOrder: org.apache.parquet.schema.MessageType = null
  var inMemCacheParquetSchema: org.apache.parquet.schema.MessageType = null
  val capacity = conf.parquetVectorizedReaderBatchSize
  var parquetColumnVectors: Array[ParquetColumnVector] = _
  var vectors: Array[OffHeapColumnVector] =
    OffHeapColumnVector.allocateColumns(capacity, selectedAttributes.toStructType)
  val columnarBatch = new ColumnarBatch(vectors.asInstanceOf[Array[vectorized.ColumnVector]])
  var rowsReturned: Long = 0L
  var numBatched = 0
  var batchIdx = 0
  var totalCountLoadedSoFar: Long = 0
  val parquetFileReader = {
    ParquetFileReader.open(new ByteArrayInputFile(parquetCachedBatch.buffer), options)
  }
  val writerVersion: ParsedVersion = try {
    VersionParser.parse(parquetFileReader.getFileMetaData.getCreatedBy)
  } catch {
    case _: Exception =>
      // If any problems occur trying to parse the writer version, fallback to sequential reads
      // if the column is a delta byte array encoding (due to PARQUET-246).
      null
  }
  val (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
  columnsInCache, typesInCache) = {
    val parquetToSparkSchemaConverter = new ParquetToSparkSchemaConverter(hadoopConf)
    // we are getting parquet schema and then converting it to catalyst schema
    // because catalyst schema that we get from Spark doesn't have the exact schema expected
    // by the columnar parquet reader
    inMemCacheParquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema
    val inMemCacheSparkSchema = parquetToSparkSchemaConverter.convert(inMemCacheParquetSchema)

    val totalRowCount = parquetFileReader.getRowGroups.asScala.map(_.getRowCount).sum
    val inMemReqSparkSchema = StructType(selectedAttributes.toStructType.map { field =>
      inMemCacheSparkSchema.fields(inMemCacheSparkSchema.fieldIndex(field.name))
    })
    val reqSparkSchemaInCacheOrder = StructType(inMemCacheSparkSchema.filter(f =>
      inMemReqSparkSchema.fields.exists(f0 => f0.name.equals(f.name))))

    // There could be a case especially in a distributed environment where the requestedSchema
    // and cacheSchema are not in the same order. We need to create a map so we can guarantee
    // that we writing to the correct columnVector
    val cacheSchemaToReqSchemaMap: Map[Int, Int] =
    reqSparkSchemaInCacheOrder.indices.map { index =>
      index -> inMemReqSparkSchema.fields.indexOf(reqSparkSchemaInCacheOrder.fields(index))
    }.toMap

    reqParquetSchemaInCacheOrder = new org.apache.parquet.schema.MessageType(
      inMemCacheParquetSchema.getName(), reqSparkSchemaInCacheOrder.fields.map { f =>
        inMemCacheParquetSchema.getFields().get(inMemCacheParquetSchema.getFieldIndex(f.name))
      }:_*)

    val columnsRequested: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
    // reset spark schema calculated from parquet schema
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, inMemReqSparkSchema.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, inMemReqSparkSchema.json)

    val columnsInCache: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
    val typesInCache: util.List[Type] = reqParquetSchemaInCacheOrder.asGroupType.getFields
    val missingColumns: util.Set[ParquetColumn] = new util.HashSet[ParquetColumn]()

    val config = new Configuration
    config.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    config.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
    config.setBoolean(SQLConf.CASE_SENSITIVE.key, false)
    val parquetColumn = new ParquetToSparkSchemaConverter(config)
        .convertParquetColumn(reqParquetSchemaInCacheOrder, Option.empty)

    // initialize missingColumns to cover the case where requested column isn't present in the
    // cache, which should never happen but just in case it does
    for (column <- parquetColumn.children) {
      checkColumn(column)
    }

    val sparkSchema = parquetColumn.sparkType.asInstanceOf[StructType]
    parquetColumnVectors = (for (i <- 0 until sparkSchema.fields.length) yield {
      new ParquetColumnVector(parquetColumn.children.apply(i),
        vectors(i), capacity, MemoryMode.OFF_HEAP, missingColumns)
    }).toArray

    (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
        columnsInCache, typesInCache)
  }

  private def containsPath(parquetType: Type, path: Array[String]): Boolean =
    containsPath(parquetType, path, 0)

  private def containsPath(parquetType: Type, path: Array[String], depth: Int): Boolean = {
    if (path.length == depth) return true
    if (parquetType.isInstanceOf[GroupType]) {
      val fieldName = path(depth)
      val parquetGroupType = parquetType.asInstanceOf[GroupType]
      if (parquetGroupType.containsField(fieldName)) {
        return containsPath(parquetGroupType.getType(fieldName), path, depth + 1)
      }
    }
    false
  }

  private def checkColumn(column: ParquetColumn): Unit = {
    val paths = column.path.toArray
    if (containsPath(inMemCacheParquetSchema, paths)) {
      if (column.isPrimitive) {
        val desc = column.descriptor.get
        val fd = inMemCacheParquetSchema.getColumnDescription(desc.getPath)
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Complex types not supported.")
        }
      } else {
        for (childColumn <- column.children) {
          checkColumn(childColumn)
        }
      }
    } else {
      if (column.required) {
        if (column.required) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " + paths)
        }
        missingColumns.add(column);
      }
    }

  }

  @throws[IOException]
  private def initColumnReader(pages: PageReadStore, cv: ParquetColumnVector): Unit = {
    if (!missingColumns.contains(cv.getColumn)) {
      if (cv.getColumn.isPrimitive) {
        val column = cv.getColumn
        val reader = new VectorizedColumnReader(
          column.descriptor.get,
          column.required,
          pages,
          null,
          LegacyBehaviorPolicy.CORRECTED.toString,
          LegacyBehaviorPolicy.EXCEPTION.toString,
          LegacyBehaviorPolicy.EXCEPTION.toString,
          null,
          writerVersion)
        cv.setColumnReader(reader)
      }
      else { // Not in missing columns and is a complex type: this must be a struct
        for (childCv <- cv.getChildren.asScala) {
          initColumnReader(pages, childCv)
        }
      }
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
    for (cv <- parquetColumnVectors) {
      initColumnReader(pages, cv)
    }
    totalCountLoadedSoFar += pages.getRowCount
  }

  /**
   * Read the next RowGroup and read each column and return the columnarBatch
   */
  def nextBatch: Boolean = {
    for (vector <- parquetColumnVectors) {
      vector.reset()
    }
    columnarBatch.setNumRows(0)
    if (rowsReturned >= totalRowCount) return false
    checkEndOfRowGroup()

    val num = Math.min(capacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
    for (cv <- parquetColumnVectors){
      for (leafCv <- cv.getLeaves.asScala) {
        val columnReader = leafCv.getColumnReader
        if (columnReader != null) {
          ParquetVectorizedReader.getReadBatchMethod.invoke(
            columnReader,
            num.asInstanceOf[AnyRef],
            leafCv.getValueVector.asInstanceOf[AnyRef],
            leafCv.getRepetitionLevelVector.asInstanceOf[AnyRef],
            leafCv.getDefinitionLevelVector.asInstanceOf[AnyRef])
        }
      }
      cv.assemble()
    }
    rowsReturned += num
    columnarBatch.setNumRows(num)
    numBatched = num
    batchIdx = 0
    true
  }

  override def hasNext: Boolean = rowsReturned < totalRowCount

  override def next(): ColumnarBatch = {
    if (nextBatch) {
      // FYI, A very IMPORTANT thing to note is that we are returning the columnar batch
      // as-is i.e. this batch has NullTypes saved as IntegerTypes with null values. The
      // way Spark optimizes the read of NullTypes makes this work without having to rip out
      // the IntegerType column to be replaced by a NullType column. This could change in
      // future and will affect this code.
      columnarBatch
    } else {
      throw new NoSuchElementException("no elements found")
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((_: TaskContext) => {
    close()
  })

  override def close(): Unit = {
    parquetFileReader.close()
  }
}