/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import java.io.OutputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import ai.rapids.cudf.{HostMemoryBuffer, ParquetOptions, Table}
import org.apache.commons.io.output.{CountingOutputStream, NullOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.filter2.compat.{FilterCompat, RowGroupFilter}
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ColumnPath, FileMetaData, ParquetMetadata}
import org.apache.parquet.schema.MessageType

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, ParquetReadSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuParquetScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap) extends ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema,
                                                           readDataSchema, readPartitionSchema, pushedFilters,options) {

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new GpuSerializableConfiguration(hadoopConf))
    GpuParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

  // TODO need a common base for these...
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    other.isInstanceOf[GpuParquetScan]
  }

  override def hashCode(): Int = super.hashCode()
}

object GpuParquetScan {
  def assertCanSupport(scan: ParquetScan): Unit = {
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    for (field <- schema) {
      if (!isSupportedType(field.dataType)) {
        throw new CannotReplaceException(s"GpuParquetScan does not support fields of type ${field.dataType}")
      }
    }

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    if (scan.sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      throw new CannotReplaceException("GpuParquetScan does not support int96 timestamp conversion")
    }
  }

  private def isSupportedType(dataType: DataType): Boolean = {
    var supported = true
    try {
      GpuColumnVector.getRapidsType(dataType)
    } catch {
      case _: IllegalArgumentException => supported = false
    }
    supported
  }
}

case class GpuParquetPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[GpuSerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter]) extends FilePartitionReaderFactory {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val debugDumpPrefix = sqlConf.getConfString(
      ParquetPartitionReader.DUMP_PATH_PREFIX_CONF, null)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val reader = buildBaseColumnarParquetReader(partitionedFile)
    ColumnarPartitionReaderWithPartitionValues.newReader(partitionedFile, reader, partitionSchema)
  }

  private def buildBaseColumnarParquetReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val filePath = new Path(new URI(file.filePath))
    //noinspection ScalaDeprecation
    val footer = ParquetFileReader.readFooter(conf, filePath,
        ParquetMetadataConverter.range(file.start, file.start + file.length))
    val fileSchema = footer.getFileMetaData.getSchema
    val pushedFilters = if (enableParquetFilterPushDown) {
      val parquetFilters = new ParquetFilters(fileSchema, pushDownDate, pushDownTimestamp,
          pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
      filters.flatMap(parquetFilters.createFilter).reduceOption(FilterApi.and)
    } else {
      None
    }

    val blocks = if (pushedFilters.isDefined) {
      //noinspection ScalaDeprecation
      RowGroupFilter.filterRowGroups(FilterCompat.get(pushedFilters.get), footer.getBlocks, fileSchema)
    } else {
      footer.getBlocks
    }

    val clippedSchema = ParquetReadSupport.clipParquetSchema(fileSchema, readDataSchema,
        isCaseSensitive)
    val columnPaths = clippedSchema.getPaths.asScala.map(x => ColumnPath.get(x:_*))
    val clippedBlocks = ParquetPartitionReader.clipBlocks(columnPaths, blocks.asScala)
    new ParquetPartitionReader(conf, filePath, clippedBlocks, clippedSchema, readDataSchema,
        debugDumpPrefix)
  }
}

/**
  * A PartitionReader that reads a Parquet file split on the GPU.
  *
  * Efficiently reading a Parquet split on the GPU requires re-constructing the Parquet file
  * in memory that contains just the column chunks that are needed. This avoids sending
  * unnecessary data to the GPU and saves GPU memory.
  *
  * @param conf the Hadoop configuration
  * @param filePath the path to the Parquet file
  * @param clippedBlocks the block metadata from the original Parquet file that has been clipped
  *                      to only contain the column chunks to be read
  * @param clippedParquetSchema the Parquet schema from the original Parquet file that has been
  *                             clipped to contain only the columns to be read
  * @param readDataSchema the Spark schema describing what will be read
  * @param debugDumpPrefix a path prefix to use for dumping the fabricated Parquet data or null
  */
class ParquetPartitionReader(
    conf: Configuration,
    filePath: Path,
    clippedBlocks: Seq[BlockMetaData],
    clippedParquetSchema: MessageType,
    readDataSchema: StructType,
    debugDumpPrefix: String) extends PartitionReader[ColumnarBatch] with Logging {
  private var isExhausted: Boolean = false
  private var batch: Option[ColumnarBatch] = None

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isExhausted) {
      isExhausted = true
      val table = readToTable()
      try {
        batch = table.map(GpuColumnVector.from)
      } finally {
        table.foreach(_.close())
      }
    }
    batch.isDefined
  }

  override def get(): ColumnarBatch = batch.getOrElse(throw new NoSuchElementException)

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    isExhausted = true
  }

  private def readPartFile(): (HostMemoryBuffer, Long) = {
    val in = filePath.getFileSystem(conf).open(filePath)
    try {
      var succeeded = false
      val hmb = HostMemoryBuffer.allocate(calculateParquetOutputSize())
      try {
        val out = new HostMemoryBufferOutputStream(hmb)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        val outputBlocks = copyClippedBlocksData(in, out)
        val footerPos = out.getPos
        writeFooter(out, outputBlocks)
        BytesUtils.writeIntLittleEndian(out, (out.getPos - footerPos).toInt)
        out.write(ParquetPartitionReader.PARQUET_MAGIC)
        succeeded = true
        (hmb, out.getPos)
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
    } finally {
      in.close()
    }
  }

  private def calculateParquetOutputSize(): Long = {
    // start with the size of Parquet magic (at start+end) and footer length values
    var size: Long = 4 + 4 + 4

    // add in the size of the row group data
    size += clippedBlocks.map(_.getTotalByteSize).sum

    // Calculate size of the footer metadata.
    // This uses the column metadata from the original file, but that should
    // always be at least as big as the updated metadata in the output.
    val out = new CountingOutputStream(new NullOutputStream)
    writeFooter(out, clippedBlocks)
    size + out.getByteCount
  }

  private def writeFooter(out: OutputStream, blocks: Seq[BlockMetaData]): Unit = {
    val fileMeta = new FileMetaData(clippedParquetSchema, Collections.emptyMap[String, String],
      ParquetPartitionReader.PARQUET_CREATOR)
    val metadataConverter = new ParquetMetadataConverter
    val footer = new ParquetMetadata(fileMeta, blocks.asJava)
    val meta = metadataConverter.toParquetMetadata(ParquetPartitionReader.PARQUET_VERSION, footer)
    org.apache.parquet.format.Util.writeFileMetaData(meta, out)
  }

  private def copyColumnData(
      column: ColumnChunkMetaData,
      in: FSDataInputStream,
      out: OutputStream,
      copyBuffer: Array[Byte]): Unit = {
    if (in.getPos != column.getStartingPos) {
      in.seek(column.getStartingPos)
    }
    var bytesLeft = column.getTotalSize
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      in.readFully(copyBuffer, 0, readLength)
      out.write(copyBuffer, 0, readLength)
      bytesLeft -= readLength
    }
  }

  /**
    * Copies the data corresponding to the clipped blocks in the original file and compute the
    * block metadata for the output. The output blocks will contain the same column chunk
    * metadata but with the file offsets updated to reflect the new position of the column data
    * as written to the output.
    *
    * @param in the input stream for the original Parquet file
    * @param out the output stream to receive the data
    * @return updated block metadata corresponding to the output
    */
  private def copyClippedBlocksData(
      in: FSDataInputStream,
      out: HostMemoryBufferOutputStream): Seq[BlockMetaData] = {
    val copyBuffer = new Array[Byte](128 * 1024)
    val outputBlocks = new ArrayBuffer[BlockMetaData](clippedBlocks.length)
    for (block <- clippedBlocks) {
      val columns = block.getColumns.asScala
      val outputColumns = new ArrayBuffer[ColumnChunkMetaData](columns.length)
      for (column <- columns) {
        // update column metadata to reflect new position in the output file
        val offsetAdjustment = out.getPos - column.getStartingPos
        val newDictOffset = if (column.getDictionaryPageOffset > 0) {
          column.getDictionaryPageOffset + offsetAdjustment
        } else {
          0
        }
        //noinspection ScalaDeprecation
        outputColumns += ColumnChunkMetaData.get(
          column.getPath,
          column.getPrimitiveType,
          column.getCodec,
          column.getEncodingStats,
          column.getEncodings,
          column.getStatistics,
          column.getFirstDataPageOffset + offsetAdjustment,
          newDictOffset,
          column.getValueCount,
          column.getTotalSize,
          column.getTotalUncompressedSize)
        copyColumnData(column, in, out, copyBuffer)
      }
      outputBlocks += ParquetPartitionReader.newParquetBlock(block.getRowCount, outputColumns)
    }
    outputBlocks
  }

  private def readToTable(): Option[Table] = {
    if (clippedBlocks.isEmpty) {
      return None
    }

    val (dataBuffer, dataSize) = readPartFile()
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpParquetData(debugDumpPrefix, dataBuffer, dataSize)
        }
        val parseOpts = ParquetOptions.builder().includeColumn(readDataSchema.fieldNames:_*).build()
        val table = Table.readParquet(parseOpts, dataBuffer, 0, dataSize)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $filePath")
        }
        Some(table)
      }
    } finally {
      dataBuffer.close()
    }
  }

  private def dumpParquetData(
      dumpPathPrefix: String,
      hmb: HostMemoryBuffer,
      dataLength: Long): Unit = {
    val (out, path) = ParquetPartitionReader.createTempFile(conf, dumpPathPrefix)
    try {
      logInfo(s"Writing Parquet split data to $path")
      val buffer = new Array[Byte](128 * 1024)
      var pos = 0
      while (pos < dataLength) {
        // downcast is safe because buffer.length is an int
        val readLength = Math.min(dataLength - pos, buffer.length).toInt
        hmb.getBytes(buffer, 0, pos, readLength)
        out.write(buffer, 0, readLength)
        pos += readLength
      }
    } finally {
      out.close()
    }
  }
}

object ParquetPartitionReader {
  private val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private val PARQUET_VERSION = 1
  private[spark] val DUMP_PATH_PREFIX_CONF = "spark.rapids.sql.parquet.debug-dump-prefix"

  /**
    * Build a new BlockMetaData
    *
    * @param rowCount the number of rows in this block
    * @param columns the new column chunks to reference in the new BlockMetaData
    * @return the new BlockMetaData
    */
  private def newParquetBlock(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    for (column <- columns) {
      block.addColumn(column)
      totalSize += column.getTotalSize
    }
    block.setTotalByteSize(totalSize)

    block
  }

  /**
    * Trim block metadata to contain only the column chunks that occur in the specified columns.
    * The column chunks that are returned are preserved verbatim
    * (i.e.: file offsets remain unchanged).
    *
    * @param columnPaths the paths of columns to preserve
    * @param blocks the block metadata from the original Parquet file
    * @return the updated block metadata with undesired column chunks removed
    */
  private[spark] def clipBlocks(columnPaths: Seq[ColumnPath], blocks: Seq[BlockMetaData]): Seq[BlockMetaData] = {
    val pathSet = columnPaths.toSet
    blocks.map(oldBlock => {
      //noinspection ScalaDeprecation
      val newColumns = oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath))
      ParquetPartitionReader.newParquetBlock(oldBlock.getRowCount, newColumns)
    })
  }

  private def createTempFile(
      conf: Configuration,
      pathPrefix: String): (FSDataOutputStream, Path) = {
    val fs = new Path(pathPrefix).getFileSystem(conf)
    val rnd = new Random
    var out: FSDataOutputStream = null
    var path: Path = null
    var succeeded = false
    while (!succeeded) {
      path = new Path(pathPrefix + rnd.nextInt(Integer.MAX_VALUE) + ".parquet")
      if (!fs.exists(path)) {
        scala.util.control.Exception.ignoring(classOf[FileAlreadyExistsException]) {
          out = fs.create(path, false)
          succeeded = true
        }
      }
    }
    (out, path)
  }
}

/**
  * An implementation of Parquet's PositionOutputStream that writes to a HostMemoryBuffer.
  *
  * NOTE: Closing this output stream does NOT close the buffer!
  *
  * @param buffer the buffer to receive written data
  */
private class HostMemoryBufferOutputStream(buffer: HostMemoryBuffer) extends OutputStream {
  private var pos: Long = 0

  override def write(i: Int): Unit = {
    buffer.setByte(pos, i.toByte)
    pos += 1
  }

  override def write(bytes: Array[Byte]): Unit = {
    buffer.setBytes(pos, bytes, 0, bytes.length)
    pos += bytes.length
  }

  override def write(bytes: Array[Byte], offset: Int, len: Int): Unit = {
    buffer.setBytes(pos, bytes, offset, len)
    pos += len
  }

  def getPos: Long = pos
}
