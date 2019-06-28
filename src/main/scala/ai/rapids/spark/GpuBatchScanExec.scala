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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import ai.rapids.cudf.{HostMemoryBuffer, Table}
import ai.rapids.cudf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FilePartitionReaderFactory}
import org.apache.spark.sql.sources.v2.reader.{PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuSerializableConfiguration(@transient var value: Configuration)
  extends Serializable with Logging {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends BatchScanExec(output, scan) with GpuExec {

}

object GpuCSVScan {
  def assertCanSupport(scan: CSVScan) : Unit = {
    if (!scan.readPartitionSchema.isEmpty) {
      // TODO would be great to support this
      throw new CannotReplaceException(s"GpuCSVScan does not support read partition fields")
    }
    val options = scan.options
    val sparkSession = scan.sparkSession
    val parsedOptions: CSVOptions = new CSVOptions(
      options.asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    if (parsedOptions.delimiter > 128) {
      throw new CannotReplaceException(s"GpuCSVScan does not support non-ASCII deliminators")
    }

    if (parsedOptions.quote > 128) {
      throw new CannotReplaceException(s"GpuCSVScan does not support non-ASCII quote chars")
    }

    if (parsedOptions.comment > 128) {
      throw new CannotReplaceException(s"GpuCSVScan does not support non-ASCII comment chars")
    }

    if (parsedOptions.inferSchemaFlag) {
      // TODO need to test this because we inherent from something that might do it.
      throw new CannotReplaceException(s"GpuCSVScan does not support schema inference")
    }

    if (parsedOptions.escape != '\\') {
      // TODO need to fix this
      throw new CannotReplaceException(s"GpuCSVScan does not support modified escape chars")
    }

    // TODO charToEscapeQuoteEscaping???


    if (StandardCharsets.UTF_8.name() != parsedOptions.charset &&
      StandardCharsets.US_ASCII.name() != parsedOptions.charset) {
      throw new CannotReplaceException(s"GpuCSVScan only supports UTF8 encoded data")
    }

    if (parsedOptions.ignoreLeadingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      throw new CannotReplaceException(s"GpuCSVScan does not support ignoring leading white space")
    }

    if (parsedOptions.ignoreTrailingWhiteSpaceInRead) {
      // TODO need to fix this (or at least verify that it is doing the right thing)
      throw new CannotReplaceException(s"GpuCSVScan does not support ignoring trailing white space")
    }

    if (parsedOptions.multiLine) {
      // TODO should we support this
      throw new CannotReplaceException(s"GpuCSVScan does not support multi-line")
    }

    if (parsedOptions.lineSeparator.getOrElse("\n") != "\n") {
      // TODO should we support this
      throw new CannotReplaceException("GpuCSVScan only supports \"\\n\" as a line separator")
    }

    // TODO parsedOptions.parseMode
    // TODO parsedOptions.columnNameOfCorruptRecord
    // TODO parsedOptions.nanValue This is here by default so we need to be able to support it
    // TODO parsedOptions.positiveInf This is here by default so we need to be able to support it
    // TODO parsedOptions.negativeInf This is here by default so we need to be able to support it
    // TODO parsedOptions.zoneId This is here by default so we need to be able to support it
    // TODO parsedOptions.local This is here by default so we need to be able to support it
    // TODO parsedOptions.dateFormat This is here by default so we need to be able to support it
    // TODO parsedOptions.timestampFormat This is here by default so we need to be able to support it
    // TODO parsedOptions.emptyValueInRead
    // TODO ColumnPruning????
    // TODO sparkSession.sessionState.conf.caseSensitiveAnalysis on the column names
  }
}

class GpuCSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType, // original schema passed in by the user (all the data)
    readDataSchema: StructType, // schema that is for the data being read in (including dropped columns)
    readPartitionSchema: StructType, // schema for the parts that come from the file path
    options: CaseInsensitiveStringMap) extends
  CSVScan(sparkSession, fileIndex, dataSchema, readDataSchema, readPartitionSchema, options) {

  // TODO eventually we need to support splits...
  override def isSplitable(path: Path): Boolean = false

  lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new GpuSerializableConfiguration(hadoopConf))

    return new GpuCSVPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, parsedOptions)
  }

  // TODO need a common base for these...
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuCSVScan]
  }

  override def hashCode(): Int = super.hashCode()
}

case class GpuCSVPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[GpuSerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType, // TODO need to filter these out, or support pulling them in. These are values from the file name/path itself
    parsedOptions: CSVOptions) extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    new SingleTablePartitionReader(conf, partFile, dataSchema, readDataSchema, parsedOptions)
  }
}


class SingleTablePartitionReader(conf: Configuration, partFile: PartitionedFile,
    dataSchema: StructType, readDataSchema: StructType, parsedOptions: CSVOptions)
  extends PartitionReader[ColumnarBatch] {
  var moreToRead = true
  override def next(): Boolean = moreToRead

  def buildCsvOptions(parsedOptions: CSVOptions, schema: StructType): cudf.CSVOptions = {
    val builder = cudf.CSVOptions.builder()
    builder.withDelim(parsedOptions.delimiter)
    // TODO if we do partitioning we need to update this so it is just for the first partition.
    builder.hasHeader(parsedOptions.headerFlag)
    // TODO parsedOptions.parseMode
    builder.withQuote(parsedOptions.quote)
    builder.withComment(parsedOptions.comment)
    builder.withNullValue(parsedOptions.nullValue)
    builder.includeColumn(schema.fields.map(_.name): _*)
    builder.build
  }

  /**
   * Grows a host buffer, returning a new buffer and closing the original
   * after copying the data into the new buffer.
   * @param original the original host memory buffer
   */
  private def growHostBuffer(original: HostMemoryBuffer, needed: Long): HostMemoryBuffer = {
    val newSize = Math.max(original.getLength * 2, needed)
    val result = HostMemoryBuffer.allocate(newSize)
    try {
      result.copyFromHostBuffer(0, original, 0, original.getLength)
      original.close()
    } catch {
      case e: Throwable =>
        result.close()
        throw e
    }
    result
  }

  private def readPartFileFully(): (HostMemoryBuffer, Long) = {
    val rawPath = new Path(partFile.filePath)
    val fs = rawPath.getFileSystem(conf)
    val path = fs.makeQualified(rawPath)
    val fileSize = fs.getFileStatus(path).getLen
    var succeeded = false
    var hmb = HostMemoryBuffer.allocate(fileSize)
    var totalBytesRead: Long = 0L
    try {
      val buffer = new Array[Byte](1024 * 16)
      val codecFactory = new CompressionCodecFactory(conf)
      val codec = codecFactory.getCodec(path)
      val rawInput = fs.open(path)
      val in = if (codec != null) codec.createInputStream(rawInput) else rawInput
      try {
        var numBytes = in.read(buffer)
        while (numBytes >= 0) {
          if (totalBytesRead + numBytes > hmb.getLength) {
            hmb = growHostBuffer(hmb, totalBytesRead + numBytes)
          }
          hmb.setBytes(totalBytesRead, buffer, 0, numBytes)
          totalBytesRead += numBytes
          numBytes = in.read(buffer)
        }
      } finally {
        in.close()
      }
      succeeded = true
    } finally {
      if (!succeeded) {
        hmb.close()
      }
    }
    (hmb, totalBytesRead)
  }

  private def readToTable() : Table = {
    val (dataBuffer, dataSize) = readPartFileFully()
    try {
      val csvSchemaBuilder = ai.rapids.cudf.Schema.builder
      dataSchema.foreach(f => csvSchemaBuilder.column(GpuColumnVector.getRapidsType(f.dataType), f.name))
      val table = Table.readCSV(csvSchemaBuilder.build(), buildCsvOptions(parsedOptions, readDataSchema),
        dataBuffer, dataSize)
      val numColumns = table.getNumberOfColumns
      if (readDataSchema.length != numColumns) {
        table.close()
        throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
          s"but only read ${table.getNumberOfColumns} from $partFile")
      }
      table
    } finally {
      dataBuffer.close()
    }
  }

  var batch: ColumnarBatch = null

  override def get(): ColumnarBatch = {
    if (moreToRead) {
      moreToRead = false
      val table = readToTable()
      try {
        batch = GpuColumnVector.from(table)
        batch
      } finally {
        table.close()
      }
    } else {
      throw new IllegalStateException("What is the right thing to throw when we read too much?")
    }
  }

  override def close(): Unit = {
    if (batch != null) {
      batch.close()
    }
  }
}