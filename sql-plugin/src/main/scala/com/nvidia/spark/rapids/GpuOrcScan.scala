/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.DataOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util
import java.util.Locale
import java.util.concurrent.{Callable, ThreadPoolExecutor}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.math.max

import ai.rapids.cudf._
import com.google.protobuf.CodedOutputStream
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{CompressionKind, DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl._
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FilePartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.OrcFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

abstract class GpuOrcScanBase(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean)
  extends ScanWithMetrics with Logging {

  def isSplitableBase(path: Path): Boolean = true

  def createReaderFactoryBase(): PartitionReaderFactory = {
    // Unset any serialized search argument setup by Spark's OrcScanBuilder as
    // it will be incompatible due to shading and potential ORC classifier mismatch.
    hadoopConf.unset(OrcConf.KRYO_SARG.getAttribute)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    if (rapidsConf.isOrcPerFileReadEnabled) {
      logInfo("Using the original per file orc reader")
      GpuOrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
    } else {
      GpuOrcMultiFilePartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        queryUsesInputFile)
    }
  }
}

object GpuOrcScanBase {
  def tagSupport(scanMeta: ScanMeta[OrcScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    if (scan.options.getBoolean("mergeSchema", false)) {
      scanMeta.willNotWorkOnGpu("mergeSchema and schema evolution is not supported yet")
    }
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      schema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    if (!meta.conf.isOrcEnabled) {
      meta.willNotWorkOnGpu("ORC input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC} to true")
    }

    if (!meta.conf.isOrcReadEnabled) {
      meta.willNotWorkOnGpu("ORC input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_ORC_READ} to true")
    }

    FileFormatChecks.tag(meta, schema, OrcFormatType, ReadFileOp)

    if (sparkSession.conf
      .getOption("spark.sql.orc.mergeSchema").exists(_.toBoolean)) {
      meta.willNotWorkOnGpu("mergeSchema and schema evolution is not supported yet")
    }
  }
}

/**
 * The multi-file partition reader factory for creating cloud reading for ORC file format.
 *
 * @param sqlConf             the SQLConf
 * @param broadcastedConf     the Hadoop configuration
 * @param dataSchema          schema of the data
 * @param readDataSchema      the Spark schema describing what will be read
 * @param partitionSchema     schema of partitions.
 * @param filters             filters on non-partition columns
 * @param rapidsConf          the Rapids configuration
 * @param metrics             the metrics
 * @param queryUsesInputFile  this is a parameter to easily allow turning it
 *                            off in GpuTransitionOverrides if InputFileName,
 *                            InputFileBlockStart, or InputFileBlockLength are used
 */
case class GpuOrcMultiFilePartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    filters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric],
    queryUsesInputFile: Boolean)
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf) {

  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix
  private val numThreads = rapidsConf.orcMultiThreadReadNumThreads
  private val maxNumFileProcessed = rapidsConf.maxNumOrcFilesParallel
  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, broadcastedConf, filters)

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isOrcCoalesceFileReadEnabled && !queryUsesInputFile

  override val canUseMultiThreadReader: Boolean = rapidsConf.isOrcMultiThreadReadEnabled

  /**
   * Build the PartitionReader for cloud reading
   *
   * @param files files to be read
   * @param conf  configuration
   * @return cloud reading PartitionReader
   */
  override def buildBaseColumnarReaderForCloud(files: Array[PartitionedFile], conf: Configuration):
      PartitionReader[ColumnarBatch] = {
    new MultiFileCloudOrcPartitionReader(conf, files, dataSchema, readDataSchema, partitionSchema,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads, maxNumFileProcessed,
      debugDumpPrefix, filters, filterHandler, metrics)
  }

  /**
   * Build the PartitionReader for coalescing reading
   *
   * @param files files to be read
   * @param conf  the configuration
   * @return coalescing reading PartitionReader
   */
  override def buildBaseColumnarReaderForCoalescing(files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    val clippedStripes = ArrayBuffer[OrcSingleStripeMeta]()
    files.map { file =>
      // TODO, Do we need to check if every file has the same CompressionKind and schema and
      // The requestedMapping?
      val orcPartitionReaderContext = filterHandler.filterStripes(file, dataSchema,
        readDataSchema, partitionSchema)
      clippedStripes ++= orcPartitionReaderContext.blockIterator.map(block =>
        OrcSingleStripeMeta(
          orcPartitionReaderContext.filePath,
          OrcDataStripe(OrcStripeWithMeta(block, orcPartitionReaderContext)),
          file.partitionValues,
          OrcSchemaWrapper(orcPartitionReaderContext.updatedReadSchema),
          false))
    }

    new MultiFileOrcPartitionReader(conf, files, clippedStripes, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics, partitionSchema, numThreads)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "ORC"
}

case class GpuOrcPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    pushedFilters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics : Map[String, GpuMetric]) extends FilePartitionReaderFactory with Arm {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix
  private val maxReadBatchSizeRows: Integer = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes
  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, broadcastedConf, pushedFilters)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
      partitionSchema)
    if (ctx == null) {
      new EmptyPartitionReader[ColumnarBatch]
    } else {
      val reader = new PartitionReaderWithBytesRead(new GpuOrcPartitionReader(conf, partFile, ctx,
        readDataSchema, debugDumpPrefix, maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics))
      ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
    }
  }
}

/**
 * This class describes a stripe that will appear in the ORC output memory file.
 *
 * @param infoBuilder builder for output stripe info that has been populated with
 *                    all fields except those that can only be known when the file
 *                    is being written (e.g.: file offset, compressed footer length)
 * @param footer stripe footer
 * @param inputDataRanges input file ranges (based at file offset 0) of stripe data
 */
case class OrcOutputStripe(
    infoBuilder: OrcProto.StripeInformation.Builder,
    footer: OrcProto.StripeFooter,
    inputDataRanges: DiskRangeList)

/**
 * This class holds fields needed to read and iterate over the OrcFile
 *
 * @param updatedReadSchema read schema mapped to the file's field names
 * @param evolution ORC SchemaEvolution
 * @param dataReader ORC DataReader
 * @param orcReader ORC Input File Reader
 * @param blockIterator An iterator over the ORC output stripes
 */
case class OrcPartitionReaderContext(
    filePath: Path,
    conf: Configuration,
    fileSchema: TypeDescription,
    updatedReadSchema: TypeDescription,
    evolution: SchemaEvolution,
    fileTail: OrcProto.FileTail,
    compressionSize: Int,
    compressionKind: CompressionKind,
    readerOpts: Reader.Options,
    blockIterator: BufferedIterator[OrcOutputStripe],
    requestedMapping: Option[Array[Int]]) {
  private var isClosed = false

  def cleanUp() = {
  }
}

trait OrcCommonFunctions extends OrcCodecWritingHelper {

  protected def copyStripeData(
      ctx: OrcPartitionReaderContext,
      out: WritableByteChannel,
      inputDataRanges: DiskRangeList): Unit = {

    withResource(OrcTools.buildDataReader(ctx)) { dataReader =>
      val bufferChunks = dataReader.readFileData(inputDataRanges, 0, false)
      var current = bufferChunks
      while (current != null) {
        out.write(current.getData)
        if (dataReader.isTrackingDiskRanges && current.isInstanceOf[BufferChunk]) {
          dataReader.releaseBuffer(current.asInstanceOf[BufferChunk].getChunk)
        }
        current = current.next
      }
    }
  }

  /** Get the ORC schema corresponding to the file being constructed for the GPU */
  protected def buildReaderSchema(ctx: OrcPartitionReaderContext): TypeDescription = {
    if (ctx.requestedMapping.isDefined) {
      // filter top-level schema based on requested mapping
      val orcSchema = ctx.fileSchema
      val orcSchemaNames = orcSchema.getFieldNames
      val orcSchemaChildren = orcSchema.getChildren
      val readerSchema = TypeDescription.createStruct()
      ctx.requestedMapping.get.foreach { orcColIdx =>
        val fieldName = orcSchemaNames.get(orcColIdx)
        val fieldType = orcSchemaChildren.get(orcColIdx)
        readerSchema.addField(fieldName, fieldType.clone())
      }
      readerSchema
    } else {
      ctx.evolution.getReaderSchema
    }
  }

  protected def cleanUpOrc(ctx: OrcPartitionReaderContext) = {
    if (ctx != null) {
    }
  }
}

/**
 * A base ORC partition reader which compose of some common methods
 */
trait OrcPartitionReaderBase extends OrcCommonFunctions with Logging with Arm with ScanWithMetrics {

  // The Spark schema describing what will be read
  def readDataSchema: StructType

  def populateCurrentBlockChunk(
      blockIterator: BufferedIterator[OrcOutputStripe],
      maxReadBatchSizeRows: Int,
      maxReadBatchSizeBytes: Long): Seq[OrcOutputStripe] = {
    val currentChunk = new ArrayBuffer[OrcOutputStripe]

    var numRows: Long = 0
    var numBytes: Long = 0
    var numOrcBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (blockIterator.hasNext) {
        val peekedStripe = blockIterator.head
        if (peekedStripe.infoBuilder.getNumberOfRows > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 ||
          numRows + peekedStripe.infoBuilder.getNumberOfRows <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema,
            peekedStripe.infoBuilder.getNumberOfRows)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += blockIterator.next()
            numRows += currentChunk.last.infoBuilder.getNumberOfRows
            numOrcBytes += currentChunk.last.infoBuilder.getDataLength
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Orc. Orc bytes read: $numOrcBytes. " +
      s"Estimated GPU bytes: $numBytes")

    currentChunk
  }

  /**
   * Read the stripes into HostMemoryBuffer.
   *
   * @param ctx     the context to provide some necessary information
   * @param stripes a sequence of Stripe to be read into HostMemeoryBuffer
   * @return HostMemeoryBuffer and its data size
   */
  protected def readPartFile(ctx: OrcPartitionReaderContext, stripes: Seq[OrcOutputStripe]):
      (HostMemoryBuffer, Long) = {
    withResource(new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))) { _ =>
      if (stripes.isEmpty) {
        return (null, 0L)
      }

      val hostBufferSize = estimateOutputSize(ctx, stripes)
      closeOnExcept(HostMemoryBuffer.allocate(hostBufferSize)) { hmb =>
        withResource(new HostMemoryOutputStream(hmb)) { out =>
          writeOrcOutputFile(ctx, out, stripes)
          (hmb, out.getPos)
        }
      }
    }
  }

  /**
   * Estimate how many bytes when writing the Stripes including HEADDER + STRIPES + FOOTER
   * @param ctx     the context to provide some necessary information
   * @param stripes a sequence of Stripe to be estimated
   * @return the estimated size
   */
  private def estimateOutputSize(ctx: OrcPartitionReaderContext, stripes: Seq[OrcOutputStripe]):
      Long = {
    // start with header magic
    var size: Long = OrcFile.MAGIC.length

    // account for the size of every stripe
    stripes.foreach { stripe =>
      size += stripe.infoBuilder.getIndexLength + stripe.infoBuilder.getDataLength
      // The true footer length is unknown since it may be compressed.
      // Use the uncompressed size as an upper bound.
      size += stripe.footer.getSerializedSize
    }

    // the original file's footer and postscript should be worst-case
    size += ctx.fileTail.getPostscript.getFooterLength
    size += ctx.fileTail.getPostscriptLength

    // and finally the single-byte postscript length at the end of the file
    size += 1

    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + 128 * 1024
  }

  /**
   * Read the Stripes to the HostMemoryBuffer with a new full ORC-format including
   * HEADER + STRIPES + FOOTER
   *
   * @param ctx     the context to provide some necessary information
   * @param rawOut  the out stream for HostMemoryBuffer
   * @param stripes a sequence of Stripe to be read
   */
  private def writeOrcOutputFile(
      ctx: OrcPartitionReaderContext,
      rawOut: HostMemoryOutputStream,
      stripes: Seq[OrcOutputStripe]): Unit = {

    // write ORC header
    val dataOut = new DataOutputStream(rawOut)
    dataOut.writeBytes(OrcFile.MAGIC)
    dataOut.flush()

    withCodecOutputStream(ctx, rawOut) { (outChannel, protoWriter, codecStream) =>
      var numRows = 0L
      val fileFooterBuilder = OrcProto.Footer.newBuilder
      // write the stripes
      stripes.foreach { stripe =>
        stripe.infoBuilder.setOffset(rawOut.getPos)
        copyStripeData(ctx, outChannel, stripe.inputDataRanges)
        val stripeFooterStartOffset = rawOut.getPos
        stripe.footer.writeTo(protoWriter)
        protoWriter.flush()
        codecStream.flush()
        stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
        fileFooterBuilder.addStripes(stripe.infoBuilder.build())
        numRows += stripe.infoBuilder.getNumberOfRows
      }

      // write the footer
      val footer = fileFooterBuilder.setHeaderLength(OrcFile.MAGIC.length)
        .setContentLength(rawOut.getPos)
        .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(buildReaderSchema(ctx)))
        .setNumberOfRows(numRows)
        .build()
      val footerStartOffset = rawOut.getPos
      footer.writeTo(protoWriter)
      protoWriter.flush()
      codecStream.flush()
      val postScriptStartOffset = rawOut.getPos

      // write the postscript (uncompressed)
      val postscript = OrcProto.PostScript.newBuilder(ctx.fileTail.getPostscript)
        .setFooterLength(postScriptStartOffset - footerStartOffset)
        .setMetadataLength(0)
        .build()
      postscript.writeTo(rawOut)
      val postScriptLength = rawOut.getPos - postScriptStartOffset
      if (postScriptLength > 255) {
        throw new IllegalArgumentException(s"PostScript length is too large at $postScriptLength")
      }
      rawOut.write(postScriptLength.toInt)
    }
  }
}

/**
 * A PartitionReader that reads an ORC file split on the GPU.
 *
 * Efficiently reading an ORC split on the GPU requires rebuilding the ORC file
 * in memory such that only relevant data is present in the memory file.
 * This avoids sending unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf Hadoop configuration
 * @param partFile file split to read
 * @param ctx     the context to provide some necessary information
 * @param readDataSchema Spark schema of what will be read from the file
 * @param debugDumpPrefix path prefix for dumping the memory file or null
 * @param maxReadBatchSizeRows maximum number of rows to read in a batch
 * @param maxReadBatchSizeBytes maximum number of bytes to read in a batch
 * @param execMetrics metrics to update during read
 */
class GpuOrcPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    ctx: OrcPartitionReaderContext,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics : Map[String, GpuMetric]) extends FilePartitionReaderBase(conf, execMetrics)
  with OrcPartitionReaderBase {
  private[this] var isFirstBatch = true

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (ctx.blockIterator.hasNext) {
      batch = readBatch()
    } else {
      metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
    }
    if (isFirstBatch) {
      if (batch.isEmpty) {
        // This is odd, but some operators return data even when there is no input so we need to
        // be sure that we grab the GPU if there were no batches.
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
      }
      isFirstBatch = false
    }

    batch.isDefined
  }

  override def close(): Unit = {
    super.close()
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("ORC readBatch", NvtxColor.GREEN)) { _ =>
      val currentStripes = populateCurrentBlockChunk(ctx.blockIterator, maxReadBatchSizeRows,
        maxReadBatchSizeBytes)
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentStripes.map(_.infoBuilder.getNumberOfRows).sum
        Some(new ColumnarBatch(Array.empty, numRows.toInt))
      } else {
        val table = readToTable(currentStripes)
        try {
          table.map(GpuColumnVector.from(_, readDataSchema.toArray.map(_.dataType)))
        } finally {
          table.foreach(_.close())
        }
      }
    }
  }

  private def readToTable(stripes: Seq[OrcOutputStripe]): Option[Table] = {
    val (dataBuffer, dataSize) = readPartFile(ctx, stripes)
    try {
      if (dataSize == 0) {
        None
      } else {
        dumpDataToFile(dataBuffer, dataSize, Array(partFile), Option(debugDumpPrefix), Some("orc"))
        val fieldNames = ctx.updatedReadSchema.getFieldNames.asScala.toArray
        val includedColumns = ctx.requestedMapping.map(_.map(fieldNames(_))).getOrElse(fieldNames)
        val parseOpts = ORCOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .withNumPyTypes(false)
          .includeColumn(includedColumns:_*)
          .build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
            metrics(GPU_DECODE_TIME))) { _ =>
          Table.readORC(parseOpts, dataBuffer, 0, dataSize)
        }
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read $numColumns from $partFile")
        }
        metrics(NUM_OUTPUT_BATCHES) += 1
        Some(table)
      }
    } finally {
      if (dataBuffer != null) {
        dataBuffer.close()
      }
    }
  }
}

// Singleton threadpool that is used across all the tasks.
// Please note that the TaskContext is not set in these threads and should not be used.
object OrcMultiFileThreadPoolFactory {
  private var threadPool: Option[ThreadPoolExecutor] = None

  private def initThreadPool(
      threadTag: String,
      numThreads: Int): ThreadPoolExecutor = synchronized {
    if (threadPool.isEmpty) {
      threadPool = Some(MultiFileThreadPoolUtil.createThreadPool(threadTag, numThreads))
    }
    threadPool.get
  }

  def getThreadPool(threadTag: String, numThreads: Int): ThreadPoolExecutor = {
    threadPool.getOrElse(initThreadPool(threadTag, numThreads))
  }
}

private object OrcTools extends Arm {

  def buildDataReader(ctx: OrcPartitionReaderContext): DataReader = {
    val fs = ctx.filePath.getFileSystem(ctx.conf)
    buildDataReader(ctx.compressionSize, ctx.compressionKind, ctx.fileSchema, ctx.readerOpts,
      ctx.filePath, fs, ctx.conf)
  }

  def buildDataReader(
      compressionSize: Int,
      compressionKind: CompressionKind,
      fileSchema: TypeDescription,
      readerOpts: Reader.Options,
      filePath: Path,
      fs: FileSystem,
      conf: Configuration): DataReader = {
    if (readerOpts.getDataReader != null) {
      readerOpts.getDataReader
    } else {
      val zeroCopy: Boolean = if (readerOpts.getUseZeroCopy != null) {
        readerOpts.getUseZeroCopy
      } else {
        OrcConf.USE_ZEROCOPY.getBoolean(conf)
      }
      val maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(conf)
      val file = filePath.getFileSystem(conf).open(filePath)
      //noinspection ScalaDeprecation
      RecordReaderUtils.createDefaultDataReader(DataReaderProperties.builder()
        .withBufferSize(compressionSize)
        .withCompression(compressionKind)
        .withFileSystem(fs)
        .withPath(filePath)
        .withFile(file) // explicitly specify the FSDataInputStream
        .withTypeCount(org.apache.orc.OrcUtils.getOrcTypes(fileSchema).size)
        .withZeroCopy(zeroCopy)
        .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
        .build())
    }
  }

}
/**
 * A tool to filter stripes
 *
 * @param sqlConf           SQLConf
 * @param broadcastedConf   the Hadoop configuration
 * @param pushedFilters     the PushDown filters
 */
private case class GpuOrcFileFilterHandler(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    pushedFilters: Array[Filter]) extends Arm {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis

  def filterStripes(
      partFile: PartitionedFile,
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType): OrcPartitionReaderContext = {

    val conf = broadcastedConf.value.value
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val filePath = new Path(new URI(partFile.filePath))
    val fs = filePath.getFileSystem(conf)
    val orcFileReaderOpts = OrcFile.readerOptions(conf).filesystem(fs)

    withResource(OrcFile.createReader(filePath, orcFileReaderOpts)) { orcReader =>
      val resultedColPruneInfo = requestedColumnIds(isCaseSensitive, dataSchema,
        readDataSchema, orcReader)
      if (resultedColPruneInfo.isEmpty) {
        orcReader.close()
        // Be careful when the OrcPartitionReaderContext is null, we should change
        // reader to EmptyPartitionReader for throwing exception
        null
      } else {
        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        orcResultSchemaString(canPruneCols, dataSchema, readDataSchema, partitionSchema, conf)
        assert(requestedColIds.length == readDataSchema.length,
          "[BUG] requested column IDs do not match required schema")
        // Only need to filter ORC's schema evolution if it cannot prune directly
        val requestedMapping = if (canPruneCols) {
          None
        } else {
          Some(requestedColIds)
        }
        val fullSchema = StructType(dataSchema ++ partitionSchema)
        val readerOpts = buildOrcReaderOpts(conf, orcReader, partFile, fullSchema)

        withResource(OrcTools.buildDataReader(orcReader.getCompressionSize,
          orcReader.getCompressionKind, orcReader.getSchema, readerOpts, filePath, fs, conf)) {
          dataReader =>
            new GpuOrcPartitionReaderUtils(filePath, conf, partFile, orcFileReaderOpts, orcReader,
              readerOpts, dataReader, requestedMapping).getOrcPartitionReaderContext
        }
      }
    }
  }

  private def buildOrcReaderOpts(
      conf: Configuration,
      orcReader: Reader,
      partFile: PartitionedFile,
      fullSchema: StructType): Reader.Options = {
    val readerOpts = OrcInputFormat.buildOptions(
      conf, orcReader, partFile.start, partFile.length)
    // create the search argument if we have pushed filters
    OrcFilters.createFilter(fullSchema, pushedFilters).foreach { f =>
      readerOpts.searchArgument(f, fullSchema.fieldNames)
    }
    readerOpts
  }


  /**
   * @return Returns the combination of requested column ids from the given ORC file and
   *         boolean flag to find if the pruneCols is allowed or not. Requested Column id can be
   *         -1, which means the requested column doesn't exist in the ORC file. Returns None
   *         if the given ORC file is empty.
   */
  def requestedColumnIds(
      isCaseSensitive: Boolean,
      dataSchema: StructType,
      requiredSchema: StructType,
      reader: Reader): Option[(Array[Int], Boolean)] = {
    val orcFieldNames = reader.getSchema.getFieldNames.asScala
    if (orcFieldNames.isEmpty) {
      // SPARK-8501: Some old empty ORC files always have an empty schema stored in their footer.
      None
    } else {
      if (orcFieldNames.forall(_.startsWith("_col"))) {
        // This is a ORC file written by Hive, no field names in the physical schema, assume the
        // physical schema maps to the data scheme by index.
        assert(orcFieldNames.length <= dataSchema.length, "The given data schema " +
          s"${dataSchema.catalogString} has less fields than the actual ORC physical schema, " +
          "no idea which columns were dropped, fail to read.")
        // for ORC file written by Hive, no field names
        // in the physical schema, there is a need to send the
        // entire dataSchema instead of required schema.
        // So pruneCols is not done in this case
        Some((requiredSchema.fieldNames.map { name =>
          val index = dataSchema.fieldIndex(name)
          if (index < orcFieldNames.length) {
            index
          } else {
            -1
          }
        }, false))
      } else {
        if (isCaseSensitive) {
          Some((requiredSchema.fieldNames.zipWithIndex.map { case (name, idx) =>
            if (orcFieldNames.indexWhere(caseSensitiveResolution(_, name)) != -1) {
              idx
            } else {
              -1
            }
          }, true))
        } else {
          // Do case-insensitive resolution only if in case-insensitive mode
          val caseInsensitiveOrcFieldMap = orcFieldNames.groupBy(_.toLowerCase(Locale.ROOT))
          Some((requiredSchema.fieldNames.zipWithIndex.map { case (requiredFieldName, idx) =>
            caseInsensitiveOrcFieldMap
              .get(requiredFieldName.toLowerCase(Locale.ROOT))
              .map { matchedOrcFields =>
                if (matchedOrcFields.size > 1) {
                  // Need to fail if there is ambiguity, i.e. more than one field is matched.
                  val matchedOrcFieldsString = matchedOrcFields.mkString("[", ", ", "]")
                  reader.close()
                  throw new RuntimeException(s"""Found duplicate field(s) "$requiredFieldName": """
                    + s"$matchedOrcFieldsString in case-insensitive mode")
                } else {
                  idx
                }
              }.getOrElse(-1)
          }, true))
        }
      }
    }
  }

  /**
   * Returns the result schema to read from ORC file. In addition, It sets
   * the schema string to 'orc.mapred.input.schema' so ORC reader can use later.
   *
   * @param canPruneCols Flag to decide whether pruned cols schema is send to resultSchema
   *                     or to send the entire dataSchema to resultSchema.
   * @param dataSchema   Schema of the orc files.
   * @param readDataSchema Result data schema created after pruning cols.
   * @param partitionSchema Schema of partitions.
   * @param conf Hadoop Configuration.
   * @return Returns the result schema as string.
   */
  def orcResultSchemaString(
      canPruneCols: Boolean,
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      conf: Configuration): String = {
    val resultSchemaString = if (canPruneCols) {
      OrcUtils.orcTypeDescriptionString(readDataSchema)
    } else {
      OrcUtils.orcTypeDescriptionString(StructType(dataSchema.fields ++ partitionSchema.fields))
    }
    OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, resultSchemaString)
    resultSchemaString
  }

  /**
   * An utility to get OrcPartitionReaderContext which contains some necessary information
   */
  private class GpuOrcPartitionReaderUtils(
      filePath: Path,
      conf: Configuration,
      partFile: PartitionedFile,
      orcFileReaderOpts: OrcFile.ReaderOptions,
      orcReader: Reader,
      readerOpts: Reader.Options,
      dataReader: DataReader,
      requestedMapping: Option[Array[Int]]) {

    private val ORC_STREAM_KINDS_IGNORED = util.EnumSet.of(
      OrcProto.Stream.Kind.BLOOM_FILTER,
      OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
      OrcProto.Stream.Kind.ROW_INDEX)

    def getOrcPartitionReaderContext: OrcPartitionReaderContext = {
      val updatedReadSchema = checkSchemaCompatibility(orcReader.getSchema, readerOpts.getSchema,
        readerOpts.getIsSchemaEvolutionCaseAware)
      val evolution = new SchemaEvolution(orcReader.getSchema, readerOpts.getSchema, readerOpts)
      val (sargApp, sargColumns) = getSearchApplier(evolution,
        orcFileReaderOpts.getUseUTCTimestamp)
      val splitStripes = orcReader.getStripes.asScala.filter(s =>
        s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
      val stripes = buildOutputStripes(splitStripes, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion)
      OrcPartitionReaderContext(filePath, conf, orcReader.getSchema, updatedReadSchema, evolution,
        orcReader.getFileTail, orcReader.getCompressionSize, orcReader.getCompressionKind,
        readerOpts, stripes.iterator.buffered, requestedMapping)
    }

    /**
     * Build an integer array that maps the original ORC file's column IDs
     * to column IDs in the memory file. Columns that are not present in
     * the memory file will have a mapping of -1.
     *
     * @param fileIncluded indicator per column in the ORC file whether it should be included
     * @return column mapping array
     */
    private def columnRemap(fileIncluded: Array[Boolean]): Array[Int] = {
      var nextOutputColumnId = 0
      val result = new Array[Int](fileIncluded.length)
      fileIncluded.indices.foreach { i =>
        if (fileIncluded(i)) {
          result(i) = nextOutputColumnId
          nextOutputColumnId += 1
        } else {
          result(i) = -1
        }
      }
      result
    }

    /**
     * Compute an array of booleans, one for each column in the ORC file, indicating whether the
     * corresponding ORC column ID should be included in the file to be loaded by the GPU.
     *
     * @param evolution ORC schema evolution instance
     * @return per-column inclusion flags
     */
    private def calcOrcFileIncluded(evolution: SchemaEvolution): Array[Boolean] = {
      if (requestedMapping.isDefined) {
        // ORC schema has no column names, so need to filter based on index
        val orcSchema = orcReader.getSchema
        val topFields = orcSchema.getChildren
        val numFlattenedCols = orcSchema.getMaximumId
        val included = new Array[Boolean](numFlattenedCols + 1)
        util.Arrays.fill(included, false)
        // first column is the top-level schema struct, always add it
        included(0) = true
        // find each top-level column requested by top-level index and add it and all child columns
        requestedMapping.get.foreach { colIdx =>
          val field = topFields.get(colIdx)
          (field.getId to field.getMaximumId).foreach { i =>
            included(i) = true
          }
        }
        included
      } else {
        evolution.getFileIncluded
      }
    }

    /**
     * Build the output stripe descriptors for what will appear in the ORC memory file.
     *
     * @param stripes descriptors for the ORC input stripes, filtered to what is in the split
     * @param evolution ORC SchemaEvolution
     * @param sargApp ORC search argument applier
     * @param sargColumns mapping of ORC search argument columns
     * @param ignoreNonUtf8BloomFilter true if bloom filters other than UTF8 should be ignored
     * @param writerVersion writer version from the original ORC input file
     * @return output stripes descriptors
     */
    private def buildOutputStripes(
        stripes: Seq[StripeInformation],
        evolution: SchemaEvolution,
        sargApp: SargApplier,
        sargColumns: Array[Boolean],
        ignoreNonUtf8BloomFilter: Boolean,
        writerVersion: OrcFile.WriterVersion): Seq[OrcOutputStripe] = {
      val fileIncluded = calcOrcFileIncluded(evolution)
      val columnMapping = columnRemap(fileIncluded)
      val result = new ArrayBuffer[OrcOutputStripe](stripes.length)
      stripes.foreach { stripe =>
        val stripeFooter = dataReader.readStripeFooter(stripe)
        val needStripe = if (sargApp != null) {
          // An ORC schema is a single struct type describing the schema fields
          val orcFileSchema = evolution.getFileType(0)
          val orcIndex = dataReader.readRowIndex(stripe, orcFileSchema, stripeFooter,
            ignoreNonUtf8BloomFilter, fileIncluded, null, sargColumns,
            writerVersion, null, null)
          val rowGroups = sargApp.pickRowGroups(stripe, orcIndex.getRowGroupIndex,
            orcIndex.getBloomFilterKinds, stripeFooter.getColumnsList, orcIndex.getBloomFilterIndex,
            true)
          rowGroups != SargApplier.READ_NO_RGS
        } else {
          true
        }

        if (needStripe) {
          result.append(buildOutputStripe(stripe, stripeFooter, columnMapping))
        }
      }

      result
    }

    /**
     * Build the output stripe descriptor for a corresponding input stripe
     * that should be copied to the ORC memory file.
     *
     * @param inputStripe input stripe descriptor
     * @param inputFooter input stripe footer
     * @param columnMapping mapping of input column IDs to output column IDs
     * @return output stripe descriptor
     */
    private def buildOutputStripe(
        inputStripe: StripeInformation,
        inputFooter: OrcProto.StripeFooter,
        columnMapping: Array[Int]): OrcOutputStripe = {
      val rangeCreator = new DiskRangeList.CreateHelper
      val footerBuilder = OrcProto.StripeFooter.newBuilder()
      var inputFileOffset = inputStripe.getOffset
      var outputStripeDataLength = 0L

      // copy stream descriptors for columns that are requested
      inputFooter.getStreamsList.asScala.foreach { stream =>
        val streamEndOffset = inputFileOffset + stream.getLength

        if (stream.hasKind && stream.hasColumn) {
          val outputColumn = columnMapping(stream.getColumn)
          val wantKind = !ORC_STREAM_KINDS_IGNORED.contains(stream.getKind)
          if (outputColumn >= 0 && wantKind) {
            // remap the column ID when copying the stream descriptor
            footerBuilder.addStreams(
              OrcProto.Stream.newBuilder(stream).setColumn(outputColumn).build)
            outputStripeDataLength += stream.getLength
            rangeCreator.addOrMerge(inputFileOffset, streamEndOffset, true, true)
          }
        }

        inputFileOffset = streamEndOffset
      }

      // add the column encodings that are relevant
      for (i <- 0 until inputFooter.getColumnsCount) {
        if (columnMapping(i) >= 0) {
          footerBuilder.addColumns(inputFooter.getColumns(i))
        }
      }

      // copy over the timezone
      if (inputFooter.hasWriterTimezone) {
        footerBuilder.setWriterTimezoneBytes(inputFooter.getWriterTimezoneBytes)
      }

      val outputStripeFooter = footerBuilder.build()

      // Fill out everything for StripeInformation except the file offset and footer length
      // which will be calculated when the stripe data is finally written.
      val infoBuilder = OrcProto.StripeInformation.newBuilder()
        .setIndexLength(0)
        .setDataLength(outputStripeDataLength)
        .setNumberOfRows(inputStripe.getNumberOfRows)

      OrcOutputStripe(infoBuilder, outputStripeFooter, rangeCreator.get)
    }

    /**
     * Check if the read schema is compatible with the file schema.
     *
     * @param fileSchema input file's ORC schema
     * @param readSchema ORC schema for what will be read
     * @param isCaseAware true if field names are case-sensitive
     * @return read schema mapped to the file's field names
     */
    private def checkSchemaCompatibility(
        fileSchema: TypeDescription,
        readSchema: TypeDescription,
        isCaseAware: Boolean): TypeDescription = {
      val fileFieldNames = fileSchema.getFieldNames.asScala
      val fileChildren = fileSchema.getChildren.asScala
      val caseSensitiveFileTypes = fileFieldNames.zip(fileChildren.zip(fileFieldNames)).toMap
      val fileTypesMap = if (isCaseAware) {
        caseSensitiveFileTypes
      } else {
        CaseInsensitiveMap[(TypeDescription, String)](caseSensitiveFileTypes)
      }

      val readerFieldNames = readSchema.getFieldNames.asScala
      val readerChildren = readSchema.getChildren.asScala
      val newReadSchema = TypeDescription.createStruct()
      readerFieldNames.zip(readerChildren).foreach { case (readField, readType) =>
        val (fileType, fileFieldName) = fileTypesMap.getOrElse(readField, (null, null))
        if (readType != fileType) {
          throw new QueryExecutionException("Incompatible schemas for ORC file" +
            s" at ${partFile.filePath}\n" +
            s" file schema: $fileSchema\n" +
            s" read schema: $readSchema")
        }
        newReadSchema.addField(fileFieldName, fileType)
      }

      newReadSchema
    }

    /**
     * Build an ORC search argument applier that can filter input file splits
     * when predicate push-down filters have been specified.
     *
     * @param evolution ORC SchemaEvolution
     * @param useUTCTimestamp true if timestamps are UTC
     * @return the search argument applier and search argument column mapping
     */
    private def getSearchApplier(
        evolution: SchemaEvolution,
        useUTCTimestamp: Boolean): (SargApplier, Array[Boolean]) = {
      val searchArg = readerOpts.getSearchArgument
      if (searchArg != null && orcReader.getRowIndexStride != 0) {
        val sa = new SargApplier(searchArg, orcReader.getRowIndexStride, evolution,
          orcReader.getWriterVersion, useUTCTimestamp)
        // SargApplier.sargColumns is unfortunately not visible so we redundantly compute it here.
        val filterCols = RecordReaderImpl.mapSargColumnsToOrcInternalColIdx(searchArg.getLeaves,
          evolution)
        val saCols = new Array[Boolean](evolution.getFileIncluded.length)
        filterCols.foreach { i =>
          if (i > 0) {
            saCols(i) = true
          }
        }
        (sa, saCols)
      } else {
        (null, null)
      }
    }
  }

}

/**
 * A PartitionReader that can read multiple ORC files in parallel. This is most efficient
 * running in a cloud environment where the I/O of reading is slow.
 *
 * Efficiently reading a ORC split on the GPU requires re-constructing the ORC file
 * in memory that contains just the Stripes that are needed. This avoids sending
 * unnecessary data to the GPU and saves GPU memory.
 *
 * @param conf the Hadoop configuration
 * @param files the partitioned files to read
 * @param dataSchema schema of the data
 * @param readDataSchema the Spark schema describing what will be read
 * @param partitionSchema Schema of partitions.
 * @param maxReadBatchSizeRows soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param numThreads the size of the threadpool
 * @param maxNumFileProcessed threshold to control the maximum file number to be
 *                            submitted to threadpool
 * @param debugDumpPrefix a path prefix to use for dumping the fabricated ORC data or null
 * @param filters filters passed into the filterHandler
 * @param filterHandler used to filter the ORC stripes
 * @param execMetrics the metrics
 */
class MultiFileCloudOrcPartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    dataSchema: StructType,
    override val readDataSchema: StructType,
    partitionSchema: StructType,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    numThreads: Int,
    maxNumFileProcessed: Int,
    debugDumpPrefix: String,
    filters: Array[Filter],
    filterHandler: GpuOrcFileFilterHandler,
    execMetrics: Map[String, GpuMetric])
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, filters,
    execMetrics) with MultiFileReaderFunctions with OrcPartitionReaderBase {

  private case class HostMemoryBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
    override val bytesRead: Long,
    updatedReadSchema: TypeDescription,
    requestedMapping: Option[Array[Int]]) extends HostMemoryBuffersWithMetaDataBase

  private class ReadBatchRunner(
      partFile: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends Callable[HostMemoryBuffersWithMetaDataBase]  {

    private var blockChunkIter: BufferedIterator[OrcOutputStripe] = null

    override def call(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()

      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
        partitionSchema)
      try {
        if (ctx == null || ctx.blockIterator.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          return HostMemoryBuffersWithMetaData(partFile, Array((null, 0)), bytesRead,
            ctx.updatedReadSchema, ctx.requestedMapping)
        }
        blockChunkIter = ctx.blockIterator
        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryBuffersWithMetaData(partFile, Array((null, 0)), bytesRead,
            ctx.updatedReadSchema, ctx.requestedMapping)
        } else {
          if (readDataSchema.isEmpty) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = ctx.blockIterator.map(_.infoBuilder.getNumberOfRows).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryBuffersWithMetaData(partFile, Array((null, numRows)), bytesRead,
              ctx.updatedReadSchema, ctx.requestedMapping)
          } else {
            while (blockChunkIter.hasNext) {
              val blocksToRead = populateCurrentBlockChunk(blockChunkIter, maxReadBatchSizeRows,
                maxReadBatchSizeBytes)
              hostBuffers += readPartFile(ctx, blocksToRead)
            }
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            if (isDone) {
              // got close before finishing
              hostBuffers.foreach(_._1.safeClose())
              HostMemoryBuffersWithMetaData(partFile, Array((null, 0)), bytesRead,
                ctx.updatedReadSchema, ctx.requestedMapping)
            } else {
              HostMemoryBuffersWithMetaData(partFile, hostBuffers.toArray, bytesRead,
                ctx.updatedReadSchema, ctx.requestedMapping)
            }
          }
        }
      } catch {
        case e: Throwable =>
          hostBuffers.foreach(_._1.safeClose())
          throw e
      }
    }
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param file    file to be read
   * @param conf    the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(file: PartitionedFile, conf: Configuration, filters: Array[Filter]):
      Callable[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(file, conf, filters)
  }

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The requirements:
   * 1. Same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. Different file formats have different ThreadPoolExecutors
   *
   * @param numThreads max number of threads to create
   * @return ThreadPoolExecutor
   */
  override def getThreadPool(numThreads: Int): ThreadPoolExecutor = {
    OrcMultiFileThreadPoolFactory.getThreadPool(getFileFormatShortName, numThreads)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override def getFileFormatShortName: String = "ORC"

  /**
   * Decode HostMemoryBuffers in GPU
   *
   * @param fileBufsAndMeta the file HostMemoryBuffer read from a PartitionedFile
   * @return Option[ColumnarBatch] which has been decoded by GPU
   */
  override def readBatch(fileBufsAndMeta: HostMemoryBuffersWithMetaDataBase):
      Option[ColumnarBatch] = {
    fileBufsAndMeta match {
      case buffer: HostMemoryBuffersWithMetaData =>
        val memBuffersAndSize = buffer.memBuffersAndSizes
        val (hostBuffer, size) = memBuffersAndSize.head
        val nextBatch = readBufferToTable(hostBuffer, size, buffer.partitionedFile.partitionValues,
          buffer.partitionedFile.filePath, buffer.updatedReadSchema, buffer.requestedMapping)
        if (memBuffersAndSize.length > 1) {
          val updatedBuffers = memBuffersAndSize.drop(1)
          currentFileHostBuffers = Some(buffer.copy(memBuffersAndSizes = updatedBuffers))
        } else {
          currentFileHostBuffers = None
        }
        nextBatch
      case _ => throw new RuntimeException("Wrong HostMemoryBuffersWithMetaData")
    }
  }

  // Send the buffer to GPU to decode
  private def readBufferToTable(
      hostBuffer: HostMemoryBuffer,
      dataSize: Long,
      partValues: InternalRow,
      fileName: String,
      updatedReadSchema: TypeDescription,
      requestedMapping: Option[Array[Int]] = None): Option[ColumnarBatch] = {
    // Not reading any data, but add in partition data if needed
    if (hostBuffer == null) {
      // Someone is going to process this data, even if it is just a row count
      GpuSemaphore.acquireIfNecessary(TaskContext.get())
      val emptyBatch = new ColumnarBatch(Array.empty, dataSize.toInt)
      return addPartitionValues(Some(emptyBatch), partValues, partitionSchema)
    }

    val table = withResource(hostBuffer) { _ =>
      // Dump ORC data into a file
      dumpDataToFile(hostBuffer, dataSize, files, Option(debugDumpPrefix), Some("orc"))

      val fieldNames = updatedReadSchema.getFieldNames.asScala.toArray
      val includedColumns = requestedMapping.map(_.map(fieldNames(_))).getOrElse(fieldNames)
      val parseOpts = ORCOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .withNumPyTypes(false)
        .includeColumn(includedColumns:_*)
        .build()

      // about to start using the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
          metrics(GPU_DECODE_TIME))) { _ =>
        Table.readORC(parseOpts, hostBuffer, 0, dataSize)
      }
      closeOnExcept(table) { _ =>
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
            s"but read $numColumns from $fileName")
        }
      }

      metrics(NUM_OUTPUT_BATCHES) += 1
      Some(table)
    }

    withResource(table) { _ =>
      val colTypes = readDataSchema.fields.map(f => f.dataType)
      val maybeBatch = table.map(t => GpuColumnVector.from(t, colTypes))
      maybeBatch.foreach { batch =>
        logDebug(s"GPU batch size: ${GpuColumnVector.getTotalDeviceMemoryUsed(batch)} bytes")
      }
      // we have to add partition values here for this batch, we already verified that
      // its not different for all the blocks in this batch
      addPartitionValues(maybeBatch, partValues, partitionSchema)
    }
  }
}

trait OrcCodecWritingHelper extends Arm {

  /** Executes the provided code block in the codec environment */
  def withCodecOutputStream[T](
      ctx: OrcPartitionReaderContext,
      out: HostMemoryOutputStream)
    (block: (WritableByteChannel, CodedOutputStream, OutStream) => T): T = {

    withResource(Channels.newChannel(out)) { outChannel =>
      val outReceiver = new PhysicalWriter.OutputReceiver {
        override def output(buffer: ByteBuffer): Unit = outChannel.write(buffer)
        override def suppress(): Unit = throw new UnsupportedOperationException(
          "suppress should not be called")
      }
      val codec = OrcCodecPool.getCodec(ctx.compressionKind)
      try {
        // buffer size must be greater than zero or writes hang (ORC-381)
        val orcBufferSize = if (ctx.compressionSize > 0) {
          ctx.compressionSize
        } else {
          // note that this buffer is just for writing meta-data
          OrcConf.BUFFER_SIZE.getDefaultValue.asInstanceOf[Int]
        }
        withResource(new OutStream(getClass.getSimpleName, orcBufferSize, codec,
            outReceiver)) { codecStream =>
          val protoWriter = CodedOutputStream.newInstance(codecStream)
          block(outChannel, protoWriter, codecStream)
        }
      } finally {
        OrcCodecPool.returnCodec(ctx.compressionKind, codec)
      }
    }
  }
}

// Orc schema wrapper
private case class OrcSchemaWrapper(schema: TypeDescription) extends SchemaBase

case class OrcStripeWithMeta(stripe: OrcOutputStripe, ctx: OrcPartitionReaderContext)
// OrcOutputStripe wrapper
private case class OrcDataStripe(stripeMeta: OrcStripeWithMeta) extends DataBlockBase
    with OrcCodecWritingHelper {

  override def getRowCount: Long = stripeMeta.stripe.infoBuilder.getNumberOfRows

  override def getReadDataSize: Long =
    stripeMeta.stripe.infoBuilder.getIndexLength + stripeMeta.stripe.infoBuilder.getDataLength

  // The stripe size in ORC should be equal to INDEX+DATA+STRIPE_FOOTER
  override def getBlockSize: Long = {
    stripeSize
  }

  // Calculate the true stripe size
  private lazy val stripeSize: Long = {
    val stripe = stripeMeta.stripe
    val ctx = stripeMeta.ctx
    val stripeDataSize = stripe.infoBuilder.getIndexLength + stripe.infoBuilder.getDataLength
    var initialSize: Long = 0
    // use stripe footer uncompressed size as a reference.
    // the size of compressed stripe footer can be < or = or > uncompressed size
    initialSize += stripe.footer.getSerializedSize
    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    initialSize += 128 * 1024

    // calculate the true stripe footer size
    withResource(HostMemoryBuffer.allocate(initialSize)) { hmb =>
      withResource(new HostMemoryOutputStream(hmb)) { rawOut =>
        withCodecOutputStream(ctx, rawOut) { (_, protoWriter, codecStream) =>
          stripe.footer.writeTo(protoWriter)
          protoWriter.flush()
          codecStream.flush()
          val stripeFooterSize = rawOut.getPos
          stripeDataSize + stripeFooterSize
        }
      }
    }
  }
}

// Contains meta about a single stripe of an ORC file
private case class OrcSingleStripeMeta(
  filePath: Path,
  dataBlock: OrcDataStripe,
  partitionValues: InternalRow,
  schema: OrcSchemaWrapper,
  isCorrectedRebaseMode: Boolean) extends SingleDataBlockInfo

/**
 *
 * @param conf                  Configuration
 * @param files                 files to be read
 * @param clippedStripes        the stripe metadata from the original Orc file that has been clipped
 *                              to only contain the column chunks to be read
 * @param readDataSchema        the Spark schema describing what will be read
 * @param debugDumpPrefix       a path prefix to use for dumping the fabricated Orc data or null
 * @param maxReadBatchSizeRows  soft limit on the maximum number of rows the reader reads per batch
 * @param maxReadBatchSizeBytes soft limit on the maximum number of bytes the reader reads per batch
 * @param execMetrics           metrics
 * @param partitionSchema       schema of partitions
 * @param numThreads            the size of the threadpool
 */
class MultiFileOrcPartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    clippedStripes: Seq[OrcSingleStripeMeta],
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedStripes, readDataSchema,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads, execMetrics)
    with OrcCommonFunctions {

  // implicit to convert SchemaBase to Orc TypeDescription
  implicit def toTypeDescription(schema: SchemaBase): TypeDescription =
    schema.asInstanceOf[OrcSchemaWrapper].schema

  implicit def toStripe(block: DataBlockBase): OrcStripeWithMeta =
    block.asInstanceOf[OrcDataStripe].stripeMeta

  implicit def toOrcStripeWithMetas(stripes: Seq[DataBlockBase]): Seq[OrcStripeWithMeta] = {
    stripes.map(_.asInstanceOf[OrcDataStripe].stripeMeta)
  }

  // TODO
  private val requestedMapping = clippedStripes(0).dataBlock.ctx.requestedMapping

  // The runner to copy stripes to the offset of HostMemoryBuffer and update
  // the StripeInformation to construct the file Footer
  class OrcCopyStripesRunner(
      file: Path,
      outhmb: HostMemoryBuffer,
      stripes: ArrayBuffer[DataBlockBase],
      offset: Long)
    extends Callable[(Seq[DataBlockBase], Long)] {

    override def call(): (Seq[DataBlockBase], Long) = {
      val startBytesRead = fileSystemBytesRead()
      // copy stripes to the HostMemoryBuffer
      withResource(outhmb) { _ =>
        withResource(new HostMemoryOutputStream(outhmb)) { rawOut =>
          // All stripes are from the same file, so it's safe to use the first stripe's ctx
          val ctx = stripes(0).ctx
          withCodecOutputStream(ctx, rawOut) { (outChannel, protoWriter, codecStream) =>
            // write the stripes including INDEX+DATA+STRIPE_FOOTER
            stripes.foreach { stripeWithMeta =>
              val stripe = stripeWithMeta.stripe
              stripe.infoBuilder.setOffset(offset + rawOut.getPos)
              copyStripeData(ctx, outChannel, stripe.inputDataRanges)
              val stripeFooterStartOffset = rawOut.getPos
              stripe.footer.writeTo(protoWriter)
              protoWriter.flush()
              codecStream.flush()
              stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
            }
          }
        }
      }
      val bytesRead = fileSystemBytesRead() - startBytesRead
      // the stripes returned has been updated, eg, stripe offset, stripe footer length
      (stripes, bytesRead)
    }
  }

  /**
   * To check if the next block will be split into another ColumnarBatch
   *
   * @param currentBlockInfo current SingleDataBlockInfo
   * @param nextBlockInfo    next SingleDataBlockInfo
   * @return true: split the next block into another ColumnarBatch and vice versa
   */
  override def checkIfNeedToSplitDataBlock(
      currentBlockInfo: SingleDataBlockInfo,
      nextBlockInfo: SingleDataBlockInfo): Boolean = {
    val schemaNextFile =
      nextBlockInfo.schema.getFieldNames.asScala
    val schemaCurrentfile =
      currentBlockInfo.schema.getFieldNames.asScala

    if (!schemaNextFile.sameElements(schemaCurrentfile)) {
      logInfo(s"Orc File schema for the next file ${nextBlockInfo.filePath}" +
        s" doesn't match current ${currentBlockInfo.filePath}, splitting it into another batch!")
      return true
    }
    false
  }

  /**
   * Calculate the output size according to the block chunks and the schema, and the
   * estimated output size will be used as the initialized size of allocating HostMemoryBuffer
   *
   * Please be note, the estimated size should be at least equal to size of HEAD + Blocks + FOOTER
   *
   * @param blocks a sequence of data block to be evaluated
   * @param schema schema info
   * @return Long, the estimated output size
   */
  override def calculateEstimatedBlocksOutputSize(
      filesAndBlocks: LinkedHashMap[Path, ArrayBuffer[DataBlockBase]],
      schema: SchemaBase): Long = {

    // start with header magic
    var size: Long = OrcFile.MAGIC.length

    filesAndBlocks.foreach {
      case (_, stripes) =>
        stripes.foreach { stripeMeta =>
          // account for the size of every stripe including index + data + stripe footer
          size += stripeMeta.getBlockSize
        }
        // the ctx is the same for all stripes
        val ctx = stripes(0).ctx
        // the original file's footer should be worst-case
        size += ctx.fileTail.getPostscript.getFooterLength
    }
    // the size of Postscript must be less than 256 bytes.
    size += 256
    // finally the single-byte postscript length at the end of the file
    size += 1
    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + 128 * 1024
  }

  /**
   * calculate the final block output size.
   *
   * There is no need to re-calculate the block size,
   * just calculate the footer size and plus footerOffset
   *
   * @param footerOffset  footer offset
   * @param blocks        blocks to be evaluated
   * @param schema        schema info
   * @return the output size
   */
  override def calculateFinalBlocksOutputSize(
      footerOffset: Long,
      stripes: Seq[DataBlockBase],
      schema: SchemaBase): Long = {

    // In calculateEstimatedBlocksOutputSize, we have got the true size for HEADER + All STRIPES
    // and the estimated the FileFooter size with the worst-case. So in this case, there is no need
    // to do re-allocate. We just ensure the final size is smaller than the initial size.

    footerOffset
  }

  /**
   * Get ThreadPoolExecutor to run the Callable.
   *
   * The rules:
   * 1. same ThreadPoolExecutor for cloud and coalescing for the same file format
   * 2. different file formats have different ThreadPoolExecutors
   *
   * @return ThreadPoolExecutor
   */
  override def getThreadPool(numThreads: Int): ThreadPoolExecutor = {
    OrcMultiFileThreadPoolFactory.getThreadPool(getFileFormatShortName, numThreads)
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param file   file to be read
   * @param outhmb the sliced HostMemoryBuffer to hold the blocks, and the implementation
   *               is in charge of closing it in sub-class
   * @param blocks blocks meta info to specify which blocks to be read
   * @param offset used as the offset adjustment
   * @return Callable[(Seq[DataBlockBase], Long)], which will be submitted to a
   *         ThreadPoolExecutor, and the Callable will return a tuple result and
   *         result._1 is block meta info with the offset adjusted
   *         result._2 is the bytes read
   */
  override def getBatchRunner(
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long): Callable[(Seq[DataBlockBase], Long)] = {
    new OrcCopyStripesRunner(file, outhmb, blocks, offset)
  }

  /**
   * File format short name used for logging and other things to uniquely identity
   * which file format is being used.
   *
   * @return the file format short name
   */
  override final def getFileFormatShortName: String = "ORC"

  /**
   * Sent host memory to GPU to decode
   *
   * @param dataBuffer  the data which can be decoded in GPU
   * @param dataSize    data size
   * @param isCorrectRebaseMode rebase mode
   * @param clippedSchema the clipped schema
   * @return Table
   */
  override def readBufferToTable(
      dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      isCorrectRebaseMode: Boolean,
      clippedSchema: SchemaBase): Table = {

    // Dump ORC data into a file
    dumpDataToFile(dataBuffer, dataSize, files, Option(debugDumpPrefix), Some("orc"))

    val fieldNames = clippedSchema.getFieldNames.asScala.toArray
    val includedColumns = requestedMapping.map(_.map(fieldNames(_))).getOrElse(fieldNames)
    val parseOpts = ORCOptions.builder()
      .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
      .withNumPyTypes(false)
      .includeColumn(includedColumns: _*)
      .build()

    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
      metrics(GPU_DECODE_TIME))) { _ =>
      Table.readORC(parseOpts, dataBuffer, 0, dataSize)
    }

    metrics(NUM_OUTPUT_BATCHES) += 1
    table
  }

  /**
   * Write a header for a specific file format. If there is no header for the file format,
   * just ignore it and return 0
   *
   * @param buffer where the header will be written
   * @return how many bytes written
   */
  override def writeFileHeader(buffer: HostMemoryBuffer): Long = {
    withResource(new HostMemoryOutputStream(buffer)) { out =>
      withResource(new DataOutputStream(out)) { dataOut =>
        dataOut.writeBytes(OrcFile.MAGIC)
        dataOut.flush()
      }
      out.getPos
    }
  }

  /**
   * Writer a footer for a specific file format. If there is no footer for the file format,
   * just return (hmb, offset)
   *
   * Please be note, some file formats may re-allocate the HostMemoryBuffer because of the
   * estimated initialized buffer size may be a little smaller than the actual size. So in
   * this case, the hmb should be closed in the implementation.
   *
   * @param buffer         The buffer holding (header + data blocks)
   * @param bufferSize     The total buffer size which equals to size of (header + blocks + footer)
   * @param footerOffset   Where begin to write the footer
   * @param blocks         The data block meta info
   * @param clippedSchema  The clipped schema info
   * @return the buffer and the buffer size
   */
  override def writeFileFooter(
      buffer: HostMemoryBuffer,
      bufferSize: Long,
      footerOffset: Long,
      stripes: Seq[DataBlockBase],
      clippedSchema: SchemaBase): (HostMemoryBuffer, Long) = {
    val lenLeft = bufferSize - footerOffset
    closeOnExcept(buffer) { _ =>
      withResource(buffer.slice(footerOffset, lenLeft)) { finalizehmb =>
        withResource(new HostMemoryOutputStream(finalizehmb)) { rawOut =>
          // We use the first stripe's ctx
          // What if the codec is different for the files which need to be combined?
          val ctx = stripes(0).ctx
          withCodecOutputStream(ctx, rawOut) { (_, protoWriter, codecStream) =>
            var numRows = 0L
            val fileFooterBuilder = OrcProto.Footer.newBuilder
            // get all the StripeInformation and the total number rows
            stripes.foreach { stripeWithMeta =>
              numRows += stripeWithMeta.stripe.infoBuilder.getNumberOfRows
              fileFooterBuilder.addStripes(stripeWithMeta.stripe.infoBuilder.build())
            }

            // write the footer
            val footer = fileFooterBuilder.setHeaderLength(OrcFile.MAGIC.length)
              .setContentLength(footerOffset) //the content length is everythings before file footer
              .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(buildReaderSchema(ctx)))
              .setNumberOfRows(numRows)
              .build()

            footer.writeTo(protoWriter)
            protoWriter.flush()
            codecStream.flush()

            val footerLength = rawOut.getPos

            // write the postscript (uncompressed)
            val postscript = OrcProto.PostScript.newBuilder(ctx.fileTail.getPostscript)
              .setFooterLength(footerLength)
              .setMetadataLength(0)
              .build()
            postscript.writeTo(rawOut)

            val postScriptLength = rawOut.getPos - footerLength
            if (postScriptLength > 255) {
              throw new IllegalArgumentException(s"PostScript length is too large at" +
                s" $postScriptLength")
            }
            rawOut.write(postScriptLength.toInt)
            (buffer, rawOut.getPos + footerOffset)
          }
        }
      }
    }
  }
}
