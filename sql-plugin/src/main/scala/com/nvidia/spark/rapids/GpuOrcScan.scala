/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

import java.io.{DataOutputStream, FileNotFoundException, IOException}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util
import java.util.Locale
import java.util.concurrent.{Callable, ThreadPoolExecutor}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.collection.mutable
import scala.language.implicitConversions
import scala.math.max

import ai.rapids.cudf._
import com.google.protobuf.CodedOutputStream
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.SchemaUtils._
import com.nvidia.spark.rapids.shims.OrcShims
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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.rapids.OrcFiltersWrapper
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

case class GpuOrcScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    rapidsConf: RapidsConf,
    queryUsesInputFile: Boolean = false)
  extends ScanWithMetrics with FileScan with Logging {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
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

  override def equals(obj: Any): Boolean = obj match {
    case o: GpuOrcScan =>
      super.equals(o) && dataSchema == o.dataSchema && options == o.options &&
          equivalentFilters(pushedFilters, o.pushedFilters) && rapidsConf == o.rapidsConf &&
          queryUsesInputFile == o.queryUsesInputFile
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}

object GpuOrcScan {
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
 * The multi-file partition reader factory for creating cloud reading or coalescing reading for
 * ORC file format.
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
  private val ignoreMissingFiles = sqlConf.ignoreMissingFiles
  private val ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  // we can't use the coalescing files reader when InputFileName, InputFileBlockStart,
  // or InputFileBlockLength because we are combining all the files into a single buffer
  // and we don't know which file is associated with each row.
  override val canUseCoalesceFilesReader: Boolean =
    rapidsConf.isOrcCoalesceFileReadEnabled && !(queryUsesInputFile || ignoreCorruptFiles)

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
      debugDumpPrefix, filters, filterHandler, metrics, ignoreMissingFiles, ignoreCorruptFiles)
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
    // Coalescing reading can't coalesce orc files with different compression kind, which means
    // we must split the different compress files into different ColumnarBatch.
    // So here try the best to group the same compression files together before hand.
    val compressionAndStripes = LinkedHashMap[CompressionKind, ArrayBuffer[OrcSingleStripeMeta]]()
    files.map { file =>
      val orcPartitionReaderContext = filterHandler.filterStripes(file, dataSchema,
        readDataSchema, partitionSchema)
      compressionAndStripes.getOrElseUpdate(orcPartitionReaderContext.compressionKind,
        new ArrayBuffer[OrcSingleStripeMeta]) ++=
        orcPartitionReaderContext.blockIterator.map(block =>
          OrcSingleStripeMeta(
            orcPartitionReaderContext.filePath,
            OrcDataStripe(OrcStripeWithMeta(block, orcPartitionReaderContext)),
            file.partitionValues,
            OrcSchemaWrapper(orcPartitionReaderContext.updatedReadSchema),
            OrcExtraInfo(orcPartitionReaderContext.requestedMapping)))
    }
    val clippedStripes = compressionAndStripes.values.flatten.toSeq
    new MultiFileOrcPartitionReader(conf, files, clippedStripes, readDataSchema, debugDumpPrefix,
      maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics, partitionSchema, numThreads,
      filterHandler.isCaseSensitive)
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
        readDataSchema, debugDumpPrefix, maxReadBatchSizeRows, maxReadBatchSizeBytes, metrics,
        filterHandler.isCaseSensitive))
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
 * @param filePath  ORC file path
 * @param conf      the Hadoop configuration
 * @param fileSchema the schema of the whole ORC file
 * @param updatedReadSchema read schema mapped to the file's field names
 * @param evolution  infer and track the evolution between the schema as stored in the file and
 *                   the schema that has been requested by the reader.
 * @param fileTail   the ORC FileTail
 * @param compressionSize  the ORC compression size
 * @param compressionKind  the ORC compression type
 * @param readerOpts  options for creating a RecordReader.
 * @param blockIterator an iterator over the ORC output stripes
 * @param requestedMapping the optional requested column ids
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
    requestedMapping: Option[Array[Int]])

/** Collections of some common functions for ORC */
trait OrcCommonFunctions extends OrcCodecWritingHelper {
  def execMetrics: Map[String, GpuMetric]

  /** Copy the stripe to the channel */
  protected def copyStripeData(
      ctx: OrcPartitionReaderContext,
      out: WritableByteChannel,
      inputDataRanges: DiskRangeList): Unit = {

    withResource(OrcTools.buildDataReader(ctx)) { dataReader =>
      val start = System.nanoTime()
      val bufferChunks = OrcShims.readFileData(dataReader, inputDataRanges)
      val mid = System.nanoTime()
      var current = bufferChunks
      while (current != null) {
        out.write(current.getData)
        if (dataReader.isTrackingDiskRanges && current.isInstanceOf[BufferChunk]) {
          dataReader.releaseBuffer(current.getData)
        }
        current = current.next
      }
      val end = System.nanoTime()
      execMetrics.get(READ_FS_TIME).foreach(_.add(mid - start))
      execMetrics.get(WRITE_BUFFER_TIME).foreach(_.add(end - mid))
    }
  }

  /** Get the ORC schema corresponding to the file being constructed for the GPU */
  protected def buildReaderSchema(ctx: OrcPartitionReaderContext): TypeDescription = {
    if (ctx.requestedMapping.isDefined) {
      // filter top-level schema based on requested mapping
      val orcSchema = ctx.updatedReadSchema
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

  /** write the ORC file Footer and PostScript  */
  protected def writeOrcFileFooter(
      ctx: OrcPartitionReaderContext,
      fileFooterBuilder: OrcProto.Footer.Builder,
      rawOut: HostMemoryOutputStream,
      footerStartOffset: Long,
      numRows: Long,
      protoWriter: CodedOutputStream,
      codecStream: OutStream) = {

    val startPoint = rawOut.getPos

    // write the footer
    val footer = fileFooterBuilder.setHeaderLength(OrcFile.MAGIC.length)
      .setContentLength(footerStartOffset) // the content length is everything before file footer
      .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(buildReaderSchema(ctx)))
      .setNumberOfRows(numRows)
      .build()

    footer.writeTo(protoWriter)
    protoWriter.flush()
    codecStream.flush()

    val footerLen = rawOut.getPos - startPoint

    // write the postscript (uncompressed)
    val postscript = OrcProto.PostScript.newBuilder(ctx.fileTail.getPostscript)
      .setFooterLength(footerLen)
      .setMetadataLength(0)
      .build()
    postscript.writeTo(rawOut)
    val postScriptLength = rawOut.getPos - startPoint - footerLen
    if (postScriptLength > 255) {
      throw new IllegalArgumentException(s"PostScript length is too large at $postScriptLength")
    }
    rawOut.write(postScriptLength.toInt)
  }

  protected def resolveTableSchema(schema: TypeDescription,
      requestedIds: Option[Array[Int]]): TypeDescription = {
    requestedIds.map { ids =>
      val retSchema = TypeDescription.createStruct()
      ids.foreach(id =>
        if (id >= 0) {
          retSchema.addField(schema.getFieldNames.get(id), schema.getChildren.get(id))
        }
      )
      retSchema
    }.getOrElse(schema)
  }

  /**
   * Extracts all fields(columns) of DECIMAL128, including child columns of nested types,
   * and returns the names of all fields.
   * The names of nested children are prefixed with their parents' information, which is the
   * acceptable format of cuDF reader options.
   */
  protected def filterDecimal128Fields(readColumns: Array[String],
      readSchema: StructType): Array[String] = {
    val buffer = mutable.ArrayBuffer.empty[String]

    def findImpl(prefix: String, fieldName: String, fieldType: DataType): Unit = fieldType match {
      case dt: DecimalType if DecimalType.isByteArrayDecimalType(dt) =>
        buffer.append(prefix + fieldName)
      case dt: StructType =>
        dt.fields.foreach(f => findImpl(prefix + fieldName + ".", f.name, f.dataType))
      case dt: ArrayType =>
        findImpl(prefix + fieldName + ".", "1", dt.elementType)
      case MapType(kt: DataType, vt: DataType, _) =>
        findImpl(prefix + fieldName + ".", "0", kt)
        findImpl(prefix + fieldName + ".", "1", vt)
      case _ =>
    }

    val rootFields = readColumns.toSet
    readSchema.fields.foreach {
      case f if rootFields.contains(f.name) => findImpl("", f.name, f.dataType)
      case _ =>
    }

    buffer.toArray
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

      writeOrcFileFooter(ctx, fileFooterBuilder, rawOut, rawOut.getPos, numRows,
        protoWriter, codecStream)
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
 * @param isCaseSensitive whether the name check should be case sensitive or not
 */
class GpuOrcPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    ctx: OrcPartitionReaderContext,
    override val readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    override val execMetrics : Map[String, GpuMetric],
    isCaseSensitive: Boolean) extends FilePartitionReaderBase(conf, execMetrics)
  with OrcPartitionReaderBase {

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (ctx.blockIterator.hasNext) {
      batch = readBatch()
    } else {
      metrics(PEAK_DEVICE_MEMORY) += maxDeviceMemory
    }

    // NOTE: At this point, the task may not have yet acquired the semaphore if `batch` is `None`.
    // We are not acquiring the semaphore here since this next() is getting called from
    // the `PartitionReaderIterator` which implements a standard iterator pattern, and
    // advertises `hasNext` as false when we return false here. No downstream tasks should
    // try to call next after `hasNext` returns false, and any task that produces some kind of
    // data when `hasNext` is false is responsible to get the semaphore themselves.
    batch.isDefined
  }

  override def close(): Unit = {
    super.close()
  }

  private def readBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxRange("ORC readBatch", NvtxColor.GREEN)) { _ =>
      val currentStripes = populateCurrentBlockChunk(ctx.blockIterator, maxReadBatchSizeRows,
        maxReadBatchSizeBytes)
      if (ctx.updatedReadSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentStripes.map(_.infoBuilder.getNumberOfRows).sum.toInt
        if (numRows == 0) {
          None
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = readDataSchema.safeMap(f =>
            GpuColumnVector.fromNull(numRows, f.dataType).asInstanceOf[SparkVector])
          Some(new ColumnarBatch(nullColumns.toArray, numRows))
        }
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
        val tableSchema = resolveTableSchema(ctx.updatedReadSchema, ctx.requestedMapping)
        val includedColumns = tableSchema.getFieldNames.asScala
        val decimal128Fields = filterDecimal128Fields(includedColumns.toArray, readDataSchema)
        val parseOpts = ORCOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .withNumPyTypes(false)
          .includeColumn(includedColumns:_*)
          .decimal128Column(decimal128Fields:_*)
          .build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

        val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
            metrics(GPU_DECODE_TIME))) { _ =>
          Table.readORC(parseOpts, dataBuffer, 0, dataSize)
        }
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
        metrics(NUM_OUTPUT_BATCHES) += 1

        Some(SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema, readDataSchema,
          isCaseSensitive))
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
object OrcMultiFileThreadPool extends MultiFileReaderThreadPool

private object OrcTools extends Arm {

  /** Build an ORC data reader using OrcPartitionReaderContext */
  def buildDataReader(ctx: OrcPartitionReaderContext): DataReader = {
    val fs = ctx.filePath.getFileSystem(ctx.conf)
    buildDataReader(ctx.compressionSize, ctx.compressionKind, ctx.fileSchema, ctx.readerOpts,
      ctx.filePath, fs, ctx.conf)
  }

  /** Build an ORC data reader */
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

      val typeCount = org.apache.orc.OrcUtils.getOrcTypes(fileSchema).size
      //noinspection ScalaDeprecation
      val reader = RecordReaderUtils.createDefaultDataReader(
        OrcShims.newDataReaderPropertiesBuilder(compressionSize, compressionKind, typeCount)
          .withFileSystem(fs)
          .withPath(filePath)
          .withZeroCopy(zeroCopy)
          .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
          .build())
      reader.open()
      reader
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

  private[rapids] val isCaseSensitive = sqlConf.caseSensitiveAnalysis

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

    // After getting the necessary information from ORC reader, we must close the ORC reader
    OrcShims.withReader(OrcFile.createReader(filePath, orcFileReaderOpts)) { orcReader =>
    val resultedColPruneInfo = requestedColumnIds(isCaseSensitive, dataSchema,
        readDataSchema, orcReader, conf)
      if (resultedColPruneInfo.isEmpty) {
        // Be careful when the OrcPartitionReaderContext is null, we should change
        // reader to EmptyPartitionReader for throwing exception
        null
      } else {
        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        orcResultSchemaString(canPruneCols, dataSchema, readDataSchema, partitionSchema, conf)
        assert(requestedColIds.length == readDataSchema.length,
          "[BUG] requested column IDs do not match required schema")

        // Create a local copy of broadcastedConf before we set task-local configs.
        val taskConf = new Configuration(conf)
        // Following SPARK-35783, set requested columns as OrcConf. This setting may not make
        // any difference. Just in case it might be important for the ORC methods called by us,
        // either today or in the future.
        val includeColumns = requestedColIds.filter(_ != -1).sorted.mkString(",")
        taskConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, includeColumns)

        // Only need to filter ORC's schema evolution if it cannot prune directly
        val requestedMapping = if (canPruneCols) {
          None
        } else {
          Some(requestedColIds)
        }
        val fullSchema = StructType(dataSchema ++ partitionSchema)
        val readerOpts = buildOrcReaderOpts(taskConf, orcReader, partFile, fullSchema)

        withResource(OrcTools.buildDataReader(orcReader.getCompressionSize,
          orcReader.getCompressionKind, orcReader.getSchema, readerOpts, filePath, fs, taskConf)) {
          dataReader =>
            new GpuOrcPartitionReaderUtils(filePath, taskConf, partFile, orcFileReaderOpts,
              orcReader, readerOpts, dataReader, requestedMapping).getOrcPartitionReaderContext
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
    OrcFiltersWrapper.createFilter(fullSchema, pushedFilters).foreach { f =>
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
      reader: Reader,
      conf: Configuration): Option[(Array[Int], Boolean)] = {
    val orcFieldNames = reader.getSchema.getFieldNames.asScala
    if (orcFieldNames.isEmpty) {
      // SPARK-8501: Some old empty ORC files always have an empty schema stored in their footer.
      None
    } else {
      if (OrcShims.forcePositionalEvolution(conf) || orcFieldNames.forall(_.startsWith("_col"))) {
        // This is either an ORC file written by an old version of Hive and there are no field
        // names in the physical schema, or `orc.force.positional.evolution=true` is forced because
        // the file was written by a newer version of Hive where
        // `orc.force.positional.evolution=true` was set (possibly because columns were renamed so
        // the physical schema doesn't match the data schema).
        // In these cases we map the physical schema to the data schema by index.
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
                  OrcShims.closeReader(reader)
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
      OrcShims.getOrcSchemaString(readDataSchema)
    } else {
      OrcShims.getOrcSchemaString(StructType(dataSchema.fields ++ partitionSchema.fields))
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
      val isCaseSensitive = readerOpts.getIsSchemaEvolutionCaseAware

      // align include status with the read schema during the potential field prune
      val readerOptInclude = readerOpts.getInclude match {
        case null => Array.fill(readerOpts.getSchema.getMaximumId + 1)(true)
        case a => a
      }
      val (updatedReadSchema, updatedInclude) = checkSchemaCompatibility(
        orcReader.getSchema, readerOpts.getSchema, isCaseSensitive, readerOptInclude)
      if (readerOpts.getInclude != null) {
        readerOpts.include(updatedInclude)
      }

      val evolution = new SchemaEvolution(orcReader.getSchema, updatedReadSchema, readerOpts)
      val (sargApp, sargColumns) = getSearchApplier(evolution,
        orcFileReaderOpts.getUseUTCTimestamp,
        orcReader.writerUsedProlepticGregorian(), orcFileReaderOpts.getConvertToProlepticGregorian)

      val splitStripes = orcReader.getStripes.asScala.filter(s =>
        s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
      val stripes = buildOutputStripes(splitStripes, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion, updatedReadSchema, isCaseSensitive)
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
     * @param fileSchema   ORC file schema
     * @param readerSchema ORC schema for what will be read
     * @return new column id mapping array and new column id -> old column id mapping array
     */
    private def columnRemap(
        fileIncluded: Array[Boolean],
        fileSchema: TypeDescription,
        readerSchema: TypeDescription,
        isCaseSensitive: Boolean): (Array[Int], Array[Int]) = {

      // A column mapping for the new column id in the new re-constructing orc
      val columnMapping = Array.fill[Int](fileIncluded.length)(-1)
      // The first column is the top-level schema struct, always set it to 0
      columnMapping(0) = 0
      // The mapping for the new column id to the old column id which is used to get the encodings
      val idMapping = Array.fill[Int](fileIncluded.length)(-1)
      // The first column is the top-level schema struct, always set it to 0
      idMapping(0) = 0

      // the new column sequential id for the in-coming re-constructing orc
      var nextOutputColumnId = 1

      def setMapping(id: Int) = {
        if (fileIncluded(id)) {
          idMapping(nextOutputColumnId) = id
          // change the column id for the new orc file
          columnMapping(id) = nextOutputColumnId
          nextOutputColumnId += 1
        }
      }

      // Recursively update columnMapping and idMapping according to the TypeDescription id.
      def updateMapping(rSchema: TypeDescription, fSchema: TypeDescription,
                        isRoot: Boolean = false): Unit = {
        assert(rSchema.getCategory == fSchema.getCategory)
        // 'findSubtype' in v1.5.x always follows the case sensitive rule to comparing the
        // column names, so cannot be used here.
        // The first level must be STRUCT type, and this also supports nested STRUCT type.
        if (rSchema.getCategory == TypeDescription.Category.STRUCT) {
          val mapSensitive = fSchema.getFieldNames.asScala.zip(fSchema.getChildren.asScala).toMap
          val name2ChildMap = if (isCaseSensitive) {
            mapSensitive
          } else {
            CaseInsensitiveMap[TypeDescription](mapSensitive)
          }
          // Config to match the top level columns using position rather than column names
          if (OrcShims.forcePositionalEvolution(conf)) {
            val rChildren = rSchema.getChildren
            val fChildren = fSchema.getChildren
            if (rChildren != null) {
              rChildren.asScala.zipWithIndex.foreach { case (rChild, id) =>
                val fChild = fChildren.get(id)
                setMapping(fChild.getId)
                updateMapping(rChild, fChild)
              }
            }
          } else {
            rSchema.getFieldNames.asScala.zip(rSchema.getChildren.asScala)
                .foreach { case (rName, rChild) =>
                  val fChild = name2ChildMap(rName)
                  setMapping(fChild.getId)
                  updateMapping(rChild, fChild)
                }
          }
        } else {
          val rChildren = rSchema.getChildren
          val fChildren = fSchema.getChildren
          if (rChildren != null) {
            // Go into children for List, Map, Union.
            rChildren.asScala.zipWithIndex.foreach { case (rChild, id) =>
              val fChild = fChildren.get(id)
              setMapping(fChild.getId)
              updateMapping(rChild, fChild)
            }
          }
        }
      }

      updateMapping(readerSchema, fileSchema, isRoot=true)
      (columnMapping, idMapping)
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
     * @param updatedReadSchema the read schema
     * @return output stripes descriptors
     */
    private def buildOutputStripes(
        stripes: Seq[StripeInformation],
        evolution: SchemaEvolution,
        sargApp: SargApplier,
        sargColumns: Array[Boolean],
        ignoreNonUtf8BloomFilter: Boolean,
        writerVersion: OrcFile.WriterVersion,
        updatedReadSchema: TypeDescription,
        isCaseSensitive: Boolean): Seq[OrcOutputStripe] = {
      val fileIncluded = calcOrcFileIncluded(evolution)
      val (columnMapping, idMapping) = columnRemap(fileIncluded, evolution.getFileSchema,
        updatedReadSchema, isCaseSensitive)
      OrcShims.filterStripes(stripes, conf, orcReader, dataReader,
        buildOutputStripe, evolution,
        sargApp, sargColumns, ignoreNonUtf8BloomFilter,
        writerVersion, fileIncluded, columnMapping, idMapping)
    }

    /**
     * Build the output stripe descriptor for a corresponding input stripe
     * that should be copied to the ORC memory file.
     *
     * @param inputStripe input stripe descriptor
     * @param inputFooter input stripe footer
     * @param columnMapping mapping of input column IDs to output column IDs
     * @param idMapping mapping for the new column id of the new file and
     *                  the old column id of original file
     * @return output stripe descriptor
     */
    private def buildOutputStripe(
        inputStripe: StripeInformation,
        inputFooter: OrcProto.StripeFooter,
        columnMapping: Array[Int],
        idMapping: Array[Int]): OrcOutputStripe = {
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
      idMapping.foreach { id =>
        if (id >= 0) {
          footerBuilder.addColumns(inputFooter.getColumns(id))
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
     * Check if the read schema is compatible with the file schema. Meanwhile, recursively
     * prune all incompatible required fields in terms of both ORC schema and include status.
     *
     * Only do the check for columns that can be found in the file schema, and
     * the missing ones are ignored, since a null column will be added in the final
     * output for each of them.
     *
     * It takes care of both the top and nested columns.
     *
     * @param fileSchema  input file's ORC schema
     * @param readSchema  ORC schema for what will be read
     * @param isCaseAware true if field names are case-sensitive
     * @param include     Array[Boolean] represents whether a field of specific ID is included
     * @return A tuple contains the pruned read schema and the pruned include status. For both
     *         return items, they only carry columns who also exist in the file schema.
     */
    private def checkSchemaCompatibility(
        fileSchema: TypeDescription,
        readSchema: TypeDescription,
        isCaseAware: Boolean,
        include: Array[Boolean]): (TypeDescription, Array[Boolean]) = {
      assert(fileSchema.getCategory == readSchema.getCategory)
      readSchema.getCategory match {
        case TypeDescription.Category.STRUCT =>
          // Check for the top or nested struct types.
          val fileFieldNames = fileSchema.getFieldNames.asScala
          val fileChildren = fileSchema.getChildren.asScala
          val caseSensitiveFileTypes = fileFieldNames.zip(fileChildren).toMap
          val fileTypesMap = if (isCaseAware) {
            caseSensitiveFileTypes
          } else {
            CaseInsensitiveMap[TypeDescription](caseSensitiveFileTypes)
          }
          val readerFieldNames = readSchema.getFieldNames.asScala
          val readerChildren = readSchema.getChildren.asScala
          val fileTypesWithIndex = for (
            (ftMap, index) <- fileTypesMap.zipWithIndex) yield (index, ftMap)

          val prunedReadSchema = TypeDescription.createStruct()
          val prunedInclude = mutable.ArrayBuffer(include(readSchema.getId))
          val readerMap = readerFieldNames.zip(readerChildren).toList
          readerMap.zipWithIndex.foreach { case ((readField, readType), idx) =>
            // Config to match the top level columns using position rather than column names
            if (OrcShims.forcePositionalEvolution(conf)) {
              fileTypesWithIndex(idx) match {
                case (_, fileReadType) => if (fileReadType == readType) {
                  val (newChild, childInclude) =
                    checkSchemaCompatibility(fileReadType, readType, isCaseAware, include)
                  prunedReadSchema.addField(readField, newChild)
                  prunedInclude ++= childInclude
                }
              }
            } else if (fileTypesMap.contains(readField)) {
              // Skip check for the missing names because a column with nulls will be added
              // for each of them.
              val (newChild, childInclude) = checkSchemaCompatibility(
                fileTypesMap(readField), readType, isCaseAware, include)
              prunedReadSchema.addField(readField, newChild)
              prunedInclude ++= childInclude
            }
          }
          prunedReadSchema -> prunedInclude.toArray
        // Go into children for LIST, MAP, UNION to filter out the missing names
        // for struct children.
        case TypeDescription.Category.LIST =>
          val prunedInclude = mutable.ArrayBuffer(include(readSchema.getId))
          val (newChild, childInclude) = checkSchemaCompatibility(fileSchema.getChildren.get(0),
            readSchema.getChildren.get(0), isCaseAware, include)
          prunedInclude ++= childInclude
          TypeDescription.createList(newChild) -> prunedInclude.toArray
        case TypeDescription.Category.MAP =>
          val prunedInclude = mutable.ArrayBuffer(include(readSchema.getId))
          val (newKey, keyInclude) = checkSchemaCompatibility(fileSchema.getChildren.get(0),
            readSchema.getChildren.get(0), isCaseAware, include)
          val (newValue, valueInclude) = checkSchemaCompatibility(fileSchema.getChildren.get(1),
            readSchema.getChildren.get(1), isCaseAware, include)
          prunedInclude ++= keyInclude
          prunedInclude ++= valueInclude
          TypeDescription.createMap(newKey, newValue) -> prunedInclude.toArray
        case TypeDescription.Category.UNION =>
          val newUnion = TypeDescription.createUnion()
          val prunedInclude = mutable.ArrayBuffer(include(readSchema.getId))
          readSchema.getChildren.asScala.zip(fileSchema.getChildren.asScala)
            .foreach { case (r, f) =>
              val (newChild, childInclude) = checkSchemaCompatibility(f, r, isCaseAware, include)
              newUnion.addUnionChild(newChild)
              prunedInclude ++= childInclude
            }
          newUnion -> prunedInclude.toArray
        // Primitive types should be equal to each other.
        case _ =>
          if (!OrcShims.typeDescriptionEqual(fileSchema, readSchema)) {
            throw new QueryExecutionException("Incompatible schemas for ORC file" +
              s" at ${partFile.filePath}\n" +
              s" file schema: $fileSchema\n" +
              s" read schema: $readSchema")
          }
          readSchema.clone() -> Array(include(readSchema.getId))
      }
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
        useUTCTimestamp: Boolean,
        writerUsedProlepticGregorian: Boolean,
        convertToProlepticGregorian: Boolean): (SargApplier, Array[Boolean]) = {
      val searchArg = readerOpts.getSearchArgument
      if (searchArg != null && orcReader.getRowIndexStride != 0) {
        val sa = new SargApplier(searchArg, orcReader.getRowIndexStride, evolution,
          orcReader.getWriterVersion, useUTCTimestamp,
          writerUsedProlepticGregorian, convertToProlepticGregorian)
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
 * @param ignoreMissingFiles Whether to ignore missing files
 * @param ignoreCorruptFiles Whether to ignore corrupt files
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
    override val execMetrics: Map[String, GpuMetric],
    ignoreMissingFiles: Boolean,
    ignoreCorruptFiles: Boolean)
  extends MultiFileCloudPartitionReaderBase(conf, files, numThreads, maxNumFileProcessed, filters,
    execMetrics, ignoreCorruptFiles) with MultiFileReaderFunctions with OrcPartitionReaderBase {

  private case class HostMemoryEmptyMetaData(
    override val partitionedFile: PartitionedFile,
    bufferSize: Long,
    override val bytesRead: Long,
    updatedReadSchema: TypeDescription,
    readSchema: StructType) extends HostMemoryBuffersWithMetaDataBase {

    override def memBuffersAndSizes: Array[(HostMemoryBuffer, Long)] =
      Array(null.asInstanceOf[HostMemoryBuffer] -> bufferSize)
  }

  private case class HostMemoryBuffersWithMetaData(
    override val partitionedFile: PartitionedFile,
    override val memBuffersAndSizes: Array[(HostMemoryBuffer, Long)],
    override val bytesRead: Long,
    updatedReadSchema: TypeDescription,
    requestedMapping: Option[Array[Int]]) extends HostMemoryBuffersWithMetaDataBase


  private class ReadBatchRunner(
      taskContext: TaskContext,
      partFile: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]) extends Callable[HostMemoryBuffersWithMetaDataBase]  {

    private var blockChunkIter: BufferedIterator[OrcOutputStripe] = null

    override def call(): HostMemoryBuffersWithMetaDataBase = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        doRead()
      } catch {
        case e: FileNotFoundException if ignoreMissingFiles =>
          logWarning(s"Skipped missing file: ${partFile.filePath}", e)
          HostMemoryEmptyMetaData(partFile, 0, 0, null, null)
        // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
        case e: FileNotFoundException if !ignoreMissingFiles => throw e
        case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
          logWarning(
            s"Skipped the rest of the content in the corrupted file: ${partFile.filePath}", e)
          HostMemoryEmptyMetaData(partFile, 0, 0, null, null)
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): HostMemoryBuffersWithMetaDataBase = {
      val startingBytesRead = fileSystemBytesRead()

      val hostBuffers = new ArrayBuffer[(HostMemoryBuffer, Long)]
      val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
        partitionSchema)
      try {
        if (ctx == null || ctx.blockIterator.isEmpty) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // no blocks so return null buffer and size 0
          return HostMemoryEmptyMetaData(partFile, 0, bytesRead,
            ctx.updatedReadSchema, readDataSchema)
        }
        blockChunkIter = ctx.blockIterator
        if (isDone) {
          val bytesRead = fileSystemBytesRead() - startingBytesRead
          // got close before finishing
          HostMemoryEmptyMetaData(partFile, 0, bytesRead, ctx.updatedReadSchema, readDataSchema)
        } else {
          if (ctx.updatedReadSchema.isEmpty) {
            val bytesRead = fileSystemBytesRead() - startingBytesRead
            val numRows = ctx.blockIterator.map(_.infoBuilder.getNumberOfRows).sum.toInt
            // overload size to be number of rows with null buffer
            HostMemoryEmptyMetaData(partFile, numRows, bytesRead,
              ctx.updatedReadSchema, readDataSchema)
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
              HostMemoryEmptyMetaData(partFile, 0, bytesRead, ctx.updatedReadSchema, readDataSchema)
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
   * @param tc      task context to use
   * @param file    file to be read
   * @param conf    the Configuration parameters
   * @param filters push down filters
   * @return Callable[HostMemoryBuffersWithMetaDataBase]
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: PartitionedFile,
      conf: Configuration,
      filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] = {
    new ReadBatchRunner(tc, file, conf, filters)
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
    OrcMultiFileThreadPool.getOrCreateThreadPool(getFileFormatShortName, numThreads)
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
      case meta: HostMemoryEmptyMetaData =>
        // Not reading any data, but add in partition data if needed
        val rows = meta.bufferSize.toInt
        val batch = if (rows == 0) {
          new ColumnarBatch(Array.empty, 0)
        } else {
          // Someone is going to process this data, even if it is just a row count
          GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))
          val nullColumns = meta.readSchema.fields.safeMap(f =>
            GpuColumnVector.fromNull(rows, f.dataType).asInstanceOf[SparkVector])
          new ColumnarBatch(nullColumns, rows)
        }
        addPartitionValues(Some(batch), meta.partitionedFile.partitionValues, partitionSchema)

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
    val table = withResource(hostBuffer) { _ =>
      // Dump ORC data into a file
      dumpDataToFile(hostBuffer, dataSize, files, Option(debugDumpPrefix), Some("orc"))

      val tableSchema = resolveTableSchema(updatedReadSchema, requestedMapping)
      val includedColumns = tableSchema.getFieldNames.asScala
      val decimal128Fields = filterDecimal128Fields(includedColumns.toArray, readDataSchema)
      val parseOpts = ORCOptions.builder()
        .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
        .withNumPyTypes(false)
        .includeColumn(includedColumns:_*)
        .decimal128Column(decimal128Fields:_*)
        .build()

      // about to start using the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

      val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
          metrics(GPU_DECODE_TIME))) { _ =>
        Table.readORC(parseOpts, hostBuffer, 0, dataSize)
      }
      closeOnExcept(table) { _ =>
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
      }

      metrics(NUM_OUTPUT_BATCHES) += 1
      Some(SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema, readDataSchema,
        filterHandler.isCaseSensitive))
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
        withResource(OrcShims.newOrcOutStream(
          getClass.getSimpleName, orcBufferSize, codec, outReceiver)) { codecStream =>
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
private case class OrcSchemaWrapper(schema: TypeDescription) extends SchemaBase {

  override def fieldNames: Array[String] = schema.getFieldNames.asScala.toArray
}

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

/** Orc extra information containing the requested column ids for the current coalescing stripes */
case class OrcExtraInfo(requestedMapping: Option[Array[Int]]) extends ExtraInfo

// Contains meta about a single stripe of an ORC file
private case class OrcSingleStripeMeta(
  filePath: Path, // Orc file path
  dataBlock: OrcDataStripe, // Orc stripe information with the OrcPartitionReaderContext
  partitionValues: InternalRow, // partitioned values
  schema: OrcSchemaWrapper, // Orc schema
  extraInfo: OrcExtraInfo // Orc ExtraInfo containing the requested column ids
) extends SingleDataBlockInfo

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
 * @param isCaseSensitive       whether the name check should be case sensitive or not
 */
class MultiFileOrcPartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    clippedStripes: Seq[OrcSingleStripeMeta],
    readDataSchema: StructType,
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    override val execMetrics: Map[String, GpuMetric],
    partitionSchema: StructType,
    numThreads: Int,
    isCaseSensitive: Boolean)
  extends MultiFileCoalescingPartitionReaderBase(conf, clippedStripes, readDataSchema,
    partitionSchema, maxReadBatchSizeRows, maxReadBatchSizeBytes, numThreads, execMetrics)
    with OrcCommonFunctions {

  // implicit to convert SchemaBase to Orc TypeDescription
  implicit def toTypeDescription(schema: SchemaBase): TypeDescription =
    schema.asInstanceOf[OrcSchemaWrapper].schema

  implicit def toStripe(block: DataBlockBase): OrcStripeWithMeta =
    block.asInstanceOf[OrcDataStripe].stripeMeta

  implicit def toOrcStripeWithMetas(stripes: Seq[DataBlockBase]): Seq[OrcStripeWithMeta] =
    stripes.map(_.asInstanceOf[OrcDataStripe].stripeMeta)

  implicit def toOrcExtraInfo(in: ExtraInfo): OrcExtraInfo =
    in.asInstanceOf[OrcExtraInfo]

  // Estimate the size of StripeInformation with the worst case.
  // The serialized size may be different because of the different values.
  // Here set most of values to "Long.MaxValue" to get the worst case.
  lazy val sizeOfStripeInformation = {
    OrcProto.StripeInformation.newBuilder()
      .setOffset(Long.MaxValue)
      .setIndexLength(0) // Index stream is pruned
      .setDataLength(Long.MaxValue)
      .setFooterLength(Int.MaxValue) // StripeFooter size should be small
      .setNumberOfRows(Long.MaxValue)
      .build().getSerializedSize
  }

  // The runner to copy stripes to the offset of HostMemoryBuffer and update
  // the StripeInformation to construct the file Footer
  class OrcCopyStripesRunner(
      taskContext: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      stripes: ArrayBuffer[DataBlockBase],
      offset: Long)
    extends Callable[(Seq[DataBlockBase], Long)] {

    override def call(): (Seq[DataBlockBase], Long) = {
      TrampolineUtil.setTaskContext(taskContext)
      try {
        doRead()
      } finally {
        TrampolineUtil.unsetTaskContext()
      }
    }

    private def doRead(): (Seq[DataBlockBase], Long) = {
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

    if (currentBlockInfo.dataBlock.ctx.compressionKind !=
        nextBlockInfo.dataBlock.ctx.compressionKind) {
      logInfo(s"Orc File compression for the next file ${nextBlockInfo.filePath}" +
        s" doesn't match current ${currentBlockInfo.filePath}, splitting it into another batch!")
      return true
    }

    val ret = (currentBlockInfo.extraInfo.requestedMapping,
      nextBlockInfo.extraInfo.requestedMapping) match {
      case (None, None) => true
      case (Some(cols1), Some(cols2)) =>
        if (cols1.sameElements(cols2)) true else false
      case (_, _) => {
        false
      }
    }

    if (!ret) {
      logInfo(s"Orc requested column ids for the next file ${nextBlockInfo.filePath}" +
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
   * @param batchContext the batch building context
   * @return Long, the estimated output size
   */
  override def calculateEstimatedBlocksOutputSize(batchContext: BatchContext): Long = {
    val filesAndBlocks = batchContext.origChunkedBlocks
    // start with header magic
    var size: Long = OrcFile.MAGIC.length

    filesAndBlocks.foreach {
      case (_, stripes) =>
        // path is not needed here anymore, since filesAndBlocks is already a map: file -> stripes.
        // and every stripe in the same file has the OrcPartitionReaderContext. we just get the
        // OrcPartitionReaderContext from the first stripe and use the file footer size as
        // the worst-case
        stripes.foreach { stripeMeta =>
          // account for the size of every stripe including index + data + stripe footer
          size += stripeMeta.getBlockSize

          // add StripeInformation size in advance which should be calculated in Footer
          size += sizeOfStripeInformation
        }
    }

    val blockIter = filesAndBlocks.valuesIterator
    if (blockIter.hasNext) {
      val blocks = blockIter.next()

      // add the first orc file's footer length to cover ORC schema and other information
      size += blocks(0).ctx.fileTail.getPostscript.getFooterLength
    }

    // Per ORC v1 spec, the size of Postscript must be less than 256 bytes.
    size += 256
    // finally the single-byte postscript length at the end of the file
    size += 1
    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + 128 * 1024
  }

  /**
   * Calculate the final block output size which will be used to decide
   * if re-allocate HostMemoryBuffer
   *
   * For now, we still don't know the ORC file footer size, so we can't get the final size.
   *
   * Since calculateEstimatedBlocksOutputSize has over-estimated the size, it's safe to
   * use it and it will not cause HostMemoryBuffer re-allocating.
   *
   * @param footerOffset  footer offset
   * @param stripes       stripes to be evaluated
   * @param batchContext  the batch building context
   * @return the output size
   */
  override def calculateFinalBlocksOutputSize(
      footerOffset: Long,
      stripes: Seq[DataBlockBase],
      batchContext: BatchContext): Long = {

    // In calculateEstimatedBlocksOutputSize, we have got the true size for
    // HEADER + All STRIPES + the estimated the FileFooter size with the worst-case.
    // We return a size that is smaller than the initial size to avoid the re-allocate

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
    OrcMultiFileThreadPool.getOrCreateThreadPool(getFileFormatShortName, numThreads)
  }

  /**
   * The sub-class must implement the real file reading logic in a Callable
   * which will be running in a thread pool
   *
   * @param tc     task context to use
   * @param file   file to be read
   * @param outhmb the sliced HostMemoryBuffer to hold the blocks, and the implementation
   *               is in charge of closing it in sub-class
   * @param blocks blocks meta info to specify which blocks to be read
   * @param offset used as the offset adjustment
   * @param batchContext the batch building context
   * @return Callable[(Seq[DataBlockBase], Long)], which will be submitted to a
   *         ThreadPoolExecutor, and the Callable will return a tuple result and
   *         result._1 is block meta info with the offset adjusted
   *         result._2 is the bytes read
   */
  override def getBatchRunner(
      tc: TaskContext,
      file: Path,
      outhmb: HostMemoryBuffer,
      blocks: ArrayBuffer[DataBlockBase],
      offset: Long,
      batchContext: BatchContext): Callable[(Seq[DataBlockBase], Long)] = {
    new OrcCopyStripesRunner(tc, file, outhmb, blocks, offset)
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
   * @param clippedSchema the clipped schema
   * @return Table
   */
  override def readBufferToTable(
      dataBuffer: HostMemoryBuffer,
      dataSize: Long,
      clippedSchema: SchemaBase,
      extraInfo: ExtraInfo): Table = {

    // Dump ORC data into a file
    dumpDataToFile(dataBuffer, dataSize, files, Option(debugDumpPrefix), Some("orc"))

    val tableSchema = resolveTableSchema(clippedSchema, extraInfo.requestedMapping)
    val includedColumns = tableSchema.getFieldNames.asScala
    val decimal128Fields = filterDecimal128Fields(includedColumns.toArray, readDataSchema)
    val parseOpts = ORCOptions.builder()
      .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
      .withNumPyTypes(false)
      .includeColumn(includedColumns: _*)
      .decimal128Column(decimal128Fields:_*)
      .build()

    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

    val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
      metrics(GPU_DECODE_TIME))) { _ =>
      Table.readORC(parseOpts, dataBuffer, 0, dataSize)
    }

    metrics(NUM_OUTPUT_BATCHES) += 1
    SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema, readDataSchema, isCaseSensitive)
  }

  /**
   * Write a header for a specific file format. If there is no header for the file format,
   * just ignore it and return 0
   *
   * @param buffer where the header will be written
   * @param batchContext the batch building context
   * @return how many bytes written
   */
  override def writeFileHeader(buffer: HostMemoryBuffer, batchContext: BatchContext): Long = {
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
   * @param stripes        The data block meta info
   * @param clippedSchema  The clipped schema info
   * @param batchContext   The batch building context
   * @return the buffer and the buffer size
   */
  override def writeFileFooter(
      buffer: HostMemoryBuffer,
      bufferSize: Long,
      footerOffset: Long,
      stripes: Seq[DataBlockBase],
      batchContext: BatchContext): (HostMemoryBuffer, Long) = {
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

            writeOrcFileFooter(ctx, fileFooterBuilder, rawOut, footerOffset, numRows,
              protoWriter, codecStream)
            (buffer, rawOut.getPos + footerOffset)
          }
        }
      }
    }
  }
}
