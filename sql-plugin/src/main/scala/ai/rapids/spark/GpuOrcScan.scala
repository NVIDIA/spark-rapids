/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import java.io.DataOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.math.max
import ai.rapids.cudf._
import ai.rapids.spark.GpuMetricNames._
import ai.rapids.spark.GpuOrcPartitionReader.{OrcOutputStripe, OrcPartitionReaderContext}
import com.google.protobuf.CodedOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.io.DiskRangeList
import org.apache.orc.{CompressionKind, DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl._
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, FileScan}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.OrcFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.TaskContext

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
    rapidsConf: RapidsConf)
  extends FileScan with ScanWithMetrics {

  override def isSplitable(path: Path): Boolean = true

  override def createReaderFactory(): PartitionReaderFactory = {
    // Unset any serialized search argument setup by Spark's OrcScanBuilder as
    // it will be incompatible due to shading and potential ORC classifier mismatch.
    hadoopConf.unset(OrcConf.KRYO_SARG.getAttribute)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    GpuOrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics)
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: GpuOrcScan =>
      super.equals(o) && dataSchema == o.dataSchema && options == o.options &&
        equivalentFilters(pushedFilters, o.pushedFilters) && rapidsConf == o.rapidsConf
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}

object GpuOrcScan {
  def tagSupport(scanMeta: ScanMeta[OrcScan]): Unit = {
    val scan = scanMeta.wrapped
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    tagSupport(scan.sparkSession, schema, scanMeta)
  }

  def tagSupport(
      sparkSession: SparkSession,
      schema: StructType,
      meta: RapidsMeta[_, _, _]): Unit = {
    schema.foreach { field =>
      if (!GpuColumnVector.isSupportedType(field.dataType)) {
        meta.willNotWorkOnGpu(s"GpuOrcScan does not support fields of type ${field.dataType}")
      }
    }
  }
}

case class GpuOrcPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    pushedFilters: Array[Filter],
    @transient rapidsConf: RapidsConf,
    metrics : Map[String, SQLMetric]) extends FilePartitionReaderFactory {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix
  private val maxReadBatchSizeRows: Integer = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val orcSchemaString = OrcUtils.orcTypeDescriptionString(readDataSchema)
    OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, orcSchemaString)
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val fullSchema = StructType(dataSchema ++ partitionSchema)
    val reader = new GpuOrcPartitionReader(conf, partFile, dataSchema, readDataSchema,
      fullSchema, pushedFilters, debugDumpPrefix, maxReadBatchSizeRows, maxReadBatchSizeBytes,
      metrics)
    ColumnarPartitionReaderWithPartitionValues.newReader(partFile, reader, partitionSchema)
  }
}

object GpuOrcPartitionReader {
  /**
    * This class describes a stripe that will appear in the ORC output memory file.
    *
    * @param infoBuilder builder for output stripe info that has been populated with
    *                    all fields except those that can only be known when the file
    *                    is being written (e.g.: file offset, compressed footer length)
    * @param footer stripe footer
    * @param inputDataRanges input file ranges (based at file offset 0) of stripe data
    */
  private case class OrcOutputStripe(
      infoBuilder: OrcProto.StripeInformation.Builder,
      footer: OrcProto.StripeFooter,
      inputDataRanges: DiskRangeList)

  // These streams are not copied to the GPU since they are only used for filtering.
  // Filtering is already being performed as the ORC memory file is built.
  private val ORC_STREAM_KINDS_IGNORED = util.EnumSet.of(
    OrcProto.Stream.Kind.BLOOM_FILTER,
    OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
    OrcProto.Stream.Kind.ROW_INDEX)

  /**
    * This class holds fields needed to read and iterate over the OrcFile
    *
    * @param updatedReadSchema read schema mapped to the file's field names
    * @param evolution ORC SchemaEvolution
    * @param dataReader ORC DataReader
    * @param orcReader ORC Input File Reader
    * @param blockIterator An iterator over the ORC output stripes
    */
  private case class OrcPartitionReaderContext(updatedReadSchema: TypeDescription,
    evolution: SchemaEvolution, dataReader: DataReader, orcReader: Reader,
    blockIterator: BufferedIterator[OrcOutputStripe])
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
  * @param dataSchema Spark schema of the file
  * @param readDataSchema Spark schema of what will be read from the file
  * @param debugDumpPrefix path prefix for dumping the memory file or null
  */
class GpuOrcPartitionReader(
    conf: Configuration,
    partFile: PartitionedFile,
    dataSchema: StructType,
    readDataSchema: StructType,
    fullSchema: StructType,
    pushedFilters: Array[Filter],
    debugDumpPrefix: String,
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics : Map[String, SQLMetric]) extends PartitionReader[ColumnarBatch] with Logging
  with ScanWithMetrics {
  private var batch: Option[ColumnarBatch] = None
  private val ctx = initializeOrcReaders
  private var maxDeviceMemory: Long = 0

  metrics = execMetrics

  private def initializeOrcReaders: OrcPartitionReaderContext = {
    val filePath = new Path(new URI(partFile.filePath))
    val fs = filePath.getFileSystem(conf)
    val orcFileReaderOpts = OrcFile.readerOptions(conf).filesystem(fs)
    val orcReader = OrcFile.createReader(filePath, orcFileReaderOpts)
    val readerOpts = OrcInputFormat.buildOptions(
      conf, orcReader, partFile.start, partFile.length)
    // create the search argument if we have pushed filters
    OrcFilters.createFilter(fullSchema, pushedFilters).foreach { f =>
      readerOpts.searchArgument(f, fullSchema.fieldNames)
    }
    val updatedReadSchema = checkSchemaCompatibility(
      orcReader.getSchema, readerOpts.getSchema,
      readerOpts.getIsSchemaEvolutionCaseAware)
    val evolution = new SchemaEvolution(orcReader.getSchema, readerOpts.getSchema, readerOpts)
    val dataReader = getDataReader(orcReader, readerOpts, filePath, fs, conf)
    val (sargApp, sargColumns) = getSearchApplier(orcReader, readerOpts, evolution,
      orcFileReaderOpts.getUseUTCTimestamp)
    val splitStripes = orcReader.getStripes.asScala.filter(s =>
      s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
    val stripes = buildOutputStripes(splitStripes, evolution,
      sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
      orcReader.getWriterVersion, dataReader)
    OrcPartitionReaderContext(updatedReadSchema, evolution, dataReader, orcReader,
      stripes.iterator.buffered)
  }

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (ctx.blockIterator.hasNext) {
      batch = readBatch()
    } else {
      metrics("peakDevMemory") += maxDeviceMemory
    }
    batch.isDefined
  }

  override def get(): ColumnarBatch = {
    val ret = batch.getOrElse(throw new NoSuchElementException)
    batch = None
    ret
  }

  override def close(): Unit = {
    batch.foreach(_.close())
    batch = None
    ctx.dataReader.close()
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val nvtxRange = new NvtxWithMetrics("ORC readBatch", NvtxColor.GREEN, metrics(TOTAL_TIME))
    try {
      val currentStripes = populateCurrentBlockChunk()
      if (readDataSchema.isEmpty) {
        // not reading any data, so return a degenerate ColumnarBatch with the row count
        val numRows = currentStripes.map(_.infoBuilder.getNumberOfRows).sum
        Some(new ColumnarBatch(Array.empty, numRows.toInt))
      } else {
        val table = readToTable(currentStripes)
        try {
          table.map(GpuColumnVector.from)
        } finally {
          table.foreach(_.close())
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  /**
    * Build an integer array that maps the original ORC file's column IDs
    * to column IDs in the memory file. Columns that are not present in
    * the memory file will have a mapping of -1.
    *
    * @param evolution ORC SchemaEvolution
    * @return column mapping array
    */
  private def columnRemap(evolution: SchemaEvolution): Array[Int] = {
    val fileIncluded = evolution.getFileIncluded
    if (fileIncluded != null) {
      val result = new Array[Int](fileIncluded.length)
      var nextOutputColumnId = 0
      fileIncluded.indices.foreach { i =>
        if (fileIncluded(i)) {
          result(i) = nextOutputColumnId
          nextOutputColumnId += 1
        } else {
          result(i) = -1
        }
      }
      result
    } else {
      (0 to evolution.getFileSchema.getMaximumId).toArray
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
    * @param dataReader ORC DataReader
    * @return output stripes descriptors
    */
  private def buildOutputStripes(
      stripes: Seq[StripeInformation],
      evolution: SchemaEvolution,
      sargApp: SargApplier,
      sargColumns: Array[Boolean],
      ignoreNonUtf8BloomFilter: Boolean,
      writerVersion: OrcFile.WriterVersion,
      dataReader: DataReader): Seq[OrcOutputStripe] = {
    val columnMapping = columnRemap(evolution)
    val result = new ArrayBuffer[OrcOutputStripe](stripes.length)
    stripes.foreach { stripe =>
      val stripeFooter = dataReader.readStripeFooter(stripe)
      val needStripe = if (sargApp != null) {
        // An ORC schema is a single struct type describing the schema fields
        val orcFileSchema = evolution.getFileType(0)
        val orcIndex = dataReader.readRowIndex(stripe, orcFileSchema, stripeFooter,
          ignoreNonUtf8BloomFilter, evolution.getFileIncluded, null, sargColumns,
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
        val wantKind = !GpuOrcPartitionReader.ORC_STREAM_KINDS_IGNORED.contains(stream.getKind)
        if (outputColumn >= 0 && wantKind) {
          // remap the column ID when copying the stream descriptor
          footerBuilder.addStreams(OrcProto.Stream.newBuilder(stream).setColumn(outputColumn).build)
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

  private def estimateRowCount(stripes: Seq[OrcOutputStripe]): Long =
    stripes.map(_.infoBuilder.getNumberOfRows).sum

  private def estimateOutputSize(stripes: Seq[OrcOutputStripe]): Long = {
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
    size += ctx.orcReader.getFileTail.getPostscript.getFooterLength
    size += ctx.orcReader.getFileTail.getPostscriptLength

    // and finally the single-byte postscript length at the end of the file
    size += 1

    // Add in a bit of fudging in case the whole file is being consumed and
    // our codec version isn't as efficient as the original writer's codec.
    size + 128 * 1024
  }

  private def copyStripeData(
      out: WritableByteChannel,
      inputDataRanges: DiskRangeList,
      dataReader: DataReader): Unit = {
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

  private def writeOrcOutputFile(
      rawOut: HostMemoryOutputStream,
      stripes: Seq[OrcOutputStripe]): Unit = {
    val outChannel = Channels.newChannel(rawOut)
    val outReceiver = new PhysicalWriter.OutputReceiver {
      override def output(buffer: ByteBuffer): Unit = outChannel.write(buffer)
      override def suppress(): Unit = throw new UnsupportedOperationException(
        "suppress should not be called")
    }

    // write ORC header
    val dataOut = new DataOutputStream(rawOut)
    dataOut.writeBytes(OrcFile.MAGIC)
    dataOut.flush()

    val codec = OrcCodecPool.getCodec(ctx.orcReader.getCompressionKind)
    try {

      // buffer size must be greater than zero or writes hang (ORC-381)
      val orcBufferSize = if (ctx.orcReader.getCompressionSize > 0) {
        ctx.orcReader.getCompressionSize
      } else {
        // note that this buffer is just for writing meta-data
        OrcConf.BUFFER_SIZE.getDefaultValue.asInstanceOf[Int]
      }

      val codecStream = new OutStream(getClass.getSimpleName, orcBufferSize,
        codec, outReceiver)
      val protoWriter = CodedOutputStream.newInstance(codecStream)
      var numRows = 0L
      val fileFooterBuilder = OrcProto.Footer.newBuilder

      // write the stripes
      stripes.foreach { stripe =>
        stripe.infoBuilder.setOffset(rawOut.getPos)
        copyStripeData(outChannel, stripe.inputDataRanges, ctx.dataReader)
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
          .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(ctx.evolution.getReaderSchema))
          .setNumberOfRows(numRows)
          .build()
      val footerStartOffset = rawOut.getPos
      footer.writeTo(protoWriter)
      protoWriter.flush()
      codecStream.flush()
      val postScriptStartOffset = rawOut.getPos

      // write the postscript (uncompressed)
      val postscript = OrcProto.PostScript.newBuilder(ctx.orcReader.getFileTail.getPostscript)
          .setFooterLength(postScriptStartOffset - footerStartOffset)
          .setMetadataLength(0)
          .build()
      postscript.writeTo(rawOut)
      val postScriptLength = rawOut.getPos - postScriptStartOffset
      if (postScriptLength > 255) {
        throw new IllegalArgumentException(s"PostScript length is too large at $postScriptLength")
      }
      rawOut.write(postScriptLength.toInt)
    } finally {
      OrcCodecPool.returnCodec(ctx.orcReader.getCompressionKind, codec)
    }
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
    * @param orcReader ORC input file reader
    * @param readerOpts ORC reader options
    * @param evolution ORC SchemaEvolution
    * @param useUTCTimestamp true if timestamps are UTC
    * @return the search argument applier and search argument column mapping
    */
  private def getSearchApplier(
      orcReader: Reader,
      readerOpts: Reader.Options,
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

  private def getDataReader(
      orcReader: Reader,
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
      //noinspection ScalaDeprecation
      RecordReaderUtils.createDefaultDataReader(DataReaderProperties.builder()
          .withBufferSize(orcReader.getCompressionSize)
          .withCompression(orcReader.getCompressionKind)
          .withFileSystem(fs)
          .withPath(filePath)
          .withTypeCount(orcReader.getTypes.size)
          .withZeroCopy(zeroCopy)
          .withMaxDiskRangeChunkLimit(maxDiskRangeChunkLimit)
          .build())
    }
  }

  private def readPartFile(stripes: Seq[OrcOutputStripe]): (HostMemoryBuffer, Long, Long) = {
    val nvtxRange = new NvtxWithMetrics("Buffer file split", NvtxColor.YELLOW,
      metrics("bufferTime"))
    try {
      if (stripes.isEmpty) {
        return (null, 0L, 0)
      }

      val hostBufferSize = estimateOutputSize(stripes)
      var succeeded = false
      val hmb = HostMemoryBuffer.allocate(hostBufferSize)
      try {
        val out = new HostMemoryOutputStream(hmb)
        writeOrcOutputFile(out, stripes)
        succeeded = true
        (hmb, out.getPos, estimateRowCount(stripes))
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
    } finally {
      nvtxRange.close()
    }
  }

  private def readToTable(stripes: Seq[OrcOutputStripe]): Option[Table] = {
    val (dataBuffer, dataSize, rowCount) = readPartFile(stripes)
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpOrcData(dataBuffer, dataSize)
        }
        val cudfSchema = GpuColumnVector.from(readDataSchema)
        val includedColumns = ctx.updatedReadSchema.getFieldNames.asScala
        val parseOpts = ORCOptions.builder()
          .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
          .withNumPyTypes(false)
          .includeColumn(includedColumns:_*)
          .withOutputSizeGuess(cudfSchema.guessTableSize(rowCount.toInt))
          .build()

        // about to start using the GPU
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val table = Table.readORC(parseOpts, dataBuffer, 0, dataSize)
        val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(table)
        logDebug(s"GPU batch size: $batchSizeBytes bytes")
        maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $partFile")
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

  private def populateCurrentBlockChunk(): Seq[OrcOutputStripe] = {
    val currentChunk = new ArrayBuffer[OrcOutputStripe]

    var numRows: Long = 0
    var numBytes: Long = 0
    var numOrcBytes: Long = 0

    @tailrec
    def readNextBatch(): Unit = {
      if (ctx.blockIterator.hasNext) {
        val peekedStripe = ctx.blockIterator.head
        if (peekedStripe.infoBuilder.getNumberOfRows > Integer.MAX_VALUE) {
          throw new UnsupportedOperationException("Too many rows in split")
        }
        if (numRows == 0 || numRows + peekedStripe.infoBuilder.getNumberOfRows <= maxReadBatchSizeRows) {
          val estimatedBytes = GpuBatchUtils.estimateGpuMemory(readDataSchema, peekedStripe.infoBuilder.getNumberOfRows)
          if (numBytes == 0 || numBytes + estimatedBytes <= maxReadBatchSizeBytes) {
            currentChunk += ctx.blockIterator.next()
            numRows += currentChunk.last.infoBuilder.getNumberOfRows
            numOrcBytes += currentChunk.last.infoBuilder.getDataLength
            numBytes += estimatedBytes
            readNextBatch()
          }
        }
      }
    }

    readNextBatch()

    logDebug(s"Loaded $numRows rows from Orc. Orc bytes read: $numOrcBytes. Estimated GPU bytes: $numBytes")

    currentChunk
  }

  private def dumpOrcData(hmb: HostMemoryBuffer, dataLength: Long): Unit = {
    val (out, path) = FileUtils.createTempFile(conf, debugDumpPrefix, ".orc")
    try {
      logInfo(s"Writing ORC split data for $partFile to $path")
      val in = new HostMemoryInputStream(hmb, dataLength)
      IOUtils.copy(in, out)
    } finally {
      out.close()
    }
  }
}
