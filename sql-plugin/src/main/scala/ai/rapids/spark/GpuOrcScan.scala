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

import java.io.DataOutputStream
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, HostMemoryBuffer, ORCOptions, Table, TimeUnit}
import ai.rapids.spark.GpuOrcPartitionReader.OrcOutputStripe
import com.google.protobuf25.CodedOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.orc.{CompressionKind, DataReader, OrcConf, OrcFile, OrcProto, PhysicalWriter, Reader, StripeInformation, TypeDescription}
import org.apache.orc.impl.{BufferChunk, DataReaderProperties, OrcCodecPool, OutStream, RecordReaderImpl, RecordReaderUtils, SchemaEvolution}
import org.apache.orc.impl.RecordReaderImpl.SargApplier
import org.apache.orc.mapred.OrcInputFormat
import org.apache.orc.storage.common.io.DiskRangeList

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuOrcScan(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    rapidsConf: RapidsConf)
  extends OrcScan(sparkSession, hadoopConf, fileIndex, dataSchema,
    readDataSchema, readPartitionSchema, options, pushedFilters) with GpuScan {

  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new GpuSerializableConfiguration(hadoopConf))
    GpuOrcPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, rapidsConf)
  }
}

object GpuOrcScan {
  def assertCanSupport(scan: OrcScan): Unit = {
    val schema = StructType(scan.readDataSchema ++ scan.readPartitionSchema)
    schema.foreach { field =>
      if (!GpuColumnVector.isSupportedType(field.dataType)) {
        throw new CannotReplaceException(s"GpuOrcScan does not support fields of type ${field.dataType}")
      }
    }
  }
}

case class GpuOrcPartitionReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[GpuSerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf) extends FilePartitionReaderFactory {
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = rapidsConf.orcDebugDumpPrefix

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf = broadcastedConf.value.value
    val orcSchemaString = OrcUtils.orcTypeDescriptionString(readDataSchema)
    OrcConf.MAPRED_INPUT_SCHEMA.setString(conf, orcSchemaString)
    OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)

    val reader = new GpuOrcPartitionReader(conf, partFile, dataSchema, readDataSchema,
      debugDumpPrefix)
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
    debugDumpPrefix: String) extends PartitionReader[ColumnarBatch] with Logging {
  private var batch: Option[ColumnarBatch] = None
  private var isExhausted: Boolean = false

  override def next(): Boolean = {
    batch.foreach(_.close())
    batch = None
    if (!isExhausted) {
      // We only support a single batch
      isExhausted = true
      batch = readBatch()
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
    isExhausted = true
  }

  private def readBatch(): Option[ColumnarBatch] = {
    val filePath = new Path(new URI(partFile.filePath))
    val fs = filePath.getFileSystem(conf)
    val orcFileReaderOpts = OrcFile.readerOptions(conf).filesystem(fs)
    val orcReader = OrcFile.createReader(filePath, orcFileReaderOpts)
    val splitStripes = orcReader.getStripes.asScala.filter(s =>
      s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)

    if (readDataSchema.isEmpty) {
      // not reading any data, so return a degenerate ColumnarBatch with the row count
      val numRows = splitStripes.map(_.getNumberOfRows).sum
      if (numRows > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(s"Too many rows in split $partFile")
      }
      Some(new ColumnarBatch(Array.empty, numRows.toInt))
    } else {
      val table = readToTable(filePath, fs, orcReader, splitStripes,
        orcFileReaderOpts.getUseUTCTimestamp)
      try {
        table.map(GpuColumnVector.from)
      } finally {
        table.foreach(_.close())
      }
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

  private def estimateOutputSize(
      stripes: Seq[GpuOrcPartitionReader.OrcOutputStripe],
      inputTail: OrcProto.FileTail): Long = {
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
    size += inputTail.getPostscript.getFooterLength
    size += inputTail.getPostscriptLength

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
      schema: TypeDescription,
      stripes: Seq[GpuOrcPartitionReader.OrcOutputStripe],
      compression: CompressionKind,
      bufferSize: Int,
      inputPostScript: OrcProto.PostScript,
      dataReader: DataReader): Unit = {
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

    val codec = OrcCodecPool.getCodec(compression)
    try {
      val codecStream = new OutStream(getClass.getSimpleName, bufferSize, codec, outReceiver)
      val protoWriter = CodedOutputStream.newInstance(codecStream)
      var numRows = 0L
      val fileFooterBuilder = OrcProto.Footer.newBuilder

      // write the stripes
      stripes.foreach { stripe =>
        stripe.infoBuilder.setOffset(rawOut.getPos)
        copyStripeData(outChannel, stripe.inputDataRanges, dataReader)
        val stripeFooterStartOffset = rawOut.getPos
        stripe.footer.writeTo(protoWriter)
        protoWriter.flush()
        codecStream.flush()
        stripe.infoBuilder.setFooterLength(rawOut.getPos - stripeFooterStartOffset)
        fileFooterBuilder.addStripes(stripe.infoBuilder.build())
        numRows += stripe.infoBuilder.getNumberOfRows
      }

      if (numRows > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException(s"Too many rows in split $partFile")
      }

      // write the footer
      val footer = fileFooterBuilder.setHeaderLength(OrcFile.MAGIC.length)
          .setContentLength(rawOut.getPos)
          .addAllTypes(org.apache.orc.OrcUtils.getOrcTypes(schema))
          .setNumberOfRows(numRows)
          .build()
      val footerStartOffset = rawOut.getPos
      footer.writeTo(protoWriter)
      protoWriter.flush()
      codecStream.flush()
      val postScriptStartOffset = rawOut.getPos

      // write the postscript (uncompressed)
      val postscript = OrcProto.PostScript.newBuilder(inputPostScript)
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
      OrcCodecPool.returnCodec(compression, codec)
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

  private def readPartFile(
      filePath: Path,
      fs: FileSystem,
      orcReader: Reader,
      readerOpts: Reader.Options,
      splitStripes: Seq[StripeInformation],
      useUTCTimestamp: Boolean): (HostMemoryBuffer, Long) = {
    val evolution = new SchemaEvolution(orcReader.getSchema, readerOpts.getSchema, readerOpts)
    val (sargApp, sargColumns) = getSearchApplier(orcReader, readerOpts, evolution, useUTCTimestamp)
    val dataReader = getDataReader(orcReader, readerOpts, filePath, fs, conf)
    try {
      val stripes = buildOutputStripes(splitStripes, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion, dataReader)
      if (stripes.isEmpty) {
        return (null, 0L)
      }
      val hostBufferSize = estimateOutputSize(stripes, orcReader.getFileTail)

      var succeeded = false
      val hmb = HostMemoryBuffer.allocate(hostBufferSize)
      try {
        val out = new HostMemoryOutputStream(hmb)
        writeOrcOutputFile(out, evolution.getReaderSchema, stripes, orcReader.getCompressionKind,
          orcReader.getCompressionSize, orcReader.getFileTail.getPostscript, dataReader)
        succeeded = true
        (hmb, out.getPos)
      } finally {
        if (!succeeded) {
          hmb.close()
        }
      }
    } finally {
      dataReader.close()
    }
  }

  private def readToTable(
      filePath: Path,
      fs: FileSystem,
      orcReader: Reader,
      splitStripes: Seq[StripeInformation],
      useUTCTimestamp: Boolean): Option[Table] = {
    val readerOpts = OrcInputFormat.buildOptions(conf, orcReader, partFile.start, partFile.length)
    val updatedReadSchema = checkSchemaCompatibility(orcReader.getSchema, readerOpts.getSchema,
      readerOpts.getIsSchemaEvolutionCaseAware)

    val (dataBuffer, dataSize) = readPartFile(filePath, fs, orcReader, readerOpts,
      splitStripes, useUTCTimestamp)
    try {
      if (dataSize == 0) {
        None
      } else {
        if (debugDumpPrefix != null) {
          dumpOrcData(dataBuffer, dataSize)
        }
        val includedColumns = updatedReadSchema.getFieldNames.asScala
        val parseOpts = ORCOptions.builder().includeColumn(includedColumns:_*).build()
        val table = Table.readORC(parseOpts, dataBuffer, 0, dataSize)
        val numColumns = table.getNumberOfColumns
        if (readDataSchema.length != numColumns) {
          table.close()
          throw new QueryExecutionException(s"Expected ${readDataSchema.length} columns " +
              s"but read ${table.getNumberOfColumns} from $partFile")
        }
        Some(handleDate64Casts(table))
      }
    } finally {
      if (dataBuffer != null) {
        dataBuffer.close()
      }
    }
  }

  // The GPU ORC reader always casts date and timestamp columns to DATE64.
  // See https://github.com/rapidsai/cudf/issues/2384.
  // Cast DATE64 columns back to either DATE32 or TIMESTAMP based on the read schema
  private def handleDate64Casts(table: Table): Table = {
    var columns: ArrayBuffer[ColumnVector] = null
    // If we have to create a new column from the cast, we need to close it after adding it to the
    // table, which will increment its reference count
    var toClose = new ArrayBuffer[ColumnVector]()
    try {
      for (i <- 0 until table.getNumberOfColumns) {
        val column = table.getColumn(i)
        if (column.getType == DType.DATE64) {
          if (columns == null) {
            columns = (0 until table.getNumberOfColumns).map(table.getColumn).to[ArrayBuffer]
          }
          val rapidsType = GpuColumnVector.getRapidsType(readDataSchema.fields(i).dataType)
          columns(i) = columns(i).castTo(rapidsType, TimeUnit.MICROSECONDS)
          toClose += columns(i)
        }
      }

      var result = table
      if (columns != null) {
        try {
          result = new Table(columns: _*)
        } finally {
          table.close()
        }
      }

      result
    } finally {
      toClose.foreach(_.close())
    }
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
