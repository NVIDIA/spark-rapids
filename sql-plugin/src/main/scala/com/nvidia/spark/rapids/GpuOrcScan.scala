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
import java.util.concurrent.Callable

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
import com.nvidia.spark.rapids.shims.{OrcReadingShims, OrcShims, ShimFilePartitionReaderFactory}
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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.rapids.OrcFiltersWrapper
import org.apache.spark.sql.execution.datasources.v2.{EmptyPartitionReader, FileScan}
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
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, rapidsConf, metrics,
        options.asScala.toMap)
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

object GpuOrcScan extends Arm {
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

  private lazy val numericLevels = Seq(
    DType.DTypeEnum.BOOL8,
    DType.DTypeEnum.INT8,
    DType.DTypeEnum.INT16,
    DType.DTypeEnum.INT32,
    DType.DTypeEnum.INT64,
    DType.DTypeEnum.FLOAT32,
    DType.DTypeEnum.FLOAT64,
    DType.DTypeEnum.DECIMAL32,
    DType.DTypeEnum.DECIMAL64,
    DType.DTypeEnum.DECIMAL128
  ).zipWithIndex.toMap

  /**
   * Cast the input column to the target type, and replace overflow rows with nulls.
   * Only for conversion between integral types.
   */
  private def downCastAnyInteger(col: ColumnView, toType: DType): ColumnVector = {
    // Overflow happens in b when
    //    val b = (toDt)a
    //    val c = (fromDt)b
    //    c != a
    withResource(col.castTo(toType)) { casted =>
      val overflowFlags = withResource(casted.castTo(col.getType)) { backed =>
        col.equalTo(backed)
      }
      // Replace values that cause overflow with nulls, same with CPU ORC.
      withResource(overflowFlags) { _ =>
        casted.copyWithBooleanColumnAsValidity(overflowFlags)
      }
    }
  }

  /**
   * Get the overflow flags in booleans.
   * true means no overflow, while false means getting overflow.
   *
   * @param doubleMillis the input double column
   * @param millis the long column casted from the doubleMillis
   */
  private def getOverflowFlags(doubleMillis: ColumnView, millis: ColumnView): ColumnView = {
    // No overflow when
    //     doubleMillis <= Long.MAX_VALUE &&
    //     doubleMillis >= Long.MIN_VALUE &&
    //     ((millis >= 0) == (doubleMillis >= 0))
    val rangeCheck = withResource(Scalar.fromLong(Long.MaxValue)) { max =>
      withResource(doubleMillis.lessOrEqualTo(max)) { upperCheck =>
        withResource(Scalar.fromLong(Long.MinValue)) { min =>
          withResource(doubleMillis.greaterOrEqualTo(min)) { lowerCheck =>
            upperCheck.and(lowerCheck)
          }
        }
      }
    }
    withResource(rangeCheck) { _ =>
      val signCheck = withResource(Scalar.fromInt(0)) { zero =>
        withResource(millis.greaterOrEqualTo(zero)) { longSign =>
          withResource(doubleMillis.greaterOrEqualTo(zero)) { doubleSign =>
            longSign.equalTo(doubleSign)
          }
        }
      }
      withResource(signCheck) { _ =>
        rangeCheck.and(signCheck)
      }
    }
  }

  /**
   * Borrowed from ORC "ConvertTreeReaderFactory"
   * Scala does not support such numeric literal, so parse from string.
   */
  private val MIN_LONG_AS_DOUBLE = java.lang.Double.valueOf("-0x1p63")

  /**
   * We cannot store Long.MAX_VALUE as a double without losing precision. Instead, we store
   * Long.MAX_VALUE + 1 == -Long.MIN_VALUE, and then offset all comparisons by 1.
   */
  private val MAX_LONG_AS_DOUBLE_PLUS_ONE = java.lang.Double.valueOf("0x1p63")

  /**
   * Return a boolean column indicates whether the rows in col can fix in a long.
   * It assumes the input type is float or double.
   */
  private def doubleCanFitInLong(col: ColumnView): ColumnVector = {
    // It is true when
    //   (MIN_LONG_AS_DOUBLE - doubleValue < 1.0) &&
    //   (doubleValue < MAX_LONG_AS_DOUBLE_PLUS_ONE)
    val lowRet = withResource(Scalar.fromDouble(MIN_LONG_AS_DOUBLE)) { sMin =>
      withResource(Scalar.fromDouble(1.0)) { sOne =>
        withResource(sMin.sub(col)) { diff =>
          diff.lessThan(sOne)
        }
      }
    }
    withResource(lowRet) { _ =>
      withResource(Scalar.fromDouble(MAX_LONG_AS_DOUBLE_PLUS_ONE)) { sMax =>
        withResource(col.lessThan(sMax)) { highRet =>
          lowRet.and(highRet)
        }
      }
    }
  }


  /**
   * Cast the column to the target type for ORC schema evolution.
   * It is designed to support all the cases that `canCast` returns true.
   * Both of the column type and target type should be primitive.
   *
   * The returned column may be either the input or a new one, users should check and
   * close it when needed.
   */
  def castColumnTo(col: ColumnView, targetType: DataType): ColumnView = {
    val fromDt = col.getType
    val toDt = GpuColumnVector.getNonNestedRapidsType(targetType)
    if (fromDt == toDt) {
      return col
    }
    (fromDt, toDt) match {
      // integral to integral
      case (DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64,
          DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32 | DType.INT64) =>
        if (numericLevels(fromDt.getTypeId) <= numericLevels(toDt.getTypeId) ||
            toDt == DType.BOOL8) {
          // no downcast
          col.castTo(toDt)
        } else {
          downCastAnyInteger(col, toDt)
        }

      // float/double(float64) to {bool, integer types, double/float, string, timestamp}
      // float to bool/integral
      case (DType.FLOAT32 | DType.FLOAT64, DType.BOOL8 | DType.INT8 | DType.INT16 | DType.INT32
                                           | DType.INT64) =>
        // Follow the CPU ORC conversion:
        //   First replace rows that cannot fit in long with nulls,
        //   next convert to long,
        //   then down cast long to the target integral type.
        val longDoubles = withResource(doubleCanFitInLong(col)) { fitLongs =>
          col.copyWithBooleanColumnAsValidity(fitLongs)
        }
        withResource(longDoubles) { _ =>
          withResource(longDoubles.castTo(DType.INT64)) { longs =>
            toDt match {
              case DType.BOOL8 => longs.castTo(toDt)
              case DType.INT64 => longs.incRefCount()
              case _ => downCastAnyInteger(longs, toDt)
            }
          }
        }

      // float/double to double/float
      case (DType.FLOAT32 | DType.FLOAT64, DType.FLOAT32 | DType.FLOAT64) =>
        col.castTo(toDt)

      // FIXME float/double to string, there are some precision error issues
      case (DType.FLOAT32 | DType.FLOAT64, DType.STRING) =>
        GpuCast.castFloatingTypeToString(col)

      // float/double -> timestamp
      case (DType.FLOAT32 | DType.FLOAT64, DType.TIMESTAMP_MICROSECONDS) =>
        // Follow the CPU ORC conversion.
        //     val doubleMillis = doubleValue * 1000,
        //     val millis = Math.round(doubleMillis)
        //     if (noOverflow) millis else null
        val milliSeconds = withResource(Scalar.fromDouble(1000.0)) { thousand =>
          // ORC assumes value is in seconds, and returns timestamps in milliseconds.
          withResource(col.mul(thousand, DType.FLOAT64)) { doubleMillis =>
            withResource(doubleMillis.round()) { millis =>
              withResource(getOverflowFlags(doubleMillis, millis)) { overflows =>
                millis.copyWithBooleanColumnAsValidity(overflows)
              }
            }
          }
        }
        // Cast milli-seconds to micro-seconds
        // We need to pay attention that when convert (milliSeconds * 1000) to INT64, there may be
        // INT64-overflow, but we do not handle this issue here (as CPU code of ORC does).
        // If (milliSeconds * 1000) > INT64.MAX, then 'castTo' will throw an exception.
        withResource(milliSeconds) { _ =>
          withResource(milliSeconds.mul(Scalar.fromDouble(1000.0))) { microSeconds =>
            withResource(microSeconds.castTo(DType.INT64)) { longVec =>
              longVec.castTo(DType.TIMESTAMP_MICROSECONDS)
            }
          }
        }

      // TODO more types, tracked in https://github.com/NVIDIA/spark-rapids/issues/5895
      case (f, t) =>
        throw new QueryExecutionException(s"Unsupported type casting: $f -> $t")
    }
  }

  /**
   * Whether the type casting is supported by GPU ORC reading.
   *
   * No need to support the whole list that CPU does in "ConvertTreeReaderFactory.canConvert",
   * but the ones between GPU supported types.
   * Each supported casting is implemented in "castColumnTo".
   */
  def canCast(from: TypeDescription, to: TypeDescription): Boolean = {
    import org.apache.orc.TypeDescription.Category._
    if (!to.getCategory.isPrimitive || !from.getCategory.isPrimitive) {
      // Don't convert from any to complex, or from complex to any.
      // Align with what CPU does.
      return false
    }
    from.getCategory match {
      case BOOLEAN | BYTE | SHORT | INT | LONG =>
        to.getCategory match {
          case BOOLEAN | BYTE | SHORT | INT | LONG => true
          case _ => false
        }
      case VARCHAR =>
        to.getCategory == STRING

      case FLOAT | DOUBLE =>
        to.getCategory match {
          case BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | STRING | TIMESTAMP => true
          case _ => false
        }
      // TODO more types, tracked in https://github.com/NVIDIA/spark-rapids/issues/5895
      case _ =>
        false
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

  private val debugDumpPrefix = Option(rapidsConf.orcDebugDumpPrefix)
  private val numThreads = rapidsConf.multiThreadReadNumThreads
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
    metrics : Map[String, GpuMetric],
    @transient params: Map[String, String])
  extends ShimFilePartitionReaderFactory(params) with Arm {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val debugDumpPrefix = Option(rapidsConf.orcDebugDumpPrefix)
  private val maxReadBatchSizeRows: Integer = rapidsConf.maxReadBatchSizeRows
  private val maxReadBatchSizeBytes: Long = rapidsConf.maxReadBatchSizeBytes
  private val filterHandler = GpuOrcFileFilterHandler(sqlConf, broadcastedConf, pushedFilters)

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalStateException("GPU column parser called to read rows")
  }

  override def buildColumnarReader(partFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val ctx = filterHandler.filterStripes(partFile, dataSchema, readDataSchema,
      partitionSchema)
    if (ctx == null) {
      new EmptyPartitionReader[ColumnarBatch]
    } else {
      val conf = broadcastedConf.value.value
      OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE.setBoolean(conf, isCaseSensitive)
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
trait OrcCommonFunctions extends OrcCodecWritingHelper { self: FilePartitionReaderBase =>
  private val orcFormat = Some("orc")

  def debugDumpPrefix: Option[String]

  // The Spark schema describing what will be read
  def readDataSchema: StructType

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
      metrics.get(READ_FS_TIME).foreach(_.add(mid - start))
      metrics.get(WRITE_BUFFER_TIME).foreach(_.add(end - mid))
    }
  }

  /** Get the ORC schema corresponding to the file being constructed for the GPU */
  protected def buildReaderSchema(ctx: OrcPartitionReaderContext): TypeDescription =
    buildReaderSchema(ctx.updatedReadSchema, ctx.requestedMapping)

  protected def buildReaderSchema(
      updatedSchema: TypeDescription,
      requestedMapping: Option[Array[Int]]): TypeDescription = {
    requestedMapping.map { colIds =>
      // filter top-level schema based on requested mapping
      val filedNames = updatedSchema.getFieldNames
      val fieldTypes = updatedSchema.getChildren
      val resultSchema = TypeDescription.createStruct()
      colIds.filter(_ >= 0).foreach { colIdx =>
        resultSchema.addField(filedNames.get(colIdx), fieldTypes.get(colIdx).clone())
      }
      resultSchema
    }.getOrElse(updatedSchema)
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

  /**
   * Extracts all fields(columns) of DECIMAL128, including child columns of nested types,
   * and returns the names of all fields.
   * The names of nested children are prefixed with their parents' information, which is the
   * acceptable format of cuDF reader options.
   */
  private def filterDecimal128Fields(readColumns: Array[String],
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

  /**
   * Read the host data to GPU for ORC decoding, and return it as a cuDF Table.
   * The input host buffer should contain valid data, otherwise the behavior is
   * undefined.
   * 'splits' is used only for debugging.
   */
  protected def decodeToTable(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      memFileSchema: TypeDescription,
      requestedMapping: Option[Array[Int]],
      isCaseSensitive: Boolean,
      splits: Array[PartitionedFile]): Table = {
    // Dump ORC data into a file
    dumpDataToFile(hostBuf, bufSize, splits, debugDumpPrefix, orcFormat)

    val tableSchema = buildReaderSchema(memFileSchema, requestedMapping)
    val includedColumns = tableSchema.getFieldNames.asScala
    val decimal128Fields = filterDecimal128Fields(includedColumns.toArray, readDataSchema)
    val parseOpts = ORCOptions.builder()
      .withTimeUnit(DType.TIMESTAMP_MICROSECONDS)
      .withNumPyTypes(false)
      .includeColumn(includedColumns: _*)
      .decimal128Column(decimal128Fields: _*)
      .build()

    // about to start using the GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get(), metrics(SEMAPHORE_WAIT_TIME))

    val table = withResource(new NvtxWithMetrics("ORC decode", NvtxColor.DARK_GREEN,
        metrics(GPU_DECODE_TIME))) { _ =>
      Table.readORC(parseOpts, hostBuf, 0, bufSize)
    }
    // Execute the schema evolution
    SchemaUtils.evolveSchemaIfNeededAndClose(table, tableSchema, readDataSchema,
      isCaseSensitive, Some(GpuOrcScan.castColumnTo))
  }
}

/**
 * A base ORC partition reader which compose of some common methods
 */
trait OrcPartitionReaderBase extends OrcCommonFunctions with Logging
  with Arm with ScanWithMetrics { self: FilePartitionReaderBase =>

  /**
   * Send a host buffer to GPU for ORC decoding, and return it as a ColumnarBatch.
   * The input hostBuf will be closed after returning, please do not use it anymore.
   * 'splits' is used only for debugging.
   */
  protected final def decodeToBatch(
      hostBuf: HostMemoryBuffer,
      bufSize: Long,
      memFileSchema: TypeDescription,
      requestedMapping: Option[Array[Int]],
      isCaseSensitive: Boolean,
      splits: Array[PartitionedFile]): Option[ColumnarBatch] = {
    withResource(hostBuf) { _ =>
      if (bufSize == 0) {
        None
      } else {
        withResource(decodeToTable(hostBuf, bufSize, memFileSchema, requestedMapping,
            isCaseSensitive, splits)) { t =>
          val batchSizeBytes = GpuColumnVector.getTotalDeviceMemoryUsed(t)
          logDebug(s"GPU batch size: $batchSizeBytes bytes")
          maxDeviceMemory = max(batchSizeBytes, maxDeviceMemory)
          metrics(NUM_OUTPUT_BATCHES) += 1
          // convert to batch
          Some(GpuColumnVector.from(t, GpuColumnVector.extractTypes(readDataSchema)))
        }
      } // end of else
    }
  }

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
    override val debugDumpPrefix: Option[String],
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics : Map[String, GpuMetric],
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
        val (dataBuffer, dataSize) = readPartFile(ctx, currentStripes)
        decodeToBatch(dataBuffer, dataSize, ctx.updatedReadSchema, ctx.requestedMapping,
          isCaseSensitive, Array(partFile))
      }
    } // end of withResource(new NvtxRange)
  } // end of readBatch

}

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
    val resultedColPruneInfo = OrcReadingShims.requestedColumnIds(isCaseSensitive, dataSchema,
        readDataSchema, orcReader, conf)
      if (resultedColPruneInfo.isEmpty) {
        // Be careful when the OrcPartitionReaderContext is null, we should change
        // reader to EmptyPartitionReader for throwing exception
        null
      } else {
        val (requestedColIds, canPruneCols) = resultedColPruneInfo.get
        OrcUtils.orcResultSchemaString(canPruneCols, dataSchema, readDataSchema,
          partitionSchema, conf)
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

      val (updatedReadSchema, fileIncluded) = checkSchemaCompatibility(orcReader.getSchema,
        readerOpts.getSchema, isCaseSensitive)
      // GPU has its own read schema, so unset the reader include to read all the columns
      // specified by its read schema.
      readerOpts.include(null)
      val evolution = new SchemaEvolution(orcReader.getSchema, updatedReadSchema, readerOpts)
      val (sargApp, sargColumns) = getSearchApplier(evolution,
        orcFileReaderOpts.getUseUTCTimestamp,
        orcReader.writerUsedProlepticGregorian(), orcFileReaderOpts.getConvertToProlepticGregorian)

      val splitStripes = orcReader.getStripes.asScala.filter( s =>
        s.getOffset >= partFile.start && s.getOffset < partFile.start + partFile.length)
      val stripes = buildOutputStripes(splitStripes, evolution,
        sargApp, sargColumns, OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(conf),
        orcReader.getWriterVersion, updatedReadSchema,
        resolveMemFileIncluded(fileIncluded, requestedMapping))
      OrcPartitionReaderContext(filePath, conf, orcReader.getSchema, updatedReadSchema, evolution,
        orcReader.getFileTail, orcReader.getCompressionSize, orcReader.getCompressionKind,
        readerOpts, stripes.iterator.buffered, requestedMapping)
    }

    /**
     * Compute an array of booleans, one for each column in the ORC file, indicating whether the
     * corresponding ORC column ID should be included in the file to be loaded by the GPU.
     *
     * @return per-column inclusion flags
     */
    protected def resolveMemFileIncluded(
        fileIncluded: Array[Boolean],
        requestedMapping: Option[Array[Int]]): Array[Boolean] = {
      requestedMapping.map { mappings =>
        // filter top-level schema based on requested mapping
        val orcFileSchema = orcReader.getSchema
        val orcSchemaChildren = orcFileSchema.getChildren
        val resultIncluded = new Array[Boolean](orcFileSchema.getMaximumId + 1)
        // first column is the top-level schema struct, always add it
        resultIncluded(0) = true
        mappings.foreach { orcColIdx =>
          if (orcColIdx >= 0) {
            // find each top-level column requested by top-level index and add it and
            // all child columns
            val fieldType = orcSchemaChildren.get(orcColIdx)
            (fieldType.getId to fieldType.getMaximumId).foreach { i =>
              resultIncluded(i) = true
            }
          }
        }
        resultIncluded
      }.getOrElse(fileIncluded)
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
        fileIncluded: Array[Boolean]): Seq[OrcOutputStripe] = {
      val columnMapping = columnRemap(fileIncluded)
      OrcShims.filterStripes(stripes, conf, orcReader, dataReader,
        buildOutputStripe, evolution,
        sargApp, sargColumns, ignoreNonUtf8BloomFilter,
        writerVersion, fileIncluded, columnMapping)
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
     * Check if the read schema is compatible with the file schema. Meanwhile, recursively
     * prune all incompatible required fields in terms of both ORC schema and file include
     * status.
     *
     * Only do the check for columns that can be found in the file schema, either by index
     * or column name. And the missing ones are ignored, since a null column will be added
     * in the final output for each of them.
     *
     * It takes care of both the top and nested columns.
     *
     * @param fileSchema  input file's ORC schema
     * @param readSchema  ORC schema for what will be read
     * @param isCaseAware true if field names are case-sensitive
     * @return A tuple contains the pruned read schema and the updated file include status.
     */
    private def checkSchemaCompatibility(
        fileSchema: TypeDescription,
        readSchema: TypeDescription,
        isCaseAware: Boolean): (TypeDescription, Array[Boolean]) = {
      // all default to false
      val fileIncluded = new Array[Boolean](fileSchema.getMaximumId + 1)
      val isForcePos = OrcShims.forcePositionalEvolution(conf)
      (checkTypeCompatibility(fileSchema, readSchema, isCaseAware, fileIncluded, isForcePos),
        fileIncluded)
    }

    /**
     * Check if the file type is compatible with the read type.
     * Return the file type (Will be pruned for struct type) if the check result is positive,
     * otherwise blows up.
     */
    private def checkTypeCompatibility(
        fileType: TypeDescription,
        readType: TypeDescription,
        isCaseAware: Boolean,
        fileIncluded: Array[Boolean],
        isForcePos: Boolean): TypeDescription = {
      (fileType.getCategory, readType.getCategory) match {
        case (TypeDescription.Category.STRUCT, TypeDescription.Category.STRUCT) =>
          // Check for the top or nested struct types.
          val readFieldNames = readType.getFieldNames.asScala
          val readField2Type = readFieldNames.zip(readType.getChildren.asScala)
          val getReadFieldType: (String, Int) => Option[(String, TypeDescription)] =
            if (isForcePos) {
              // Match the top level columns using position rather than column names.
              (_, fileFieldIdx) => readField2Type.lift(fileFieldIdx)
            } else {
              // match by column names
              val caseSensitiveReadTypes = readFieldNames.zip(readField2Type).toMap
              val readTypesMap = if (isCaseAware) {
                caseSensitiveReadTypes
              } else {
                CaseInsensitiveMap[(String, TypeDescription)](caseSensitiveReadTypes)
              }
              (fileFieldName, _) => readTypesMap.get(fileFieldName)
            }
          // buffer to cache the result schema
          val prunedReadSchema = TypeDescription.createStruct()

          fileType.getFieldNames.asScala
            .zip(fileType.getChildren.asScala)
            .zipWithIndex.foreach { case ((fileFieldName, fType), idx) =>
            getReadFieldType(fileFieldName, idx).foreach { case (rField, rType) =>
              val newChild = checkTypeCompatibility(fType, rType,
                isCaseAware, fileIncluded, isForcePos)
              prunedReadSchema.addField(rField, newChild)
            }
          }
          fileIncluded(fileType.getId) = true
          prunedReadSchema
        // Go into children for LIST, MAP to filter out the missing names
        // for struct children.
        case (TypeDescription.Category.LIST, TypeDescription.Category.LIST) =>
          val newChild = checkTypeCompatibility(fileType.getChildren.get(0),
            readType.getChildren.get(0), isCaseAware, fileIncluded, isForcePos)
          fileIncluded(fileType.getId) = true
          TypeDescription.createList(newChild)
        case (TypeDescription.Category.MAP, TypeDescription.Category.MAP) =>
          val newKey = checkTypeCompatibility(fileType.getChildren.get(0),
            readType.getChildren.get(0), isCaseAware, fileIncluded, isForcePos)
          val newValue = checkTypeCompatibility(fileType.getChildren.get(1),
            readType.getChildren.get(1), isCaseAware, fileIncluded, isForcePos)
          fileIncluded(fileType.getId) = true
          TypeDescription.createMap(newKey, newValue)
        case (ft, rt) if ft.isPrimitive && rt.isPrimitive =>
          if (OrcShims.typeDescriptionEqual(fileType, readType) ||
            GpuOrcScan.canCast(fileType, readType)) {
            // Since type casting is supported, here should return the file type.
            fileIncluded(fileType.getId) = true
            fileType.clone()
          } else {
            throw new QueryExecutionException("GPU ORC does not support type conversion" +
              s" from file type $fileType (${fileType.getId}) to" +
              s" reader type $readType (${readType.getId})")
          }
        case (f, r) =>
          // e.g. Union type is not supported yet
          throw new QueryExecutionException("Unsupported type pair of " +
            s"(file type, read type)=($f, $r)")
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
    override val debugDumpPrefix: Option[String],
    filters: Array[Filter],
    filterHandler: GpuOrcFileFilterHandler,
    execMetrics: Map[String, GpuMetric],
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
        val nextBatch = addPartitionValues(
          decodeToBatch(hostBuffer, size, buffer.updatedReadSchema, buffer.requestedMapping,
            filterHandler.isCaseSensitive, files),
          buffer.partitionedFile.partitionValues, partitionSchema)
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
    override val readDataSchema: StructType,
    override val debugDumpPrefix: Option[String],
    maxReadBatchSizeRows: Integer,
    maxReadBatchSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
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
      extraInfo: ExtraInfo): Table =
    decodeToTable(dataBuffer, dataSize, clippedSchema, extraInfo.requestedMapping,
      isCaseSensitive, files)

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
