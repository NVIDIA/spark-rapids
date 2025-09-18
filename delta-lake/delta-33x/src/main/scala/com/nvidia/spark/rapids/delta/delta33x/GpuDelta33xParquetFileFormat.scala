/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta33x

import ai.rapids.cudf._
import ai.rapids.cudf.HostColumnVector._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.deletionvectors._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ByteType, DataType, MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

case class GpuDelta33xParquetFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false
  ) extends GpuDeltaParquetFileFormat {

  // Validate either we have all arguments for DV enabled read or none of them.
  if (hasTablePath) {
    SparkSession.getActiveSession.map { session =>
      val useMetadataRowIndex =
        session.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
      require(useMetadataRowIndex == optimizationsEnabled,
        "Wrong arguments for Delta table scan with deletion vectors")
    }
  }

  if (SparkSession.getActiveSession.isDefined) {
    val session = SparkSession.getActiveSession.get
    TypeWidening.assertTableReadable(session.sessionState.conf, protocol, metadata)
  }

  val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  /**
   * This function is overridden as Delta 3.3 has an extra `PARQUET_FIELD_NESTED_IDS_METADATA_KEY`
   * key to remove from the metadata, which does not exist in earlier versions.
   */
  override def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else schema
  }

  /**
   * Helper method copied from Apache Spark
   * sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Implicits.scala
   */
  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  /**
   * Prepares filters so that they can be pushed down into the Parquet reader.
   *
   * If column mapping is enabled, then logical column names in the filters will be replaced with
   * their corresponding physical column names. This is necessary as the Parquet files will use
   * physical column names, and the requested schema pushed down in the Parquet reader will also use
   * physical column names.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (!optimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(quoteIfNeeded).mkString("."),
            physicalName.map(quoteIfNeeded).mkString("."))
          }
      filters.flatMap(translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(sparkSession: SparkSession,
     options: Map[String, String],
     path: Path): Boolean = optimizationsEnabled

  def hasTablePath: Boolean = tablePath.isDefined

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {

    // We don't want to use metadata to generate Row Indices as it will also
    // generate hidden metadata that we currently can't handle.
    // For details see https://github.com/NVIDIA/spark-rapids/issues/7458

    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)

    val schemaWithIndices = requiredSchema.fields.zipWithIndex
    def findColumn(name: String): Option[ColumnMetadata] = {
      val results = schemaWithIndices.filter(_._1.name == name)
      if (results.length > 1) {
        throw new IllegalArgumentException(
          s"There are more than one column with name=`$name` requested in the reader output")
      }
      results.headOption.map(e => ColumnMetadata(e._2, e._1))
    }

    val isRowDeletedColumn = findColumn(IS_ROW_DELETED_COLUMN_NAME)
    val rowIndexColumnName = ROW_INDEX_COLUMN_NAME

    val rowIndexColumn = findColumn(rowIndexColumnName)

    // We don't have any additional columns to generate, just return the original reader as is.
    if (isRowDeletedColumn.isEmpty && rowIndexColumn.isEmpty) return dataReader

    if (isRowDeletedColumn.isEmpty) return dataReader

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    // create an iterator with deletion vectors
    (file: PartitionedFile) => {
      val iter = dataReader(file)
      iteratorWithAdditionalMetadataColumns(
        file,
        iter,
        isRowDeletedColumn,
        rowIndexColumn,
        tablePath,
        serializableHadoopConf,
        metrics).asInstanceOf[Iterator[InternalRow]]
    }
  }

  /**
   * Translates the filter to use physical column names instead of logical column names.
   * This is needed when the column mapping mode is set to `NameMapping` or `IdMapping`
   * to match the requested schema that's passed to the [[ParquetFileFormat]].
   */
  private def translateFilterForColumnMapping(
     filter: Filter,
     physicalNameMap: Map[String, String]): Option[Filter] = {
    object PhysicalAttribute {
      def unapply(attribute: String): Option[String] = {
        physicalNameMap.get(attribute)
      }
    }

    filter match {
      case EqualTo(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualTo(physicalAttribute, value))
      case EqualNullSafe(PhysicalAttribute(physicalAttribute), value) =>
        Some(EqualNullSafe(physicalAttribute, value))
      case GreaterThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThan(physicalAttribute, value))
      case GreaterThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(GreaterThanOrEqual(physicalAttribute, value))
      case LessThan(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThan(physicalAttribute, value))
      case LessThanOrEqual(PhysicalAttribute(physicalAttribute), value) =>
        Some(LessThanOrEqual(physicalAttribute, value))
      case In(PhysicalAttribute(physicalAttribute), values) =>
        Some(In(physicalAttribute, values))
      case IsNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNull(physicalAttribute))
      case IsNotNull(PhysicalAttribute(physicalAttribute)) =>
        Some(IsNotNull(physicalAttribute))
      case And(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case (Some(l), None) => Some(l)
          case (_, _) => newRight
        }
      case Or(left, right) =>
        val newLeft = translateFilterForColumnMapping(left, physicalNameMap)
        val newRight = translateFilterForColumnMapping(right, physicalNameMap)
        (newLeft, newRight) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case (_, _) => None
        }
      case Not(child) =>
        translateFilterForColumnMapping(child, physicalNameMap).map(Not)
      case StringStartsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringStartsWith(physicalAttribute, value))
      case StringEndsWith(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringEndsWith(physicalAttribute, value))
      case StringContains(PhysicalAttribute(physicalAttribute), value) =>
        Some(StringContains(physicalAttribute, value))
      case AlwaysTrue() => Some(AlwaysTrue())
      case AlwaysFalse() => Some(AlwaysFalse())
      case _ =>
        logError(s"Failed to translate filter ${MDC(DeltaLogKeys.FILTER, filter)}")
        None
    }
  }

  /**
   * Perfile
   **/
  private def iteratorWithAdditionalMetadataColumns(
    partitionedFile: PartitionedFile,
    iterator: Iterator[Any],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexColumnOpt: Option[ColumnMetadata],
    tablePath: Option[String],
    serializableConf: SerializableConfiguration,
    metrics: Map[String, GpuMetric]): Iterator[Any] = {

    val rowIndexFilterOpt =
      getRowIndexFilter(partitionedFile, isRowDeletedColumnOpt, serializableConf, tablePath)

    var rowIndex = 0L

    iterator.map {
      case cb: ColumnarBatch =>
        val size = cb.numRows()
        val newBatch = replaceBatch(serializableConf.value, rowIndex, cb, size,
          rowIndexColumnOpt, isRowDeletedColumnOpt,
          rowIndexFilterOpt, partitionedFile, metrics)
        rowIndex += size
        newBatch

      case other =>
        throw new RuntimeException("Parquet reader returned an unknown row type: " +
          s"${other.getClass.getName}")
    }
  }

  private def getRowIndexPosSimple(size: Long): RapidsHostColumnVector = {
    withResource(new RapidsHostColumnBuilder(new BasicType(false, DType.INT64), size)) {
      rowIndexVectorBuilder =>
        // populate the row index column value.
        var i = 0L
        while (i < size) {
          rowIndexVectorBuilder.append(i)
          i += 1
        }
        new RapidsHostColumnVector(org.apache.spark.sql.types.LongType, rowIndexVectorBuilder.build())
    }
  }

  private def replaceVectors(
    batch: ColumnarBatch,
    indexVectorTuples: (Int, org.apache.spark.sql.vectorized.ColumnVector) *): ColumnarBatch = {
    val vectors = ArrayBuffer[org.apache.spark.sql.vectorized.ColumnVector]()
    for (i <- 0 until batch.numCols()) {
      var replaced: Boolean = false
      for (indexVectorTuple <- indexVectorTuples) {
        val (index, vector) = indexVectorTuple
        if (index == i) {
          vectors += vector
          // Make sure to close the existing vector allocated in the Parquet
          batch.column(i).close()
          replaced = true
        }
      }
      if (!replaced) {
        vectors += batch.column(i)
      }
    }
    new ColumnarBatch(vectors.toArray, batch.numRows())
  }

  private def getRowIndexFilter(partitionedFile: PartitionedFile,
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    serializableHadoopConf: SerializableConfiguration,
    tablePath: Option[String]): Option[RowIndexFilter] = {
    isRowDeletedColumnOpt.map { col =>
      // Fetch the DV descriptor from the partitioned file and create a row index filter
      val dvDescriptorOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_ID_ENCODED)
      val filterTypeOpt = partitionedFile.otherConstantMetadataColumnValues
        .get(FILE_ROW_INDEX_FILTER_TYPE)
      if (dvDescriptorOpt.isDefined && filterTypeOpt.isDefined) {
        val rowIndexFilter = filterTypeOpt.get match {
          case RowIndexFilterType.IF_CONTAINED => DropMarkedRowsFilter
          case RowIndexFilterType.IF_NOT_CONTAINED => KeepMarkedRowsFilter
          case unexpectedFilterType => throw new IllegalStateException(
            s"Unexpected row index filter type: ${unexpectedFilterType}")
        }
        rowIndexFilter.createInstance(
          DeletionVectorDescriptor.deserializeFromBase64(dvDescriptorOpt.get.asInstanceOf[String]),
          serializableHadoopConf.value,
          tablePath.map(new Path(_)))
      } else if (dvDescriptorOpt.isDefined || filterTypeOpt.isDefined) {
        throw new IllegalStateException(
          s"Both ${FILE_ROW_INDEX_FILTER_ID_ENCODED} and ${FILE_ROW_INDEX_FILTER_TYPE} " +
            "should either both have values or no values at all.")
      } else {
        KeepAllRowsFilter
      }
    }
  }

  /**
   * This class is intended to be used only with the method materializeIntoVectorWithRowIndex
   * this is to avoid copying memory from OffHeapColumnVector to RapidsHostColumnVector before
   * copying to the device
   */
  case class SkipRowRapidsHostWriteableVector(
     builder: RapidsHostColumnBuilder,
     size: Int,
     sparkDataType: DataType) extends WritableColumnVector(size, sparkDataType) {

    def putByte(rowId: Int, value: Byte): Unit = {
      // We are ignoring the rowId as we only use this method to materialize the ColumnVector which
      // adds the bytes sequentially.
      builder.append(value)
    }

    override def getArrayLength(x$1: Int): Int = throw new UnsupportedOperationException()

    override def getArrayOffset(x$1: Int): Int = throw new UnsupportedOperationException()

    override def getByteBuffer(x$1: Int, x$2: Int): java.nio.ByteBuffer = {
      throw new UnsupportedOperationException()
    }

    override def getBytesAsUTF8String(x$1: Int, x$2: Int): UTF8String = {
      throw new UnsupportedOperationException()
    }

    override def getDictId(x$1: Int): Int = throw new UnsupportedOperationException()

    override def putArray(x$1: Int, x$2: Int, x$3: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putBoolean(x$1: Int, x$2: Boolean): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putBooleans(x$1: Int, x$2: Byte): Unit = throw new UnsupportedOperationException()

    override def putBooleans(x$1: Int, x$2: Int, x$3: Boolean): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putByteArray(x$1: Int, x$2: Array[Byte], x$3: Int, x$4: Int): Int = {
      throw new UnsupportedOperationException()
    }

    override def putBytes(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putBytes(x$1: Int, x$2: Int, x$3: Byte): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putDouble(x$1: Int, x$2: Double): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putDoubles(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putDoubles(x$1: Int, x$2: Int, x$3: Array[Double], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putDoubles(x$1: Int, x$2: Int, x$3: Double): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putDoublesLittleEndian(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putFloat(x$1: Int, x$2: Float): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putFloats(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putFloats(x$1: Int, x$2: Int, x$3: Array[Float], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putFloats(x$1: Int, x$2: Int, x$3: Float): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putFloatsLittleEndian(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putInt(x$1: Int, x$2: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putInts(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putInts(x$1: Int, x$2: Int, x$3: Array[Int], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putInts(x$1: Int, x$2: Int, x$3: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putIntsLittleEndian(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putLong(x$1: Int, x$2: Long): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putLongs(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putLongs(x$1: Int, x$2: Int, x$3: Array[Long], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putLongs(x$1: Int, x$2: Int, x$3: Long): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putLongsLittleEndian(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putNotNull(x$1: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putNotNulls(x$1: Int, x$2: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putNull(x$1: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putNulls(x$1: Int, x$2: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putShort(x$1: Int, x$2: Short): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putShorts(x$1: Int, x$2: Int, x$3: Array[Byte], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putShorts(x$1: Int, x$2: Int, x$3: Array[Short], x$4: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def putShorts(x$1: Int, x$2: Int, x$3: Short): Unit = {
      throw new UnsupportedOperationException()
    }

    override def reserveInternal(x$1: Int): Unit = {
      throw new UnsupportedOperationException()
    }

    override def reserveNewColumn(
      x$1: Int,
      x$2: org.apache.spark.sql.types.DataType): WritableColumnVector = {
      throw new UnsupportedOperationException()
    }

    override def getBoolean(x$1: Int): Boolean = {
      throw new UnsupportedOperationException()
    }

    override def getByte(x$1: Int): Byte = {
      throw new UnsupportedOperationException()
    }

    override def getDouble(x$1: Int): Double = {
      throw new UnsupportedOperationException()
    }

    override def getFloat(x$1: Int): Float = {
      throw new UnsupportedOperationException()
    }

    override def getInt(x$1: Int): Int = {
      throw new UnsupportedOperationException()
    }

    override def getLong(x$1: Int): Long = {
      throw new UnsupportedOperationException()
    }

    override def getShort(x$1: Int): Short = {
      throw new UnsupportedOperationException()
    }

    override def isNullAt(x$1: Int): Boolean = {
      throw new UnsupportedOperationException()
    }

    override def close(): Unit = {
      builder.close()
    }
  }

  @scala.annotation.nowarn(
    "msg=method readFooter in class ParquetFileReader is deprecated"
  )
  private def replaceBatch(
    hadoopConf: Configuration,
    rowIndex: Long,
    batch: ColumnarBatch,
    size: Int,
    rowIndexColumnOpt: Option[ColumnMetadata],
    isRowDeletedColumnOpt: Option[ColumnMetadata],
    rowIndexFilterOpt: Option[RowIndexFilter],
    file: PartitionedFile,
    metrics: Map[String, GpuMetric]): ColumnarBatch = {

    var startTime = System.nanoTime()
    val rowIndexCol = getRowIndexPosSimple(size)
    metrics("rowIndexColumnGenTime") += System.nanoTime() - startTime

    startTime = System.nanoTime()
    val isRowDeletedVector =
      withResource(new RapidsHostColumnBuilder(new BasicType(false, DType.INT8), size)) { builder =>
        val isRowDeletedVector = SkipRowRapidsHostWriteableVector(builder, size, ByteType)
        rowIndexFilterOpt.get.materializeIntoVector(rowIndex, rowIndex + size, isRowDeletedVector)
        builder.buildAndPutOnDevice()
      }
    metrics("isRowDeletedColumnGenTime") += System.nanoTime() - startTime

    val indexVectorTuples = new ArrayBuffer[(Int, org.apache.spark.sql.vectorized.ColumnVector)]

    withResource(rowIndexCol.getBase) { host =>
      if (rowIndexColumnOpt.isDefined) {
        indexVectorTuples += (rowIndexColumnOpt.get.index ->
          GpuColumnVector.from(host.copyToDevice(), rowIndexCol.dataType()))
      }
    }
    indexVectorTuples += (isRowDeletedColumnOpt.get.index ->
        GpuColumnVector.from(isRowDeletedVector, ByteType))
    replaceVectors(batch, indexVectorTuples.toSeq : _*)
  }
}
