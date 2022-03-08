/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import java.io.{InputStream, IOException}
import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import ai.rapids.cudf._
import ai.rapids.cudf.ParquetWriterOptions.StatisticsFrequency
import com.nvidia.spark.GpuCachedBatchSerializer
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import java.util
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.parquet.column.{ColumnDescriptor, ParquetProperties}
import org.apache.parquet.hadoop.{CodecFactory, MemoryManager, ParquetFileReader, ParquetFileWriter, ParquetInputFormat, ParquetOutputFormat, ParquetRecordWriter, ParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{DelegatingPositionOutputStream, DelegatingSeekableInputStream, InputFile, OutputFile, PositionOutputStream, SeekableInputStream}
import org.apache.parquet.schema.Type

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{vectorized, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetToSparkSchemaConverter, ParquetWriteSupport, SparkToParquetSchemaConverter, VectorizedColumnReader}
import org.apache.spark.sql.execution.datasources.parquet.rapids.shims.v2.{ParquetRecordMaterializer, ShimVectorizedColumnReader}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * copied from Spark org.apache.spark.util.ByteBufferInputStream
 */
private class ByteBufferInputStream(private var buffer: ByteBuffer)
    extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position() + amountToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
   */
  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}

class ByteArrayInputFile(buff: Array[Byte]) extends InputFile {

  override def getLength: Long = buff.length

  override def newStream(): SeekableInputStream = {
    val byteBuffer = ByteBuffer.wrap(buff)
    new DelegatingSeekableInputStream(new ByteBufferInputStream(byteBuffer)) {
      override def getPos: Long = byteBuffer.position()

      override def seek(newPos: Long): Unit = {
        if (newPos > Int.MaxValue || newPos < Int.MinValue) {
          throw new IllegalStateException("seek value is out of supported range " + newPos)
        }
        byteBuffer.position(newPos.toInt)
      }
    }
  }
}

private object ByteArrayOutputFile {
  val BLOCK_SIZE: Int = 32 * 1024 * 1024 // 32M
}

private class ByteArrayOutputFile(stream: ByteArrayOutputStream) extends OutputFile {
  override def create(blockSizeHint: Long): PositionOutputStream = {
    new DelegatingPositionOutputStream(stream) {
      var pos = 0

      override def getPos: Long = pos

      override def write(b: Int): Unit = {
        super.write(b)
        pos += Integer.BYTES
      }

      override def write(b: Array[Byte]): Unit = {
        super.write(b)
        pos += b.length
      }

      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        super.write(b, off, len)
        pos += len
      }
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    throw new UnsupportedOperationException("Don't need to overwrite")

  override def supportsBlockSize(): Boolean = true

  override def defaultBlockSize(): Long = ByteArrayOutputFile.BLOCK_SIZE
}

private class ParquetBufferConsumer(val numRows: Int) extends HostBufferConsumer with
    AutoCloseable {
  @transient private[this] val offHeapBuffers = mutable.Queue[(HostMemoryBuffer, Long)]()
  private var buffer: Array[Byte] = _

  override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
    offHeapBuffers += Tuple2(buffer, len)
  }

  def getBuffer: Array[Byte] = {
    if (buffer == null) {
      writeBuffers()
    }
    buffer
  }

  def close(): Unit = {
    if (buffer == null) {
      writeBuffers()
    }
  }

  private def writeBuffers(): Unit = {
    val toProcess = offHeapBuffers.dequeueAll(_ => true)
    // We are making sure the input is smaller than 2gb so the parquet written should never be more
    // than Int.MAX_SIZE.
    val bytes = toProcess.map(_._2).sum

    // for now assert bytes are less than Int.MaxValue
    assert(bytes <= Int.MaxValue)
    buffer = new Array(bytes.toInt)
    try {
      var offset: Int = 0
      toProcess.foreach(ops => {
        val origBuffer = ops._1
        val len = ops._2.toInt
        origBuffer.asByteBuffer().get(buffer, offset, len)
        offset = offset + len
      })
    } finally {
      toProcess.map(_._1).safeClose()
    }
  }
}

private object ParquetCachedBatch {
  def apply(parquetBuff: ParquetBufferConsumer): ParquetCachedBatch = {
    new ParquetCachedBatch(parquetBuff.numRows, parquetBuff.getBuffer)
  }
}

case class ParquetCachedBatch(
    numRows: Int,
    buffer: Array[Byte]) extends CachedBatch {
  override def sizeInBytes: Long = buffer.length
}

/**
 * Spark wants the producer to close the batch. We have a listener in this iterator that will close
 * the batch after the task is completed
 */
private case class CloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]) extends
    Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = _

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close()
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((_: TaskContext) => {
    closeCurrentBatch()
  })

  override def hasNext: Boolean = iter.hasNext

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = iter.next()
    cb
  }
}

/**
 * This class assumes, the data is Columnar and the plugin is on.
 * Note, this class should not be referenced directly in source code.
 * It should be loaded by reflection using ShimLoader.newInstanceOf, see ./docs/dev/shims.md
 */
protected class ParquetCachedBatchSerializer extends GpuCachedBatchSerializer with Arm {

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall { f =>
    // only check spark b/c if we are on the GPU then we will be calling the gpu method regardless
    isTypeSupportedByColumnarSparkParquetWriter(f.dataType) || f.dataType == DataTypes.NullType
  }

  private def isTypeSupportedByColumnarSparkParquetWriter(dataType: DataType): Boolean = {
    // Columnar writer in Spark only supports AtomicTypes ATM
    dataType match {
      case TimestampType | StringType | BooleanType | DateType | BinaryType |
           DoubleType | FloatType | ByteType | IntegerType | LongType | ShortType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  def isSchemaSupportedByCudf(schema: Seq[Attribute]): Boolean = {
    schema.forall(field => isSupportedByCudf(field.dataType))
  }

  def isSupportedByCudf(dataType: DataType): Boolean = {
    dataType match {
      case a: ArrayType => isSupportedByCudf(a.elementType)
      case s: StructType => s.forall(field => isSupportedByCudf(field.dataType))
      case m: MapType => isSupportedByCudf(m.keyType) && isSupportedByCudf(m.valueType)
      case _ => GpuColumnVector.isNonNestedSupportedType(dataType)
    }
  }

  /**
   * This method checks if the datatype passed is officially supported by parquet.
   *
   * Please refer to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md to see
   * the what types are supported by parquet
   */
  def isTypeSupportedByParquet(dataType: DataType): Boolean = {
    dataType match {
      case CalendarIntervalType | NullType => false
      case s: StructType => s.forall(field => isTypeSupportedByParquet(field.dataType))
      case ArrayType(elementType, _) => isTypeSupportedByParquet(elementType)
      case MapType(keyType, valueType, _) => isTypeSupportedByParquet(keyType) &&
          isTypeSupportedByParquet(valueType)
      case d: DecimalType if d.scale < 0 => false
      case _ => true
    }
  }

  /**
   * Convert an `RDD[ColumnarBatch]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * This method uses Parquet Writer on the GPU to write the cached batch
   *
   * @param input        the input `RDD` to be converted.
   * @param schema       the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf         the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    val rapidsConf = new RapidsConf(conf)
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val (schemaWithUnambiguousNames, _) = getSupportedSchemaFromUnsupported(schema)
    val structSchema = schemaWithUnambiguousNames.toStructType
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(schema)) {
      def putOnGpuIfNeeded(batch: ColumnarBatch): ColumnarBatch = {
        if (!batch.column(0).isInstanceOf[GpuColumnVector]) {
          val s: StructType = structSchema
          val gpuCB = new GpuColumnarBatchBuilder(s, batch.numRows()).build(batch.numRows())
          batch.close()
          gpuCB
        } else {
          batch
        }
      }

      input.flatMap(batch => {
        if (batch.numCols() == 0) {
          List(ParquetCachedBatch(batch.numRows(), new Array[Byte](0)))
        } else {
          withResource(putOnGpuIfNeeded(batch)) { gpuCB =>
            compressColumnarBatchWithParquet(gpuCB, structSchema, schema.toStructType,
              bytesAllowedPerBatch)
          }
        }
      })
    } else {
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[ColumnarBatch](cbIter, schemaWithUnambiguousNames, schema,
            broadcastedConf).getColumnarBatchToCachedBatchIterator
      }
    }
  }

  private[rapids] def compressColumnarBatchWithParquet(
      oldGpuCB: ColumnarBatch,
      schema: StructType,
      origSchema: StructType,
      bytesAllowedPerBatch: Long): List[ParquetCachedBatch] = {
    val estimatedRowSize = scala.Range(0, oldGpuCB.numCols()).map { idx =>
      oldGpuCB.column(idx).asInstanceOf[GpuColumnVector]
          .getBase.getDeviceMemorySize / oldGpuCB.numRows()
    }.sum

    val columns = for (i <- 0 until oldGpuCB.numCols()) yield {
      val gpuVector = oldGpuCB.column(i).asInstanceOf[GpuColumnVector]
      var dataType = origSchema(i).dataType
      val v = ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(gpuVector.getBase,
        origSchema(i).dataType,
        // we are checking for scale > 0 because cudf and spark refer to scales as opposites
        // e.g. scale = -3 in Spark is scale = 3 in cudf
        (_, cv) => cv.getType.isDecimalType && cv.getType.getScale > 0,
        (_, cv) => {
          if (cv.getType.isBackedByLong) {
            dataType = LongType
            cv.bitCastTo(DType.INT64)
          } else {
            dataType = IntegerType
            cv.bitCastTo(DType.INT32)
          }
        }
      )
      GpuColumnVector.from(v, schema(i).dataType)
    }
    withResource(new ColumnarBatch(columns.toArray, oldGpuCB.numRows())) { gpuCB =>
      val rowsAllowedInBatch = (bytesAllowedPerBatch / estimatedRowSize).toInt
      val splitIndices = scala.Range(rowsAllowedInBatch, gpuCB.numRows(), rowsAllowedInBatch)
      val buffers = new ListBuffer[ParquetCachedBatch]
      if (splitIndices.nonEmpty) {
        val splitVectors = new ListBuffer[Array[ColumnVector]]
        try {
          for (index <- 0 until gpuCB.numCols()) {
            splitVectors +=
                gpuCB.column(index).asInstanceOf[GpuColumnVector].getBase.split(splitIndices: _*)
          }

          // Splitting the table
          // e.g. T0 = {col1, col2,...,coln} => split columns into 'm' cols =>
          // T00= {splitCol1(0), splitCol2(0),...,splitColn(0)}
          // T01= {splitCol1(1), splitCol2(1),...,splitColn(1)}
          // ...
          // T0m= {splitCol1(m), splitCol2(m),...,splitColn(m)}
          def makeTableForIndex(i: Int): Table = {
            val columns = splitVectors.indices.map(j => splitVectors(j)(i))
            new Table(columns: _*)
          }

          for (i <- splitVectors.head.indices) {
            withResource(makeTableForIndex(i)) { table =>
              val buffer = writeTableToCachedBatch(table, schema)
              buffers += ParquetCachedBatch(buffer)
            }
          }
        } finally {
          splitVectors.foreach(array => array.safeClose())
        }
      } else {
        withResource(GpuColumnVector.from(gpuCB)) { table =>
          val buffer = writeTableToCachedBatch(table, schema)
          buffers += ParquetCachedBatch(buffer)
        }
      }
      buffers.toList
    }
  }

  private def writeTableToCachedBatch(
      table: Table,
      schema: StructType): ParquetBufferConsumer = {
    val buffer = new ParquetBufferConsumer(table.getRowCount.toInt)
    val opts = SchemaUtils
        .writerOptionsFromSchema(ParquetWriterOptions.builder(), schema, writeInt96 = false)
        .withStatisticsFrequency(StatisticsFrequency.ROWGROUP).build()
    withResource(Table.writeParquetChunked(opts, buffer)) { writer =>
      writer.write(table)
    }
    buffer
  }

  /**
   * This method decodes the CachedBatch leaving it on the GPU to avoid the extra copying back to
   * the host
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf               the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  def gpuConvertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // optimize
    val newSelectedAttributes = if (selectedAttributes.isEmpty) {
      cacheAttributes
    } else {
      selectedAttributes
    }
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, newSelectedAttributes)
    convertCachedBatchToColumnarInternal(
      input,
      cachedSchemaWithNames,
      selectedSchemaWithNames,
      newSelectedAttributes)
  }

  private def convertCachedBatchToColumnarInternal(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      originalSelectedAttributes: Seq[Attribute]): RDD[ColumnarBatch] = {

    val cbRdd: RDD[ColumnarBatch] = input.map {
      case parquetCB: ParquetCachedBatch =>
        val parquetOptions = ParquetOptions.builder()
            .includeColumn(selectedAttributes.map(_.name).asJavaCollection).build()
        withResource(Table.readParquet(parquetOptions, parquetCB.buffer, 0,
          parquetCB.sizeInBytes)) { table =>
          withResource {
            for (i <- 0 until table.getNumberOfColumns) yield {
              ColumnCastUtil.ifTrueThenDeepConvertTypeAtoTypeB(table.getColumn(i),
                originalSelectedAttributes(i).dataType,
                (dataType, _) => dataType match {
                  case d: DecimalType if d.scale < 0 => true
                  case _ => false
                },
                (dataType, cv) => {
                  dataType match {
                    case d: DecimalType =>
                      withResource(cv.bitCastTo(DecimalUtil.createCudfDecimal(d))) {
                        _.copyToColumnVector()
                      }
                    case _ =>
                      throw new IllegalStateException("We don't cast any type besides Decimal " +
                          "with scale < 0")
                  }
                }
              )
            }
          } { col =>
            withResource(new Table(col: _*)) { t =>
              GpuColumnVector.from(t, originalSelectedAttributes.map(_.dataType).toArray)
            }
          }
        }
      case _ =>
        throw new IllegalStateException("I don't know how to convert this batch")
    }
    cbRdd
  }

  private def getSelectedSchemaFromCachedSchema(
      selectedAttributes: Seq[Attribute],
      cacheAttributes: Seq[Attribute]): Seq[Attribute] = {
    selectedAttributes.map {
      a => cacheAttributes(cacheAttributes.map(_.exprId).indexOf(a.exprId))
    }
  }

  /**
   * Convert the cached data into a ColumnarBatch taking the result data back to the host
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf               the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    // optimize
    val newSelectedAttributes = if (selectedAttributes.isEmpty) {
      cacheAttributes
    } else {
      selectedAttributes
    }
    val rapidsConf = new RapidsConf(conf)
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, newSelectedAttributes)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(cachedSchemaWithNames)) {
      val batches = convertCachedBatchToColumnarInternal(input, cachedSchemaWithNames,
        selectedSchemaWithNames, newSelectedAttributes)
      val cbRdd = batches.map(batch => {
        withResource(batch) { gpuBatch =>
          val cols = GpuColumnVector.extractColumns(gpuBatch)
          new ColumnarBatch(cols.safeMap(_.copyToHost()).toArray, gpuBatch.numRows())
        }
      })
      cbRdd.mapPartitions(iter => CloseableColumnBatchIterator(iter))
    } else {
      val origSelectedAttributesWithUnambiguousNames = 
        sanitizeColumnNames(newSelectedAttributes, selectedSchemaWithNames)
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter => {
          new CachedBatchIteratorConsumer(cbIter, cachedSchemaWithNames, selectedSchemaWithNames,
            cacheAttributes, origSelectedAttributesWithUnambiguousNames, broadcastedConf)
            .getColumnBatchIterator
        }
      }
    }
  }

  /**
   * Convert the cached batch into `InternalRow`s.
   *
   * @param input              the cached batches that should be converted.
   * @param cacheAttributes    the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data and the order they
   *                           should appear in the output rows.
   * @param conf               the configuration for the job.
   * @return RDD of the rows that were stored in the cached batches.
   */
  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    val (cachedSchemaWithNames, selectedSchemaWithNames) =
      getSupportedSchemaFromUnsupported(cacheAttributes, selectedAttributes)
    val newSelectedAttributes = sanitizeColumnNames(selectedAttributes, selectedSchemaWithNames)
    val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions {
      cbIter => {
        new CachedBatchIteratorConsumer(cbIter, cachedSchemaWithNames, selectedSchemaWithNames,
          cacheAttributes, newSelectedAttributes, broadcastedConf).getInternalRowIterator
      }
    }
  }

  private abstract class UnsupportedDataHandlerIterator extends Iterator[InternalRow] {

    def handleInternalRow(schema: Seq[Attribute], row: InternalRow, newRow: InternalRow): Unit

    def handleInterval(data: SpecializedGetters, index: Int): Any

    def handleStruct(
        data: InternalRow,
        origSchema: StructType,
        supportedSchema: StructType): InternalRow = {
      val structRow = InternalRow.fromSeq(supportedSchema)
      handleInternalRow(origSchema.map(field =>
        AttributeReference(field.name, field.dataType, field.nullable)()), data, structRow)
      structRow
    }

    def handleMap(
        keyType: DataType,
        valueType: DataType,
        mapData: MapData): MapData = {
      val keyData = mapData.keyArray()
      val newKeyData = handleArray(keyType, keyData)
      val valueData = mapData.valueArray()
      val newValueData = handleArray(valueType, valueData)
      new ArrayBasedMapData(newKeyData, newValueData)
    }

    def handleArray(
        dataType: DataType,
        arrayData: ArrayData): ArrayData = {
      dataType match {
        case s@StructType(_) =>
          val listBuffer = new ListBuffer[InternalRow]()
          val supportedSchema = mapping(dataType).asInstanceOf[StructType]
          arrayData.foreach(supportedSchema, (_, data) => {
            val structRow =
              handleStruct(data.asInstanceOf[InternalRow], s, s)
            listBuffer += structRow.copy()
          })
          new GenericArrayData(listBuffer)

        case ArrayType(elementType, _) =>
          val arrayList = new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val subArrayData = arrayData.getArray(i)
            arrayList.append(handleArray(elementType, subArrayData))
          }
          new GenericArrayData(arrayList)

        case m@MapType(_, _, _) =>
          val mapList =
            new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val mapData = arrayData.getMap(i)
            mapList.append(handleMap(m.keyType, m.valueType, mapData))
          }
          new GenericArrayData(mapList)

        case CalendarIntervalType =>
          val citList = new ListBuffer[Any]()
          scala.Range(0, arrayData.numElements()).foreach { i =>
            val citRow = handleInterval(arrayData, i)
            citList += citRow
          }
          new GenericArrayData(citList)

        case _ =>
          arrayData
      }
    }
  }

  /**
   * Consumes the Iterator[CachedBatch] to return either Iterator[ColumnarBatch] or
   * Iterator[InternalRow]
   */
  private class CachedBatchIteratorConsumer(
      cbIter: Iterator[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      origCacheSchema: Seq[Attribute],
      origRequestedSchema: Seq[Attribute],
      sharedConf: Broadcast[Map[String, String]]) {

    val conf: SQLConf = getConfFromMap(sharedConf)
    val hadoopConf: Configuration = getHadoopConf(origRequestedSchema.toStructType, conf)
    val options: ParquetReadOptions = HadoopReadOptions.builder(hadoopConf).build()
    /**
     * We are getting this method using reflection because its a package-private
     */
    val readBatchMethod: Method =
      classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
        classOf[WritableColumnVector])
    readBatchMethod.setAccessible(true)

    def getInternalRowIterator: Iterator[InternalRow] = {

      /**
       * This iterator converts an iterator[CachedBatch] to an iterator[InternalRow].
       *
       * This makes it unlike a regular iterator because CachedBatch => InternalRow* is a 1-n
       * relation. The way we have implemented this is to first go through the
       * iterator[CachedBatch] (cbIter) to look for a valid iterator (iter) i.e. hasNext() => true.
       * Then every time next() is called we return a single InternalRow from iter. When
       * iter.hasNext() => false, we find the next valid iterator in cbIter and the process
       * continues as above.
       */
      new Iterator[InternalRow]() {

        var iter: Iterator[InternalRow] = _

        override def hasNext: Boolean = {
          // go over the batch and get the next non-degenerate iterator
          // and return if it hasNext
          while ((iter == null || !iter.hasNext) && cbIter.hasNext) {
            iter = convertCachedBatchToInternalRowIter
          }
          iter != null && iter.hasNext
        }

        override def next(): InternalRow = {
          // will return the next InternalRow if hasNext() is true, otherwise throw
          if (hasNext) {
            iter.next()
          } else {
            throw new NoSuchElementException("no elements found")
          }
        }

        /**
         * This method converts a CachedBatch to an iterator of InternalRows.
         */
        private def convertCachedBatchToInternalRowIter: Iterator[InternalRow] = {
          val parquetCachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
          val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)
          withResource(ParquetFileReader.open(inputFile, options)) { parquetFileReader =>
            val parquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema
            val hasUnsupportedType = origCacheSchema.exists { field =>
              !isTypeSupportedByParquet(field.dataType)
            }

            val unsafeRows = new ArrayBuffer[InternalRow]
            import org.apache.parquet.io.ColumnIOFactory
            var pages = parquetFileReader.readNextRowGroup()
            while (pages != null) {
              val rows = pages.getRowCount
              val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)
              val recordReader =
                columnIO.getRecordReader(pages, new ParquetRecordMaterializer(parquetSchema,
                  cacheAttributes.toStructType,
                  new ParquetToSparkSchemaConverter(hadoopConf), None /*convertTz*/ ,
                  LegacyBehaviorPolicy.CORRECTED))
              for (_ <- 0 until rows.toInt) {
                val row = recordReader.read
                unsafeRows += row.copy()
              }
              pages = parquetFileReader.readNextRowGroup()
            }

            val iter = unsafeRows.iterator
            val unsafeProjection =
              GenerateUnsafeProjection.generate(selectedAttributes, cacheAttributes)
            if (hasUnsupportedType) {
              new UnsupportedDataHandlerIterator() {
                val wrappedIter: Iterator[InternalRow] = iter
                val newRow = new GenericInternalRow(cacheAttributes.length)

                override def hasNext: Boolean = wrappedIter.hasNext

                override def next(): InternalRow = {
                  //read a row and convert it to what the caller is expecting
                  val row = wrappedIter.next()
                  handleInternalRow(origCacheSchema, row, newRow)
                  val unsafeProjection =
                    GenerateUnsafeProjection.generate(origRequestedSchema, origCacheSchema)
                  unsafeProjection.apply(newRow)
                }

                override def handleInterval(
                    data: SpecializedGetters,
                    index: Int): CalendarInterval = {
                  if (data.isNullAt(index)) {
                    null
                  } else {
                    val structData = data.getStruct(index, 3)
                    new CalendarInterval(structData.getInt(0),
                      structData.getInt(1), structData.getLong(2))
                  }
                }

                override def handleInternalRow(
                    schema: Seq[Attribute],
                    row: InternalRow,
                    newRow: InternalRow): Unit = {
                  schema.indices.foreach { index =>
                    val dataType = schema(index).dataType
                    if (mapping.contains(dataType) || dataType == CalendarIntervalType ||
                        dataType == NullType ||
                        (dataType.isInstanceOf[DecimalType]
                            && dataType.asInstanceOf[DecimalType].scale < 0)) {
                      if (row.isNullAt(index)) {
                        newRow.setNullAt(index)
                      } else {
                        dataType match {
                          case s@StructType(_) =>
                            val supportedSchema = mapping(dataType)
                                .asInstanceOf[StructType]
                            val structRow =
                              handleStruct(row.getStruct(index, supportedSchema.size), s, s)
                            newRow.update(index, structRow)

                          case a@ArrayType(_, _) =>
                            val arrayData = row.getArray(index)
                            newRow.update(index, handleArray(a.elementType, arrayData))

                          case MapType(keyType, valueType, _) =>
                            val mapData = row.getMap(index)
                            newRow.update(index, handleMap(keyType, valueType, mapData))

                          case CalendarIntervalType =>
                            val interval = handleInterval(row, index)
                            if (interval == null) {
                              newRow.setNullAt(index)
                            } else {
                              newRow.setInterval(index, interval)
                            }
                          case d: DecimalType =>
                            if (row.isNullAt(index)) {
                              newRow.setDecimal(index, null, d.precision)
                            } else {
                              val dec = if (d.precision <= Decimal.MAX_INT_DIGITS) {
                                Decimal(row.getInt(index).toLong, d.precision, d.scale)
                              } else {
                                Decimal(row.getLong(index), d.precision, d.scale)
                              }
                              newRow.update(index, dec)
                            }
                          case NullType =>
                            newRow.setNullAt(index)
                          case _ =>
                            newRow.update(index, row.get(index, dataType))
                        }
                      }
                    } else {
                      newRow.update(index, row.get(index, dataType))
                    }
                  }
                }
              }
            } else {
              iter.map(unsafeProjection)
            }
          }
        }
      }
    }

    private class CurrentBatchIterator(val parquetCachedBatch: ParquetCachedBatch)
        extends Iterator[ColumnarBatch] with AutoCloseable {

      val capacity = conf.parquetVectorizedReaderBatchSize
      var columnReaders: Array[VectorizedColumnReader] = _
      val columnVectors: Array[OffHeapColumnVector] =
        OffHeapColumnVector.allocateColumns(capacity, selectedAttributes.toStructType)
      val columnarBatch = new ColumnarBatch(columnVectors
          .asInstanceOf[Array[vectorized.ColumnVector]])
      var rowsReturned: Long = 0L
      var numBatched = 0
      var batchIdx = 0
      var totalCountLoadedSoFar: Long = 0
      val parquetFileReader =
        ParquetFileReader.open(new ByteArrayInputFile(parquetCachedBatch.buffer), options)
      val (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
      columnsInCache, typesInCache) = {
        val parquetToSparkSchemaConverter = new ParquetToSparkSchemaConverter(hadoopConf)
        // we are getting parquet schema and then converting it to catalyst schema
        // because catalyst schema that we get from Spark doesn't have the exact schema expected
        // by the columnar parquet reader
        val inMemCacheParquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema
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

        val reqParquetSchemaInCacheOrder = new org.apache.parquet.schema.MessageType(
          inMemCacheParquetSchema.getName(), reqSparkSchemaInCacheOrder.fields.map { f =>
            inMemCacheParquetSchema.getFields().get(inMemCacheParquetSchema.getFieldIndex(f.name))
          }:_*)

        val columnsRequested: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
        // reset spark schema calculated from parquet schema
        hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, inMemReqSparkSchema.json)
        hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, inMemReqSparkSchema.json)

        val columnsInCache: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
        val typesInCache: util.List[Type] = reqParquetSchemaInCacheOrder.asGroupType.getFields
        val missingColumns = new Array[Boolean](reqParquetSchemaInCacheOrder.getFieldCount)

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
          }
        }

        for (i <- missingColumns.indices) {
          if (missingColumns(i)) {
            columnVectors(i).putNulls(0, capacity)
            columnVectors(i).setIsConstant()
          }
        }

        (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
            columnsInCache, typesInCache)
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
                  null /*convertTz*/ ,
                  LegacyBehaviorPolicy.CORRECTED.toString,
                  LegacyBehaviorPolicy.EXCEPTION.toString, false)
          }
        }
        totalCountLoadedSoFar += pages.getRowCount
      }

      /**
       * Read the next RowGroup and read each column and return the columnarBatch
       */
      def nextBatch: Boolean = {
        for (vector <- columnVectors) {
          vector.reset()
        }
        columnarBatch.setNumRows(0)
        if (rowsReturned >= totalRowCount) return false
        checkEndOfRowGroup()
        val num = Math.min(capacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
        for (i <- columnReaders.indices) {
          if (columnReaders(i) != null) {
            readBatchMethod.invoke(columnReaders(i), num.asInstanceOf[AnyRef],
              columnVectors(cacheSchemaToReqSchemaMap(i)).asInstanceOf[AnyRef])
          }
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

    /**
     * This method returns a ColumnarBatch iterator over a CachedBatch.
     * Each CachedBatch => ColumnarBatch is a 1-1 conversion so its pretty straight forward
     */
    def getColumnBatchIterator: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
      var iter = getIterator

      def getIterator: Iterator[ColumnarBatch] = {
        if (!cbIter.hasNext) {
          Iterator.empty
        } else {
            new CurrentBatchIterator(cbIter.next().asInstanceOf[ParquetCachedBatch])
        }
      }

      override def hasNext: Boolean = {
        // go over the batch and get the next non-degenerate iterator
        // and return if it hasNext
        while ((iter == null || !iter.hasNext) && cbIter.hasNext) {
          iter = getIterator
        }
        iter != null && iter.hasNext
      }

      override def next(): ColumnarBatch = {
        // will return the next ColumnarBatch if hasNext() is true, otherwise throw
        if (hasNext) {
          iter.next()
        } else {
          throw new NoSuchElementException("no elements found")
        }
      }
    }
  }

  private def getConfFromMap(sharedConf: Broadcast[Map[String, String]]): SQLConf = {
    val conf = new SQLConf()
    sharedConf.value.foreach { case (k, v) => conf.setConfString(k, v) }
    conf
  }

  private val intervalStructType = new StructType()
      .add("_days", IntegerType)
      .add("_months", IntegerType)
      .add("_ms", LongType)

  def getBytesAllowedPerBatch(conf: SQLConf): Long = {
    val gpuBatchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(conf)
    // we are rough estimating 0.5% as meta_data_size. we can do better estimation in future
    val approxMetaDataSizeBytes = gpuBatchSize * 0.5/100
    (gpuBatchSize - approxMetaDataSizeBytes).toLong
  }

  /**
   * This is a private helper class to return Iterator to convert InternalRow or ColumnarBatch to
   * CachedBatch. There is no type checking so if the type of T is anything besides InternalRow
   * or ColumnarBatch then the behavior is undefined.
   *
   * @param iter             - an iterator over InternalRow or ColumnarBatch
   * @param cachedAttributes - Schema of the cached batch
   * @param sharedConf       - SQL conf
   * @tparam T - Strictly either InternalRow or ColumnarBatch
   */
  private[rapids] class CachedBatchIteratorProducer[T](
      iter: Iterator[T],
      cachedAttributes: Seq[Attribute],
      origCachedAttributes: Seq[Attribute],
      sharedConf: Broadcast[Map[String, String]]) {

    val conf: SQLConf = getConfFromMap(sharedConf)
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val hadoopConf: Configuration = getHadoopConf(cachedAttributes.toStructType, conf)

    def getInternalRowToCachedBatchIterator: Iterator[CachedBatch] = {
      new InternalRowToCachedBatchIterator
    }

    def getColumnarBatchToCachedBatchIterator: Iterator[CachedBatch] = {
      new ColumnarBatchToCachedBatchIterator
    }

    /**
     * This class produces an Iterator[CachedBatch] from Iterator[InternalRow]. This is a n-1
     * relationship. Each partition represents a single parquet file, so we encode it
     * and return the CachedBatch when next is called.
     */
    class InternalRowToCachedBatchIterator extends Iterator[CachedBatch]() {

      var parquetOutputFileFormat = new ParquetOutputFileFormat()

      // For testing only
      private[rapids] def setParquetOutputFileFormat(p: ParquetOutputFileFormat): Unit = {
        parquetOutputFileFormat = p
      }

      // is there a type that spark doesn't support by default in the schema?
      val hasUnsupportedType: Boolean = origCachedAttributes.exists { attribute =>
        !isTypeSupportedByParquet(attribute.dataType)
      }

      def getIterator: Iterator[InternalRow] = {
        if (!hasUnsupportedType) {
          iter.asInstanceOf[Iterator[InternalRow]]
        } else {
          new UnsupportedDataHandlerIterator {

            val wrappedIter: Iterator[InternalRow] = iter.asInstanceOf[Iterator[InternalRow]]

            val newRow: InternalRow = InternalRow.fromSeq(cachedAttributes)

            override def hasNext: Boolean = wrappedIter.hasNext

            override def next(): InternalRow = {
              val row = wrappedIter.next()
              handleInternalRow(origCachedAttributes, row, newRow)
              newRow
            }

            override def handleInterval(
                data: SpecializedGetters,
                index: Int): InternalRow = {
              val citRow = InternalRow(IntegerType, IntegerType, LongType)
              if (data.isNullAt(index)) {
                null
              } else {
                val cit = data.getInterval(index)
                citRow.setInt(0, cit.months)
                citRow.setInt(1, cit.days)
                citRow.setLong(2, cit.microseconds)
                citRow
              }
            }

            override def handleInternalRow(
                schema: Seq[Attribute],
                row: InternalRow,
                newRow: InternalRow): Unit = {
              schema.indices.foreach { index =>
                val dataType = schema(index).dataType
                if (mapping.contains(dataType) || dataType == CalendarIntervalType ||
                    dataType == NullType ||
                    (dataType.isInstanceOf[DecimalType]
                        && dataType.asInstanceOf[DecimalType].scale < 0)) {
                  if (row.isNullAt(index)) {
                    newRow.setNullAt(index)
                  } else {
                    dataType match {
                      case s@StructType(_) =>
                        val newSchema = mapping(dataType).asInstanceOf[StructType]
                        val structRow =
                          handleStruct(row.getStruct(index, s.fields.length), s, newSchema)
                        newRow.update(index, structRow)

                      case ArrayType(arrayDataType, _) =>
                        val arrayData = row.getArray(index)
                        val newArrayData = handleArray(arrayDataType, arrayData)
                        newRow.update(index, newArrayData)

                      case MapType(keyType, valueType, _) =>
                        val mapData = row.getMap(index)
                        val map = handleMap(keyType, valueType, mapData)
                        newRow.update(index, map)

                      case CalendarIntervalType =>
                        val structData: InternalRow = handleInterval(row, index)
                        if (structData == null) {
                          newRow.setNullAt(index)
                        } else {
                          newRow.update(index, structData)
                        }

                      case d: DecimalType if d.scale < 0 =>
                        if (d.precision <= Decimal.MAX_INT_DIGITS) {
                          newRow.update(index, row.getDecimal(index, d.precision, d.scale)
                              .toUnscaledLong.toInt)
                        } else {
                          newRow.update(index, row.getDecimal(index, d.precision, d.scale)
                              .toUnscaledLong)
                        }

                      case _ =>
                        newRow.update(index, row.get(index, dataType))
                    }
                  }
                } else {
                  newRow.update(index, row.get(index, dataType))
                }
              }
            }
          }
        }
      }

      override def hasNext: Boolean = queue.nonEmpty || iter.hasNext

      private val queue = new mutable.Queue[CachedBatch]()

      //estimate the size of a row
      val estimatedSize: Int = cachedAttributes.map { attr =>
        attr.dataType.defaultSize
      }.sum

      override def next(): CachedBatch = {
        if (queue.isEmpty) {
          // to store a row if we have read it but there is no room in the parquet file to put it
          // we will put it in the next CachedBatch
          var leftOverRow: Option[InternalRow] = None
          val rowIterator = getIterator
          while (rowIterator.hasNext || leftOverRow.nonEmpty) {
            // Each partition will be a single parquet file
            var rows = 0
            // at least a single block
            val stream = new ByteArrayOutputStream(ByteArrayOutputFile.BLOCK_SIZE)
            val outputFile: OutputFile = new ByteArrayOutputFile(stream)
            conf.setConfString(ShimLoader.getSparkShims.parquetRebaseWriteKey,
              LegacyBehaviorPolicy.CORRECTED.toString)
            val recordWriter = SQLConf.withExistingConf(conf) {
              parquetOutputFileFormat.getRecordWriter(outputFile, hadoopConf)
            }
            var totalSize = 0
            while ((rowIterator.hasNext || leftOverRow.nonEmpty)
                && totalSize < bytesAllowedPerBatch) {

              val row = if (leftOverRow.nonEmpty) {
                val a = leftOverRow.get
                leftOverRow = None // reset value
                a
              } else {
                rowIterator.next()
              }
              totalSize += {
                row match {
                  case r: UnsafeRow =>
                    r.getSizeInBytes
                  case _ =>
                    estimatedSize
                }
              }
              if (totalSize <= bytesAllowedPerBatch) {
                rows += 1
                if (rows < 0) {
                  throw new IllegalStateException("CachedBatch doesn't support rows larger " +
                      "than Int.MaxValue")
                }
                recordWriter.write(null, row)
              } else {
                leftOverRow = Some(if (row.isInstanceOf[UnsafeRow]) {
                  row.copy()
                } else {
                  row
                })
              }
            }
            // passing null as context isn't used in this method
            recordWriter.close(null)
            queue += ParquetCachedBatch(rows, stream.toByteArray)
          }
        }
        queue.dequeue()
      }
    }

    /**
     * This class produces an Iterator[CachedBatch] from Iterator[ColumnarBatch]. This is a 1-1
     * relationship. Each ColumnarBatch is converted to a single ParquetCachedBatch when next()
     * is called on this iterator
     */
    class ColumnarBatchToCachedBatchIterator extends InternalRowToCachedBatchIterator {
      override def getIterator: Iterator[InternalRow] = {

        new Iterator[InternalRow] {
          // We have to check for null context because of the unit test
          Option(TaskContext.get).foreach(_.addTaskCompletionListener[Unit](_ => hostBatch.close()))

          val batch: ColumnarBatch = iter.asInstanceOf[Iterator[ColumnarBatch]].next
          val hostBatch = if (batch.column(0).isInstanceOf[GpuColumnVector]) {
            withResource(batch) { batch =>
              new ColumnarBatch(batch.safeMap(_.copyToHost()).toArray, batch.numRows())
            }
          } else {
            batch
          }

          val rowIterator = hostBatch.rowIterator().asScala

          override def next: InternalRow = rowIterator.next

          override def hasNext: Boolean = rowIterator.hasNext

        }
      }
    }

  }

  val mapping = new mutable.HashMap[DataType, DataType]()

  def getSupportedDataType(curId: AtomicLong, dataType: DataType): DataType = {
    dataType match {
      case CalendarIntervalType =>
        intervalStructType
      case NullType =>
        ByteType
      case s: StructType =>
        val newStructType = StructType(
          s.indices.map { index =>
            StructField(curId.getAndIncrement().toString,
              getSupportedDataType(curId, s.fields(index).dataType), s.fields(index).nullable,
              s.fields(index).metadata)
          })
        mapping.put(s, newStructType)
        newStructType
      case a@ArrayType(elementType, nullable) =>
        val newArrayType =
          ArrayType(getSupportedDataType(curId, elementType), nullable)
        mapping.put(a, newArrayType)
        newArrayType
      case m@MapType(keyType, valueType, nullable) =>
        val newKeyType = getSupportedDataType(curId, keyType)
        val newValueType = getSupportedDataType(curId, valueType)
        val mapType = MapType(newKeyType, newValueType, nullable)
        mapping.put(m, mapType)
        mapType
      case d: DecimalType if d.scale < 0 =>
        val newType = if (d.precision <= Decimal.MAX_INT_DIGITS) {
          IntegerType
        } else {
          LongType
        }
        newType
      case _ =>
        dataType
    }
  }

  // We want to change the original schema to have the new names as well
  private def sanitizeColumnNames(originalSchema: Seq[Attribute],
      schemaToCopyNamesFrom: Seq[Attribute]): Seq[Attribute] = {
    originalSchema.zip(schemaToCopyNamesFrom).map {
      case (origAttribute, newAttribute) => origAttribute.withName(newAttribute.name)
    }
  }

  private def getSupportedSchemaFromUnsupported(
      cachedAttributes: Seq[Attribute],
      requestedAttributes: Seq[Attribute] = Seq.empty): (Seq[Attribute], Seq[Attribute]) = {

    // We only handle CalendarIntervalType, Decimals and NullType ATM convert it to a supported type
    val curId = new AtomicLong()
    val newCachedAttributes = cachedAttributes.map {
      attribute => val name = s"_col${curId.getAndIncrement()}"
        attribute.dataType match {
          case CalendarIntervalType =>
            AttributeReference(name, intervalStructType,
              attribute.nullable, metadata = attribute.metadata)(attribute.exprId)
                .asInstanceOf[Attribute]
          case NullType =>
            AttributeReference(name, DataTypes.ByteType,
              nullable = true, metadata =
                  attribute.metadata)(attribute.exprId).asInstanceOf[Attribute]
          case StructType(_) | ArrayType(_, _) | MapType(_, _, _) | DecimalType() =>
            AttributeReference(name,
              getSupportedDataType(curId, attribute.dataType),
              attribute.nullable, attribute.metadata)(attribute.exprId)
          case _ =>
            attribute.withName(name)
        }
    }

    val newRequestedAttributes =
      getSelectedSchemaFromCachedSchema(requestedAttributes, newCachedAttributes)

    (newCachedAttributes, newRequestedAttributes)
  }

  private def getHadoopConf(requestedSchema: StructType,
      sqlConf: SQLConf): Configuration = {

    val hadoopConf = new Configuration(false)
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requestedSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requestedSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sqlConf.sessionLocalTimeZone)

    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, false)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, false)
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE.key, false)

    hadoopConf.set(ShimLoader.getSparkShims.parquetRebaseWriteKey,
      LegacyBehaviorPolicy.CORRECTED.toString)

    hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)

    hadoopConf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)

    hadoopConf.set(ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)

    ParquetWriteSupport.setSchema(requestedSchema, hadoopConf)

    hadoopConf
  }

  /**
   * Convert an `RDD[InternalRow]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * We use the RowToColumnarIterator and convert each batch at a time
   *
   * @param input        the input `RDD` to be converted.
   * @param schema       the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf         the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    val rapidsConf = new RapidsConf(conf)
    val bytesAllowedPerBatch = getBytesAllowedPerBatch(conf)
    val (schemaWithUnambiguousNames, _) = getSupportedSchemaFromUnsupported(schema)
    if (rapidsConf.isSqlEnabled && rapidsConf.isSqlExecuteOnGPU &&
        isSchemaSupportedByCudf(schema)) {
      val structSchema = schemaWithUnambiguousNames.toStructType
      val converters = new GpuRowToColumnConverter(structSchema)
      val columnarBatchRdd = input.mapPartitions(iter => {
        new RowToColumnarIterator(iter, structSchema, RequireSingleBatch, converters)
      })
      columnarBatchRdd.flatMap(cb => {
        withResource(cb)(cb => compressColumnarBatchWithParquet(cb, structSchema,
          schema.toStructType, bytesAllowedPerBatch))
      })
    } else {
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[InternalRow](cbIter, schemaWithUnambiguousNames, schema,
            broadcastedConf).getInternalRowToCachedBatchIterator
      }
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    //essentially a noop
    (_: Int, b: Iterator[CachedBatch]) => b
  }
}

/**
 * Similar to ParquetFileFormat
 */
private[rapids] class ParquetOutputFileFormat {

  @scala.annotation.nowarn(
    "msg=constructor .* in class .* is deprecated"
  )
  def getRecordWriter(output: OutputFile, conf: Configuration): RecordWriter[Void, InternalRow] = {
    import ParquetOutputFormat._

    val blockSize = getLongBlockSize(conf)
    val maxPaddingSize =
      conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
    val validating = getValidation(conf)

    val writeSupport = new ParquetWriteSupport().asInstanceOf[WriteSupport[InternalRow]]
    val init = writeSupport.init(conf)
    val writer = new ParquetFileWriter(output, init.getSchema,
      Mode.CREATE, blockSize, maxPaddingSize)
    writer.start()

    val writerVersion =
      ParquetProperties.WriterVersion.fromString(conf.get(ParquetOutputFormat.WRITER_VERSION,
        ParquetProperties.WriterVersion.PARQUET_1_0.toString))

    val codecFactory = new CodecFactory(conf, getPageSize(conf))

    new ParquetRecordWriter[InternalRow](writer, writeSupport, init.getSchema,
      init.getExtraMetaData, blockSize, getPageSize(conf),
      codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED), getDictionaryPageSize(conf),
      getEnableDictionary(conf), validating, writerVersion,
      ParquetOutputFileFormat.getMemoryManager(conf))

  }
}

private object ParquetOutputFileFormat {
  var memoryManager: MemoryManager = _
  val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
  val DEFAULT_MIN_MEMORY_ALLOCATION: Long = 1 * 1024 * 1024 // 1MB

  def getMemoryManager(conf: Configuration): MemoryManager = {
    synchronized {
      if (memoryManager == null) {
        import ParquetOutputFormat._
        val maxLoad = conf.getFloat(MEMORY_POOL_RATIO, DEFAULT_MEMORY_POOL_RATIO)
        val minAllocation = conf.getLong(MIN_MEMORY_ALLOCATION, DEFAULT_MIN_MEMORY_ALLOCATION)
        memoryManager = new MemoryManager(maxLoad, minAllocation)
      }
    }
    memoryManager
  }
}
