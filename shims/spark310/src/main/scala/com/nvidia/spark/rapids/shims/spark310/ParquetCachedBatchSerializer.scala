/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark310

import java.io.InputStream
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.{CodecFactory, MemoryManager, ParquetFileReader, ParquetFileWriter, ParquetInputFormat, ParquetOutputFormat, ParquetRecordWriter, ParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.ParquetOutputFormat.{MEMORY_POOL_RATIO, MIN_MEMORY_ALLOCATION}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{DelegatingPositionOutputStream, DelegatingSeekableInputStream, InputFile, OutputFile, PositionOutputStream, SeekableInputStream}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetToSparkSchemaConverter, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.parquet.rapids.ParquetRecordMaterializer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SerializableConfiguration

/**
 * copied from Spark org.apache.spark.util.ByteBufferInputStream
 */
class ByteBufferInputStream(private var buffer: ByteBuffer)
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

object ByteArrayOutputFile {
  val BLOCK_SIZE = 32 * 1024 * 1024 // 32M
}

class ByteArrayOutputFile(stream: ByteArrayOutputStream) extends OutputFile {
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

class ParquetBufferConsumer(val numRows: Int) extends HostBufferConsumer with AutoCloseable {
  @transient private[this] val offHeapBuffers = mutable.Queue[(HostMemoryBuffer, Long)]()
  private var buffer: Array[Byte] = null

  override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
    offHeapBuffers += Tuple2(buffer, len)
  }

  def getBuffer(): Array[Byte] = {
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
    // this could be problematic if the buffers are big as their cumulative length could be more
    // than Int.MAX_SIZE. We could just have a list of buffers in that case and iterate over them
    val bytes = toProcess.unzip._2.sum

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

object ParquetCachedBatch {
  def apply(parquetBuff: ParquetBufferConsumer): ParquetCachedBatch = {
    new ParquetCachedBatch(parquetBuff.numRows, parquetBuff.getBuffer())
  }
}

case class ParquetCachedBatch(numRows: Int, buffer: Array[Byte]) extends CachedBatch {
  override def sizeInBytes: Long = buffer.length
}

/**
 * Spark wants the producer to close the batch. We have a listener in this iterator that will close
 * the batch after the task is completed
 */
private case class CloseableColumnBatchIterator(iter: Iterator[ColumnarBatch]) extends
  Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
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
 * This class assumes, the data is Columnar and the plugin is on
 */
class ParquetCachedBatchSerializer extends CachedBatchSerializer with Arm {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall(f =>
    // only check spark b/c if we are on the GPU then we will be calling the gpu method regardless
    isSupportedBySparkColumnar(f.dataType))

  private def isSupportedBySparkColumnar(dataType: DataType): Boolean =
    // AtomicType check. Everything is supported besides Structs, Maps or UDFs
    dataType match {
      case TimestampType | StringType | BooleanType | DateType | BinaryType |
           DoubleType | FloatType | ByteType | IntegerType | LongType | ShortType => true
      case _: HiveStringType => true
      case _: DecimalType => true
      case _ => false
    }

  def isSupportedByCudf(schema: Seq[Attribute]): Boolean = {
    schema.forall(a => GpuColumnVector.isSupportedType(a.dataType))
  }

  /**
   * Convert an `RDD[ColumnarBatch]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * This method uses Parquet Writer on the GPU to write the cached batch
   *
   * @param input the input `RDD` to be converted.
   * @param schema the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
     schema: Seq[Attribute],
     storageLevel: StorageLevel,
     conf: SQLConf): RDD[CachedBatch] = {
    val rapidsConf = new RapidsConf(conf)
    if (rapidsConf.isSqlEnabled && isSupportedByCudf(schema)) {
      def putOnGpuIfNeeded(batch: ColumnarBatch): ColumnarBatch = {
        if (!batch.column(0).isInstanceOf[GpuColumnVector]) {
          val s: StructType = schema.toStructType
          val gpuCB = new GpuColumnarBatchBuilder(s, batch.numRows(), batch).build(batch.numRows())
          batch.close()
          gpuCB
        } else {
          batch
        }
      }

      input.map(batch => {
        if (batch.numCols() == 0) {
          ParquetCachedBatch(batch.numRows(), new Array[Byte](0))
        } else {
          withResource(putOnGpuIfNeeded(batch)) { gpuCB =>
            compressColumnarBatchWithParquet(gpuCB)
          }
        }
      })
    } else {
      // TODO: need to implement writing ColumnarBatches on the CPU using Parquet writer
      throw new UnsupportedOperationException("Can't convert ColumnarBatch to CachedBatch on CPU")
    }
  }

  private def compressColumnarBatchWithParquet(gpuCB: ColumnarBatch): ParquetCachedBatch = {
    val buffer = new ParquetBufferConsumer(gpuCB.numRows())
    withResource(GpuColumnVector.from(gpuCB)) { table =>
      withResource(Table.writeParquetChunked(ParquetWriterOptions.DEFAULT, buffer)) { writer =>
        writer.write(table)
      }
    }
    ParquetCachedBatch(buffer)
  }

  /**
   * This method decodes the CachedBatch leaving it on the GPU to avoid the extra copying back to
   * the host
   * @param input the cached batches that should be converted.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  def gpuConvertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute],
    conf: SQLConf): RDD[ColumnarBatch] = {
    convertCachedBatchToColumnarInternal(input, cacheAttributes, selectedAttributes)
  }

  private def convertCachedBatchToColumnarInternal(input: RDD[CachedBatch],
    cacheAttributes: Seq[Attribute],
    selectedAttributes: Seq[Attribute]): RDD[ColumnarBatch] = {

    val requestedColumnIndices = getColumnIndices(selectedAttributes, cacheAttributes)

    val cbRdd: RDD[ColumnarBatch] = input.map(batch => {
      if (batch.isInstanceOf[ParquetCachedBatch]) {
        val parquetCB = batch.asInstanceOf[ParquetCachedBatch]
        val parquetOptions = ParquetOptions.builder().includeColumn(requestedColumnIndices
           .map(i => "_col"+i).asJavaCollection).build()
        withResource(Table.readParquet(parquetOptions, parquetCB.buffer, 0,
          parquetCB.sizeInBytes)) { table =>
          GpuColumnVector.from(table)
        }
      } else {
        throw new IllegalStateException("I don't know how to convert this batch")
      }
    })
    cbRdd
  }

  private def getColumnIndices(selectedAttributes: Seq[Attribute],
     cacheAttributes: Seq[Attribute]): Seq[Int] = {
    selectedAttributes.map(a => cacheAttributes.map(_.exprId).indexOf(a.exprId))
  }

  /**
   * Convert the cached data into a ColumnarBatch taking the result data back to the host
   *
   * @param input the cached batches that should be converted.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the fields that should be loaded from the data and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return an RDD of the input cached batches transformed into the ColumnarBatch format.
   */
  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
     cacheAttributes: Seq[Attribute],
     selectedAttributes: Seq[Attribute],
     conf: SQLConf): RDD[ColumnarBatch] = {
    val batches = convertCachedBatchToColumnarInternal(input, cacheAttributes,
      selectedAttributes)
    val cbRdd = batches.map(batch => {
      withResource(batch) { gpuBatch =>
        val cols = GpuColumnVector.extractColumns(gpuBatch)
        new ColumnarBatch(cols.map(_.copyToHost()).toArray, gpuBatch.numRows())
      }
    })
    cbRdd.mapPartitions(iter => CloseableColumnBatchIterator(iter))
  }

  /**
   * Convert the cached batch into `InternalRow`s.
   *
   * @param input the cached batches that should be converted.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data and the order they
   *                           should appear in the output rows.
   * @param conf the configuration for the job.
   * @return RDD of the rows that were stored in the cached batches.
   */
  override def convertCachedBatchToInternalRow(
     input: RDD[CachedBatch],
     cacheAttributes: Seq[Attribute],
     selectedAttributes: Seq[Attribute],
     conf: SQLConf): RDD[InternalRow] = {

    val sparkSession = SparkSession.active
    val hadoopConf = getHadoopConf(selectedAttributes.toStructType, conf)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    input.mapPartitions {
      cbIter => {
        new Iterator[InternalRow]() {
          var iter: Iterator[InternalRow] = null

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

          def convertCachedBatchToInternalRowIter: Iterator[InternalRow] = {
            val parquetCachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
            val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)

            val catalystColumns =
              getColumnIndices(cacheAttributes, cacheAttributes).map { i => "_col" + i }

            val catalystSchema = cacheAttributes.zip(catalystColumns).map {
              case (attr, name) => attr.withName(name)
            }

            val selectedColumns =
              getColumnIndices(selectedAttributes, cacheAttributes).map { i => "_col" + i }

            val requestedSchema = selectedAttributes.zip(selectedColumns).map {
              case (attr, name) => attr.withName(name)
            }

            val sharedConf = broadcastedHadoopConf.value.value
            val options = HadoopReadOptions.builder(sharedConf).build()
            val parquetFileReader = ParquetFileReader.open(inputFile, options)
            val parquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema

            val convertTz = None // as we are using hadoop and parquet mr

            val unsafeRows = new ArrayBuffer[InternalRow]
            import org.apache.parquet.io.ColumnIOFactory
            var pages = parquetFileReader.readNextRowGroup()
            while (pages != null) {
              val rows = pages.getRowCount
              val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)
              val recordReader =
                columnIO.getRecordReader(pages, new ParquetRecordMaterializer(parquetSchema,
                  catalystSchema.toStructType,
                  new ParquetToSparkSchemaConverter(sharedConf), convertTz,
                  LegacyBehaviorPolicy.CORRECTED))
              for (i <- 0 until rows.toInt) {
                val row = recordReader.read
                unsafeRows += row.copy()
              }
              pages = parquetFileReader.readNextRowGroup()
            }
            parquetFileReader.close()
            val iter = unsafeRows.iterator
            val unsafeProjection =
              GenerateUnsafeProjection.generate(requestedSchema, catalystSchema)
            iter.map(unsafeProjection)
          }
        }
      }
    }
  }

  private def getHadoopConf(requestedSchema: StructType,
     sqlConf: SQLConf) : Configuration = {

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

    hadoopConf.set(
      SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key, LegacyBehaviorPolicy.CORRECTED.toString)

    hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS.toString)

    hadoopConf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)

    ParquetWriteSupport.setSchema(requestedSchema, hadoopConf)

    hadoopConf
  }

  /**
   * Convert an `RDD[InternalRow]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * We use the RowToColumnarIterator and convert each batch at a time
   * @param input the input `RDD` to be converted.
   * @param schema the schema of the data being stored.
   * @param storageLevel where the data will be stored.
   * @param conf the config for the query.
   * @return The data converted into a format more suitable for caching.
   */
  override def convertInternalRowToCachedBatch(
     input: RDD[InternalRow],
     schema: Seq[Attribute],
     storageLevel: StorageLevel,
     conf: SQLConf): RDD[CachedBatch] = {

    val parquetSchema = schema.zip(getColumnIndices(schema, schema)).map {
      case (attr, index) => attr.withName("_col" + index)
    }

    val rapidsConf = new RapidsConf(conf)
    val sparkSession = SparkSession.active

    val hadoopConf = getHadoopConf(parquetSchema.toStructType, conf)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    SQLConf.get.setConfString(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key,
      LegacyBehaviorPolicy.CORRECTED.toString)

    val broadcastedConf = sparkSession.sparkContext.broadcast(conf)

    if (rapidsConf.isSqlEnabled && isSupportedByCudf(schema)) {
      val structSchema = schema.toStructType
      val converters = new GpuRowToColumnConverter(structSchema)
      val columnarBatchRdd = input.mapPartitions(iter => {
        new RowToColumnarIterator(iter, structSchema, RequireSingleBatch, converters)
      })
      columnarBatchRdd.map(cb => {
        withResource(cb) { columnarBatch =>
          val cachedBatch = compressColumnarBatchWithParquet(columnarBatch)
          cachedBatch
        }
      })
    } else {
      // fallback to the CPU
      input.mapPartitions {
        internalRowIter =>
          new Iterator[CachedBatch]() {
            private val parquetOutputFormat = new ParquetOutputFileFormat()

            override def hasNext: Boolean = internalRowIter.hasNext

            override def next(): CachedBatch = {
              // Each partition will be a single parquet file
              var rows = 0
              // at least a single block
              val stream = new ByteArrayOutputStream(ByteArrayOutputFile.BLOCK_SIZE)
              val outputFile: OutputFile = new ByteArrayOutputFile(stream)

              val recordWriter = SQLConf.withExistingConf(broadcastedConf.value) {
                parquetOutputFormat.getRecordWriter(outputFile, broadcastedHadoopConf.value.value)
              }

              while (internalRowIter.hasNext) {
                rows += 1
                if (rows < 0) {
                  throw new IllegalStateException("CachedBatch doesn't support rows larger " +
                    "than Int.MaxValue")
                }
                val row = internalRowIter.next()
                recordWriter.write(null, row)
              }
              // passing null as context isn't used in this method
              recordWriter.close(null)
              ParquetCachedBatch(rows, stream.toByteArray)
            }
          }
      }
    }
  }

  override def buildFilter(predicates: Seq[Expression],
     cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    //essentially a noop
    (partId: Int, b: Iterator[CachedBatch]) => b
  }
}

/**
 * Similar to ParquetFileFormat
 */
class ParquetOutputFileFormat() {

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

object ParquetOutputFileFormat {
  var memoryManager: MemoryManager = null
  val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
  val DEFAULT_MIN_MEMORY_ALLOCATION: Long = 1 * 1024 * 1024 // 1MB

  def getMemoryManager(conf: Configuration): MemoryManager = {
    synchronized {
      if (memoryManager == null) {
        val maxLoad = conf.getFloat(MEMORY_POOL_RATIO, DEFAULT_MEMORY_POOL_RATIO)
        val minAllocation = conf.getLong(MIN_MEMORY_ALLOCATION, DEFAULT_MIN_MEMORY_ALLOCATION)
        memoryManager = new MemoryManager(maxLoad, minAllocation)
      }
    }
    memoryManager
  }
}
