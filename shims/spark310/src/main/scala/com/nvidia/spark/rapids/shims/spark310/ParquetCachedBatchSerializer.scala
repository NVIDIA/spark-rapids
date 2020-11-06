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

import java.io.{InputStream, IOException}
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
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{DelegatingPositionOutputStream, DelegatingSeekableInputStream, InputFile, OutputFile, PositionOutputStream, SeekableInputStream}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetToSparkSchemaConverter, ParquetWriteSupport, SparkToParquetSchemaConverter, VectorizedColumnReader}
import org.apache.spark.sql.execution.datasources.parquet.rapids.ParquetRecordMaterializer
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SerializableConfiguration

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
  val BLOCK_SIZE = 32 * 1024 * 1024 // 32M
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

private object ParquetCachedBatch {
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
      val cachedSchema = getCatalystSchema(schema, schema)
      val broadcastedHadoopConf = getBroadcastedHadoopConf(conf, cachedSchema)
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf)
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[ColumnarBatch](cbIter, broadcastedHadoopConf.value.value,
            broadcastedConf.value).getColumnarBatchToCachedBatchIterator
      }
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
    convertCachedBatchToColumnarInternal(input, cacheAttributes, selectedAttributes)
  }

  private def convertCachedBatchToColumnarInternal(
     input: RDD[CachedBatch],
     cacheAttributes: Seq[Attribute],
     selectedAttributes: Seq[Attribute]): RDD[ColumnarBatch] = {

    val requestedColumnNames = getColumnNames(selectedAttributes, cacheAttributes)

    val cbRdd: RDD[ColumnarBatch] = input.map(batch => {
      if (batch.isInstanceOf[ParquetCachedBatch]) {
        val parquetCB = batch.asInstanceOf[ParquetCachedBatch]
        val parquetOptions = ParquetOptions.builder()
          .includeColumn(requestedColumnNames.asJavaCollection).build()
        withResource(Table.readParquet(parquetOptions, parquetCB.buffer, 0,
          parquetCB.sizeInBytes)) { table =>
          GpuColumnVector.from(table, selectedAttributes.map(_.dataType).toArray)
        }
      } else {
        throw new IllegalStateException("I don't know how to convert this batch")
      }
    })
    cbRdd
  }

  private def getColumnNames(
     selectedAttributes: Seq[Attribute],
     cacheAttributes: Seq[Attribute]): Seq[String] = {
    selectedAttributes.map(a => "_col" + cacheAttributes.map(_.exprId).indexOf(a.exprId))
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
    val rapidsConf = new RapidsConf(conf)
    if (rapidsConf.isSqlEnabled) {
      val batches = convertCachedBatchToColumnarInternal(input, cacheAttributes,
        selectedAttributes)
      val cbRdd = batches.map(batch => {
        withResource(batch) { gpuBatch =>
          val cols = GpuColumnVector.extractColumns(gpuBatch)
          new ColumnarBatch(cols.safeMap(_.copyToHost()).toArray, gpuBatch.numRows())
        }
      })
      cbRdd.mapPartitions(iter => CloseableColumnBatchIterator(iter))
    } else {
      val broadcastedHadoopConf = getBroadcastedHadoopConf(conf, selectedAttributes)
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf)
      input.mapPartitions {
        cbIter => {
          new CachedBatchIteratorConsumer(cbIter, cacheAttributes, selectedAttributes,
            broadcastedHadoopConf.value.value, broadcastedConf.value).getColumnBatchIterator
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

    val broadcastedHadoopConf = getBroadcastedHadoopConf(conf, selectedAttributes)
    val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf)
    input.mapPartitions {
      cbIter => {
        new CachedBatchIteratorConsumer(cbIter, cacheAttributes, selectedAttributes,
          broadcastedHadoopConf.value.value, broadcastedConf.value).getInternalRowIterator
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
     sharedHadoopConf: Configuration,
     sharedConf: SQLConf) {

    val requestedSchema = getCatalystSchema(selectedAttributes, cacheAttributes)
    val options = HadoopReadOptions.builder(sharedHadoopConf).build()
    /**
     * We are getting this method using reflection because its a package-private
     */
    val readBatchMethod =
      classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
        classOf[WritableColumnVector])
    readBatchMethod.setAccessible(true)

    def getInternalRowIterator: Iterator[InternalRow] = {

      val cacheSchema = getCatalystSchema(cacheAttributes, cacheAttributes)

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

        /**
         * This method converts a CachedBatch to an iterator of InternalRows.
         */
        private def convertCachedBatchToInternalRowIter: Iterator[InternalRow] = {
          val parquetCachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
          val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)
          val parquetFileReader = ParquetFileReader.open(inputFile, options)
          val parquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema

          val unsafeRows = new ArrayBuffer[InternalRow]
          import org.apache.parquet.io.ColumnIOFactory
          var pages = parquetFileReader.readNextRowGroup()
          while (pages != null) {
            val rows = pages.getRowCount
            val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)
            val recordReader =
              columnIO.getRecordReader(pages, new ParquetRecordMaterializer(parquetSchema,
                cacheSchema.toStructType,
                new ParquetToSparkSchemaConverter(sharedHadoopConf), None /*convertTz*/,
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
            GenerateUnsafeProjection.generate(requestedSchema, cacheSchema)
          iter.map(unsafeProjection)
        }
      }
    }

    /**
     * This method returns a ColumnarBatch iterator over a CachedBatch.
     * Each CachedBatch => ColumnarBatch is a 1-1 conversion so its pretty straight forward
     */
    def getColumnBatchIterator: Iterator[ColumnarBatch] = {
      if (!cbIter.hasNext) return Iterator.empty
      val capacity = sharedConf.parquetVectorizedReaderBatchSize
      val parquetCachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
      val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)
      val parquetFileReader = ParquetFileReader.open(inputFile, options)
      val parquetSchema = parquetFileReader.getFooter.getFileMetaData.getSchema

      // we are getting parquet schema and then converting it to catalyst schema
      // because catalyst schema that we get from Spark doesn't have the exact schema expected by
      // the columnar parquet reader
      val parquetToSparkSchemaConverter = new ParquetToSparkSchemaConverter(sharedHadoopConf)
      val sparkSchema = parquetToSparkSchemaConverter.convert(parquetSchema)
      val sparkToParquetSchemaConverter = new SparkToParquetSchemaConverter(sharedHadoopConf)
      val reqSparkSchema =
        StructType(sparkSchema.filter(field =>
          requestedSchema.exists(a => a.name.equals(field.name))))
      val reqParquetSchema = sparkToParquetSchemaConverter.convert(reqSparkSchema)

      // reset spark schema calculated from parquet schema
      sharedHadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, reqSparkSchema.json)
      sharedHadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, reqSparkSchema.json)

      /**
       * Read the next RowGroup and read each column and return the columnarBatch
       */
      new Iterator[ColumnarBatch] {
        val columnVectors = OffHeapColumnVector.allocateColumns(capacity,
          requestedSchema.toStructType)
        val columnarBatch: ColumnarBatch = new ColumnarBatch(columnVectors
          .asInstanceOf[Array[org.apache.spark.sql.vectorized.ColumnVector]])
        val missingColumns = new Array[Boolean](reqParquetSchema.getFieldCount)
        var columnReaders: Array[VectorizedColumnReader] = null

        var rowsReturned: Long = 0L
        var totalRowCount: Long = 0L
        var totalCountLoadedSoFar: Long = 0
        var batchIdx = 0
        var numBatched = 0

        for (block <- parquetFileReader.getRowGroups.asScala) {
          this.totalRowCount += block.getRowCount
        }

        for (i <- missingColumns.indices) {
          if (missingColumns(i)) {
            columnVectors(i).putNulls(0, capacity)
            columnVectors(i).setIsConstant()
          }
        }

        @throws[IOException]
        def checkEndOfRowGroup(): Unit = {
          if (rowsReturned != totalCountLoadedSoFar) return
          val pages = parquetFileReader.readNextRowGroup
          if (pages == null) {
            throw new IOException("expecting more rows but reached last" +
              " block. Read " + rowsReturned + " out of " + totalRowCount)
          }
          val columns = reqParquetSchema.getColumns
          val types = reqParquetSchema.asGroupType.getFields
          columnReaders = new Array[VectorizedColumnReader](columns.size)
          for (i <- 0 until columns.size) {
            if (!missingColumns(i)) {
              columnReaders(i) =
                new VectorizedColumnReader(columns.get(i), types.get(i)
                  .getOriginalType, pages.getPageReader(columns.get(i)), null /*convertTz*/,
                  LegacyBehaviorPolicy.CORRECTED.toString, LegacyBehaviorPolicy.EXCEPTION.toString)
            }
          }
          totalCountLoadedSoFar += pages.getRowCount
        }

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
                columnVectors(i).asInstanceOf[AnyRef])
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
            columnarBatch
          } else {
            throw new NoSuchElementException("no elements found")
          }
        }
      }
    }
  }

  private def getCatalystSchema(
     selectedAttributes: Seq[Attribute],
     cacheAttributes: Seq[Attribute]): Seq[Attribute] = {
    val catalystColumns = getColumnNames(selectedAttributes, cacheAttributes)
    selectedAttributes.zip(catalystColumns).map {
      case (attr, name) => attr.withName(name)
    }
  }

  /**
   * This is a private helper class to return Iterator to convert InternalRow or ColumnarBatch to
   * CachedBatch. There is no type checking so if the type of T is anything besides InternalRow
   * or ColumnarBatch then the behavior is undefined.
   *
   * @param iter - an iterator over InternalRow or ColumnarBatch
   * @param sharedHadoopConf - Hadoop conf
   * @param sharedConf - SQL conf
   * @tparam T - Strictly either InternalRow or ColumnarBatch
   */
  private class CachedBatchIteratorProducer[T](
     iter: Iterator[T],
     sharedHadoopConf: Configuration,
     sharedConf: SQLConf) {

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
    private class InternalRowToCachedBatchIterator extends Iterator[CachedBatch]() {

      def getIterator: Iterator[InternalRow] = {
        iter.asInstanceOf[Iterator[InternalRow]]
      }

      private val parquetOutputFormat = new ParquetOutputFileFormat()

      override def hasNext: Boolean = iter.hasNext

      override def next(): CachedBatch = {
        // Each partition will be a single parquet file
        var rows = 0
        // at least a single block
        val stream = new ByteArrayOutputStream(ByteArrayOutputFile.BLOCK_SIZE)
        val outputFile: OutputFile = new ByteArrayOutputFile(stream)
        sharedConf.setConfString(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key,
          LegacyBehaviorPolicy.CORRECTED.toString)
        val recordWriter = SQLConf.withExistingConf(sharedConf) {
          parquetOutputFormat.getRecordWriter(outputFile, sharedHadoopConf)
        }
        val rowIterator = getIterator
        while (rowIterator.hasNext) {
          rows += 1
          if (rows < 0) {
            throw new IllegalStateException("CachedBatch doesn't support rows larger " +
              "than Int.MaxValue")
          }
          val row = rowIterator.next()
          recordWriter.write(null, row)
        }
        // passing null as context isn't used in this method
        recordWriter.close(null)
        ParquetCachedBatch(rows, stream.toByteArray)
      }
    }

    /**
     *
     */
    private class ColumnarBatchToCachedBatchIterator extends InternalRowToCachedBatchIterator {
      override def getIterator: Iterator[InternalRow] = {
        iter.asInstanceOf[Iterator[ColumnarBatch]].next.rowIterator().asScala
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

    hadoopConf.set(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key,
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

    val parquetSchema = getCatalystSchema(schema, schema)

    val rapidsConf = new RapidsConf(conf)

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
      val broadcastedHadoopConf = getBroadcastedHadoopConf(conf, parquetSchema)
      val broadcastedConf = SparkSession.active.sparkContext.broadcast(conf)
      // fallback to the CPU
      input.mapPartitions {
        cbIter =>
          new CachedBatchIteratorProducer[InternalRow](cbIter, broadcastedHadoopConf.value.value,
            broadcastedConf.value).getInternalRowToCachedBatchIterator
      }
    }
  }

  private def getBroadcastedHadoopConf(
     conf: SQLConf,
     requestedSchema: Seq[Attribute]): Broadcast[SerializableConfiguration] = {
    val hadoopConf = getHadoopConf(requestedSchema.toStructType, conf)
    SparkSession.active.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override def buildFilter(
     predicates: Seq[Expression],
     cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    //essentially a noop
    (partId: Int, b: Iterator[CachedBatch]) => b
  }
}

/**
 * Similar to ParquetFileFormat
 */
private class ParquetOutputFileFormat {

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
  var memoryManager: MemoryManager = null
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

