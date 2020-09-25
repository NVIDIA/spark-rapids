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

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.avro.util.ByteBufferInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile, SeekableInputStream}

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

class ByteArrayInputFile(buff: Array[Byte]) extends InputFile {

  override def getLength: Long = buff.length

  override def newStream(): SeekableInputStream = {
    // ParquetFileReader reads in little endian! :shrugs
    val byteBuffer = ByteBuffer.wrap(buff).order(ByteOrder.LITTLE_ENDIAN)
    new DelegatingSeekableInputStream(new ByteBufferInputStream(List(byteBuffer).asJava)) {
      override def getPos: Long = byteBuffer.position()

      override def seek(newPos: Long): Unit = {
        assert(newPos <= Int.MaxValue && newPos >= Int.MinValue)
        byteBuffer.position(newPos.toInt)
      }
    }
  }
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
    isSupportedByCudf(f.dataType) || isSupportedBySparkColumnar(f.dataType))

  private def isSupportedByCudf(dataType: DataType): Boolean =
      GpuColumnVector.isSupportedType(dataType)

  private def isSupportedBySparkColumnar(dataType: DataType): Boolean =
    // AtomicType check. Everything is supported besides Structs, Maps or UDFs
    dataType match {
      case TimestampType | StringType | BooleanType | DateType | BinaryType |
           DoubleType | FloatType | ByteType | IntegerType | LongType | ShortType => true
      case _: HiveStringType => true
      case _: DecimalType => true
      case _ => false
    }

  /**
   * Convert an `RDD[ColumnarBatch]` into an `RDD[CachedBatch]` in preparation for caching the data.
   * This method uses Parquet Writer on the GPU to write the cached batch
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
    if (rapidsConf.isSqlEnabled) {
      def putOnGpuIfNeeded(batch: ColumnarBatch): ColumnarBatch = {
        if (batch.numCols() > 0 && !batch.column(0).isInstanceOf[GpuColumnVector]) {
          val s: StructType = getSchemaFromAttributeSeq(schema)
          val gpuCB = new GpuColumnarBatchBuilder(s, batch.numRows(), batch).build(batch.numRows())
          batch.close()
          gpuCB
        } else {
          batch
        }
      }

      input.map(batch => {
        withResource(putOnGpuIfNeeded(batch)) { gpuCB =>
          compressColumnarBatchWithParquet(gpuCB)
        }
      })
    } else {
      throw new UnsupportedOperationException("Can't convert ColumnarBatch to CachedBatch on CPU")
    }
  }

  private def getSchemaFromAttributeSeq(schema: Seq[Attribute]) = {
    StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
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
    selectedAttributes: Seq[Attribute]) = {

    val requestedColumnIndices = getColumnIndices(selectedAttributes)

    val cbRdd: RDD[ColumnarBatch] = input.map(batch => {
      if (batch.isInstanceOf[ParquetCachedBatch]) {
        val parquetCB = batch.asInstanceOf[ParquetCachedBatch]
        val parquetOptions = ParquetOptions.builder().includeColumn(requestedColumnIndices
           .map(i => "_col"+i).asJavaCollection).build()
        withResource(Table.readParquet(parquetOptions, parquetCB.buffer, 0,
          parquetCB.sizeInBytes)) { table =>
          withResource(GpuColumnVector.from(table)) { cb =>
            val cols = GpuColumnVector.extractColumns(cb)
            new ColumnarBatch(requestedColumnIndices.map(ordinal =>
              cols(ordinal).incRefCount()).toArray, cb.numRows())
          }
        }
      } else {
        throw new IllegalStateException("I don't know how to convert this batch")
      }
    })
    cbRdd
  }

  private def getColumnIndices(selectedAttributes: Seq[Attribute]) = {
    selectedAttributes.map(a => selectedAttributes.map(_.exprId).indexOf(a.exprId))
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
  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
     cacheAttributes: Seq[Attribute],
     selectedAttributes: Seq[Attribute],
     conf: SQLConf): RDD[InternalRow] = {
    val rapidsConf = new RapidsConf(conf)
    if (rapidsConf.isSqlEnabled) {
      val cb = convertCachedBatchToColumnarInternal(input, cacheAttributes, selectedAttributes)
      val rowRdd = cb.mapPartitions(iter => {
        new ColumnarToRowIterator(iter)
      })
      rowRdd
    } else {
      convertCachedBatchToInternalRowCpu(input, conf, cacheAttributes, selectedAttributes)
    }
  }

  private def convertCachedBatchToInternalRowCpu(input: RDD[CachedBatch], sqlConf: SQLConf,
     cacheAttributes: Seq[Attribute],
     selectedAttributes: Seq[Attribute]): RDD[InternalRow] = {

    val sparkSession = SparkSession.active
    val hadoopConf = getHadoopConf(getSchemaFromAttributeSeq(selectedAttributes), sqlConf)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    input.mapPartitions {
      cbIter => {
        new Iterator[InternalRow]() {
          var iter: Iterator[InternalRow] = null

          override def hasNext: Boolean = (iter != null && iter.hasNext) || cbIter.hasNext

          override def next(): InternalRow = {
            if (iter == null || !iter.hasNext) {
              iter = convertColumnarBatchToInternalRowIter()
            }
            if (iter.hasNext) {
              iter.next()
            } else {
              InternalRow.empty
            }
          }

          def convertColumnarBatchToInternalRowIter(): Iterator[InternalRow] = {
            val parquetCachedBatch = cbIter.next().asInstanceOf[ParquetCachedBatch]
            val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)

            val catalystColumns =
              getColumnIndices(cacheAttributes).map { i => "_col" + i }

            val catalystSchema = cacheAttributes.zipWithIndex.map {
              case (attr, i) => attr.withName(catalystColumns(i))
            }

            val selectedColumns =
              getColumnIndices(selectedAttributes).map { i => "_col" + i }

            val requestedSchema = selectedAttributes.zipWithIndex.map {
              case (attr, i) => attr.withName(selectedColumns(i))
            }

            val sharedConf = broadcastedHadoopConf.value.value
            val options = HadoopReadOptions.builder(sharedConf).build()
            val parquetFileReader = ParquetFileReader.open(inputFile, options)
            val footerFileMetaData = parquetFileReader.getFileMetaData
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
                  getSchemaFromAttributeSeq(catalystSchema),
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
              GenerateUnsafeProjection.generate(requestedSchema, requestedSchema)
            iter.map(unsafeProjection)
          }
        }
      }
    }
  }

  private def getHadoopConf(requestedSchema: StructType,
     sqlConf: SQLConf) : Configuration = {

    val hadoopConf = new Configuration()
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

    hadoopConf.set(
      SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key, LegacyBehaviorPolicy.CORRECTED.toString)

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
  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
     schema: Seq[Attribute],
     storageLevel: StorageLevel,
     conf: SQLConf): RDD[CachedBatch] = {
    val s = StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val converters = new GpuRowToColumnConverter(s)
    val columnarBatchRdd = input.mapPartitions(iter => {
      new RowToColumnarIterator(iter, s, RequireSingleBatch, converters)
    })
    columnarBatchRdd.map(cb => {
      withResource(cb) { columnarBatch =>
        val cachedBatch = compressColumnarBatchWithParquet(columnarBatch)
        cachedBatch
      }
    })
  }

  override def buildFilter(predicates: Seq[Expression],
     cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
        //essentially a noop
        (partId: Int, b: Iterator[CachedBatch]) => b
  }
}
