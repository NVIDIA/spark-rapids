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

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

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

  override def supportsColumnarOutput(schema: StructType): Boolean = true

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
    def putOnGpuIfNeeded(batch: ColumnarBatch): ColumnarBatch = {
      if (batch.numCols() > 0 && !batch.column(0).isInstanceOf[GpuColumnVector]) {
        val s = StructType(
          schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
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

    val requestedColumnIndices = selectedAttributes.map(a =>
      cacheAttributes.map(_.exprId).indexOf(a.exprId))

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

  /**
   * Convert the cached data into a ColumnarBatch taking the result data back to the host
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
    cbRdd.mapPartitions(iter => new CloseableColumnBatchIterator(iter))
  }

  /**
   * Convert the cached batch into `InternalRow`s.
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
    val cb = convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    val rowRdd = cb.mapPartitions(iter => {
      new ColumnarToRowIterator(iter)
    })
    rowRdd
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
