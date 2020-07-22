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

package com.nvidia.spark.rapids.cache

import scala.collection.mutable

import ai.rapids.cudf._
import com.nvidia.spark.rapids.{Arm, GpuBatchUtils, GpuColumnVector, GpuMetricNames, GpuRowToColumnConverter, GpuSemaphore, NvtxWithMetrics, RequireSingleBatch, RowToColumnarIterator}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.GpuMetricNames.{NUM_INPUT_ROWS, NUM_OUTPUT_BATCHES, NUM_OUTPUT_ROWS, TOTAL_TIME}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

class ParquetCachedBatch(val numRows: Int) extends CachedBatch with HostBufferConsumer
  with AutoCloseable with Serializable {
  @transient private[this] val offHeapBuffers = mutable.Queue[(HostMemoryBuffer, Long)]()
  private[this] var buffer: Array[Byte] = null
  private var bytes: Long = 0

  override def sizeInBytes: Long = {
    if (bytes == 0) {
      val toProcess = offHeapBuffers.dequeueAll(_ => true)
      bytes = toProcess.unzip._2.sum
    }
    bytes
  }

  override def handleBuffer(hostMemoryBuffer: HostMemoryBuffer, len: Long): Unit = {
    offHeapBuffers += Tuple2(hostMemoryBuffer, len)
    bytes += len
  }

  def getBuffer(): Array[Byte] = {
    if (buffer == null) {
      writeBuffers()
    }
    buffer
  }

  def close(): Unit = {
    writeBuffers()
  }

  def writeBuffers(): Unit = {
    // this could be problematic if the buffers are big as their cumulative length could be more
    // than an Int.MAX_SIZE. We could just have a list of buffers in that case and iterate over them
    buffer = new Array(sizeInBytes.toInt)
    val toProcess = offHeapBuffers.dequeueAll(_ => true)
    try {
      var offset: Int = 0
      toProcess.foreach(ops => {
        val origBuffer = ops._1
        val len = ops._2.toInt
        origBuffer.asByteBuffer().get(buffer, offset, len)
        for (i <- 0  until len) {
          assert(origBuffer.getByte(i) == buffer(offset + i.toInt))
        }
        offset = offset + len
      })
    } finally {
      toProcess.map(_._1).safeClose()
    }
  }
}

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

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch],
                                       schema: Seq[Attribute],
                                       storageLevel: StorageLevel,
                                       conf: SQLConf): RDD[CachedBatch] = {
    // if plugin enabled
    // if data is not on gpu
    // push data on to the gpu
    val parquetCB = input.map(batch => {
      var gpuCB: ColumnarBatch = null
      try {
        // check if the data is already on GPU
        if (!batch.column(0).isInstanceOf[GpuColumnVector]) {
          val s = StructType(
            schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          gpuCB = new GpuColumnarBatchBuilder(s, batch.numRows(), batch).build(batch.numRows())
          batch.close()
        } else {
          gpuCB = batch
        }
        // now compress it using ParquetWriter
        compressColumnarBatchWithParquet(gpuCB)
      } finally {
        gpuCB.close()
      }
    })
    parquetCB
  }

  private def compressColumnarBatchWithParquet(gpuCB: ColumnarBatch) = {
    val buffer = new ParquetCachedBatch(gpuCB.numRows())
    withResource(GpuColumnVector.from(gpuCB)) { table =>
      withResource(Table.writeParquetChunked(ParquetWriterOptions.DEFAULT, buffer)) { writer =>
        writer.write(table)
      }
    }
    buffer.writeBuffers()
    buffer.asInstanceOf[CachedBatch]
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch],
                                  cacheAttributes: Seq[Attribute],
                                  selectedAttributes: Seq[Attribute],
                                  conf: SQLConf): RDD[ColumnarBatch] = {
    val requestedColumnIndices = selectedAttributes.map(a =>
      cacheAttributes.map(_.exprId).indexOf(a.exprId))

    // if plugin enabled
    val cbRdd: RDD[ColumnarBatch] = input.map(batch => {
      if (batch.isInstanceOf[ParquetCachedBatch]) {
        val parquetCB = batch.asInstanceOf[ParquetCachedBatch]
        withResource(Table.readParquet(ParquetOptions.DEFAULT, parquetCB.getBuffer(), 0,
          parquetCB.sizeInBytes)) { table =>
          withResource(GpuColumnVector.from(table)) { cb =>
            val cols = GpuColumnVector.extractColumns(cb)
            val batch = new ColumnarBatch(requestedColumnIndices.map(ordinal =>
              cols(ordinal)).map(_.copyToHost()).toArray, cb.numRows())
            batch
          }
        }
      } else {
        throw new IllegalStateException("I don't know how to convert this batch")
      }
    })
    cbRdd.mapPartitions(iter => CloseableColumnBatchIterator(iter))
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch],
                                cacheAttributes: Seq[Attribute],
                                selectedAttributes: Seq[Attribute],
                                conf: SQLConf): RDD[InternalRow] = {
    val requestedColumnIndices = selectedAttributes.map(a =>
      cacheAttributes.map(_.exprId).indexOf(a.exprId))
    // if plugin enabled
    val cbRdd: RDD[InternalRow] = input.map(batch => {
      if (batch.isInstanceOf[ParquetCachedBatch]) {
        val parquetCB = batch.asInstanceOf[ParquetCachedBatch]
        withResource(Table.readParquet(ParquetOptions.DEFAULT, parquetCB.getBuffer(), 0,
          parquetCB.sizeInBytes)) { table =>
          withResource(GpuColumnVector.from(table)) { cb =>
            val cols = GpuColumnVector.extractColumns(cb)
            cols.foreach(col => col.close())
            InternalRow(requestedColumnIndices.map(ordinal => cols(ordinal)).map(_.copyToHost()))
          }
        }
      } else {
        throw new IllegalStateException("I don't know how to convert this batch")
      }
    })
    cbRdd
  }

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow],
     schema: Seq[Attribute],
     storageLevel: StorageLevel,
     conf: SQLConf): RDD[CachedBatch] = {
//    input.foreach(row => println((row.get(0, DataTypes.ShortType), row.get(1, DataTypes.ShortType)
//    , row.get(2, DataTypes.ShortType), row.get(3, DataTypes.ShortType))))
    //convert to columnar
    val s = StructType(schema.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
    val converters = new GpuRowToColumnConverter(s)
    val numInputRows = SQLMetrics.createMetric(input.context, NUM_INPUT_ROWS)
    val numOutputBatches = SQLMetrics.createMetric(input.context, NUM_OUTPUT_BATCHES)
    val numOutputRows = SQLMetrics.createMetric(input.context, NUM_OUTPUT_ROWS)
    val totalTime = SQLMetrics.createMetric(input.context, TOTAL_TIME)
    val columnarBatchRdd = input.mapPartitions(iter => {
      new RowToColumnarIterator(iter, s, RequireSingleBatch, converters, totalTime, numInputRows,
        numOutputRows, numOutputBatches)
    })
    columnarBatchRdd.map(cb => {
      val cachedBatch = compressColumnarBatchWithParquet(cb)
      cb.close()
      cachedBatch
    })
  }

  override def buildFilter(predicates: Seq[Expression],
     cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
        //essentially a noop
        (partId: Int, b: Iterator[CachedBatch]) => b
  }
}


//class RowToColumnIterator(rowIter: Iterator[InternalRow]) extends Iterator[ColumnarBatch] {
//  override def hasNext: Boolean = rowIter.hasNext
//
//  override def next(): ColumnarBatch = {
//    if (!rowIter.hasNext) {
//      throw new NoSuchElementException
//    }
//    buildBatch()
//  }
//
//  private def buildBatch(): ColumnarBatch = {
//
//    val targetRows = 1024
//
//    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows, null)
//    try {
//      var rowCount = 0
//      var byteCount: Long = variableWidthColumnCount * 4 // offset bytes
//      // read at least one row
//      while (rowIter.hasNext &&
//        (rowCount == 0 || rowCount < targetRows && byteCount < targetSizeBytes)) {
//        val row = rowIter.next()
//        val variableWidthDataBytes = converters.convert(row, builders)
//        byteCount += fixedWidthDataSizePerRow // fixed-width data bytes
//        byteCount += variableWidthDataBytes // variable-width data bytes
//        byteCount += variableWidthColumnCount * GpuBatchUtils.OFFSET_BYTES // offset bytes
//        if (nullableColumns > 0 && rowCount % GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS == 0) {
//          byteCount += GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_BYTES * nullableColumns
//        }
//        rowCount += 1
//      }
//
//      // enforce RequireSingleBatch limit
//      if (rowIter.hasNext && localGoal == RequireSingleBatch) {
//        throw new IllegalStateException("A single batch is required for this operation." +
//          " Please try increasing your partition count.")
//      }
//
//      // About to place data back on the GPU
//      // note that TaskContext.get() can return null during unit testing so we wrap it in an
//      // option here
//      Option(TaskContext.get()).foreach(GpuSemaphore.acquireIfNecessary)
//
//      val buildRange = new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN, totalTime)
//      val ret = try {
//        builders.build(rowCount)
//      } finally {
//        buildRange.close()
//      }
//      numInputRows += rowCount
//      numOutputRows += rowCount
//      numOutputBatches += 1
//
//      // refine the targetRows estimate based on the average of all batches processed so far
//      totalOutputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
//      totalOutputRows += rowCount
//      if (totalOutputRows > 0 && totalOutputBytes > 0) {
//        targetRows =
//          GpuBatchUtils.estimateRowCount(targetSizeBytes, totalOutputBytes, totalOutputRows)
//      }
//
//      // The returned batch will be closed by the consumer of it
//      ret
//    } finally {
//      builders.close()
//    }
//  }
//}
