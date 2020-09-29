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

package com.nvidia.spark.rapids

import java.io._
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import ai.rapids.cudf.{HostColumnVector, JCudfSerialization, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Serializer for serializing `ColumnarBatch`s during shuffle.
 * The batches will be stored in an internal format specific to rapids.
 */
class GpuColumnarBatchSerializer(
      numFields: Int,
      dataSize: SQLMetric = null) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance =
    new GpuColumnarBatchSerializerInstance(numFields, dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class GpuColumnarBatchSerializerInstance(
    numFields: Int,
    dataSize: SQLMetric) extends SerializerInstance {

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val batch = value.asInstanceOf[ColumnarBatch]
      val numColumns = batch.numCols()
      val columns: Array[HostColumnVector] = new Array(numColumns)
      val toClose = new ArrayBuffer[AutoCloseable]()
      try {
        var startRow = 0
        val numRows = batch.numRows()
        if (batch.numCols() > 0) {
          val firstCol = batch.column(0)
          if (firstCol.isInstanceOf[SlicedGpuColumnVector]) {
            // We don't have control over ColumnarBatch to put in the slice, so we have to do it
            // for each column.  In this case we are using the first column.
            startRow = firstCol.asInstanceOf[SlicedGpuColumnVector].getStart
            for (i <- 0 until numColumns) {
              columns(i) = batch.column(i).asInstanceOf[SlicedGpuColumnVector].getBase
            }
          } else {
            for (i <- 0 until numColumns) {
              batch.column(i) match {
                case gpu: GpuColumnVector =>
                  val cpu = gpu.copyToHost()
                  toClose += cpu
                  columns(i) = cpu.getBase
                case cpu: RapidsHostColumnVector =>
                  columns(i) = cpu.getBase
              }
            }
          }

          if (dataSize != null) {
            dataSize.add(JCudfSerialization.getSerializedSizeInBytes(columns, startRow, numRows))
          }
          val range = new NvtxRange("Serialize Batch", NvtxColor.YELLOW)
          try {
            JCudfSerialization.writeToStream(columns, dOut, startRow, numRows)
          } finally {
            range.close()
          }
        } else {
          val range = new NvtxRange("Serialize Row Only Batch", NvtxColor.YELLOW)
          try {
            JCudfSerialization.writeRowsToStream(dOut, numRows)
          } finally {
            range.close()
          }
        }
      } finally {
        toClose.safeClose()
      }
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new Iterator[(Int, ColumnarBatch)] {
          var toBeReturned: Option[ColumnarBatch] = None

          TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
            toBeReturned.foreach(_.close())
            toBeReturned = None
            dIn.close()
          })

          def tryReadNext(): Option[ColumnarBatch] = {
            // about to start using the GPU in this task
            GpuSemaphore.acquireIfNecessary(TaskContext.get())

            val range = new NvtxRange("Deserialize Batch", NvtxColor.YELLOW)
            try {
              val tableInfo = JCudfSerialization.readTableFrom(dIn)
              try {
                val table = tableInfo.getTable
                if (table == null && tableInfo.getNumRows == 0) {
                  dIn.close()
                  None
                } else {
                  if (table != null) {
                    Some(GpuColumnVector.from(table))
                  } else {
                    Some(new ColumnarBatch(Array.empty, tableInfo.getNumRows))
                  }
                }
              } finally {
                tableInfo.close()
              }
            } finally {
              range.close()
            }
          }

          override def hasNext: Boolean = {
            if (toBeReturned.isEmpty) {
              toBeReturned = tryReadNext()
            }

            toBeReturned.isDefined
          }

          override def next(): (Int, ColumnarBatch) = {
            if (toBeReturned.isEmpty) {
              toBeReturned = tryReadNext()
              if (toBeReturned.isEmpty) {
                throw new NoSuchElementException("Walked off of the end...")
              }
            }
            val ret = toBeReturned.get
            toBeReturned = None
            (0, ret)
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T]()(implicit classType: ClassTag[T]): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T]()(implicit classType: ClassTag[T]): T = {
        // about to start using the GPU in this task
        GpuSemaphore.acquireIfNecessary(TaskContext.get())

        val range = new NvtxRange("Deserialize Batch", NvtxColor.YELLOW)
        try {
          val tableInfo = JCudfSerialization.readTableFrom(dIn)
          val cb = try {
            val table = tableInfo.getTable
            if (table != null) {
              Some(GpuColumnVector.from(table))
            } else {
              Some(new ColumnarBatch(Array.empty, tableInfo.getNumRows))
            }
          } finally {
            tableInfo.close()
          }
          cb.asInstanceOf[T]
        } finally {
          range.close()
        }
      }

      override def readObject[T]()(implicit classType: ClassTag[T]): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
