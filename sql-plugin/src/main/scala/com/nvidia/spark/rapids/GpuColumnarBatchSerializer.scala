/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{HostColumnVector, HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkColumnVector}

class SerializedBatchIterator(dIn: DataInputStream, deserTime: GpuMetric
) extends Iterator[(Int, ColumnarBatch)] {
  private[this] var nextHeader: Option[SerializedTableHeader] = None
  private[this] var toBeReturned: Option[ColumnarBatch] = None
  private[this] var streamClosed: Boolean = false

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      toBeReturned.foreach(_.close())
      toBeReturned = None
      dIn.close()
    }
  }

  def tryReadNextHeader(): Option[Long] = {
    if (streamClosed){
      None
    } else {
      if (nextHeader.isEmpty) {
        withResource(new NvtxRange("Read Header", NvtxColor.YELLOW)) { _ =>
          val header = new SerializedTableHeader(dIn)
          if (header.wasInitialized) {
            nextHeader = Some(header)
          } else {
            dIn.close()
            streamClosed = true
            nextHeader = None
          }
        }
      }
      nextHeader.map(_.getDataLen)
    }
  }

  def tryReadNext(): Option[ColumnarBatch] = {
    if (nextHeader.isEmpty) {
      None
    } else {
      withResource(new NvtxRange("Read Batch", NvtxColor.YELLOW)) { _ =>
        val header = nextHeader.get
        if (header.getNumColumns > 0) {
          // This buffer will later be concatenated into another host buffer before being
          // sent to the GPU, so no need to use pinned memory for these buffers.
          closeOnExcept(
            HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
            JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
            Some(SerializedTableColumn.from(header, hostBuffer))
          }
        } else {
          Some(SerializedTableColumn.from(header))
        }
      }
    }
  }

  override def hasNext: Boolean = {
    deserTime.ns(tryReadNextHeader())
    nextHeader.isDefined
  }

  override def next(): (Int, ColumnarBatch) = {
    if (toBeReturned.isEmpty) {
      deserTime.ns {
        tryReadNextHeader()
        toBeReturned = tryReadNext()
      }
      if (nextHeader.isEmpty || toBeReturned.isEmpty) {
        throw new NoSuchElementException("Walked off of the end...")
      }
    }
    val ret = toBeReturned.get
    toBeReturned = None
    nextHeader = None
    (0, ret)
  }
}

/**
 * Serializer for serializing `ColumnarBatch`s for use during normal shuffle. And it supports
 * two types of batch as input.
 *
 * Type 1 (isSerializedTable == false):
 * The serialization write path takes the cudf `Table` that is described by the `ColumnarBatch`
 * and uses cudf APIs to serialize the data into a sequence of bytes on the host. The data is
 * returned to the Spark shuffle code where it is compressed by the CPU and written to disk.
 *
 * The serialization read path is notably different. The sequence of serialized bytes IS NOT
 * deserialized into a cudf `Table` but rather tracked in host memory by a `ColumnarBatch`
 * that contains a [[SerializedTableColumn]]. During query planning, each GPU columnar shuffle
 * exchange is followed by a [[GpuShuffleCoalesceExec]] that expects to receive only these
 * custom batches of [[SerializedTableColumn]]. [[GpuShuffleCoalesceExec]] coalesces the smaller
 * shuffle partitions into larger tables before placing them on the GPU for further processing.
 *
 * Type 2 (isSerializedTable == true)
 * The table inside an input ColumnBatch is already serialized and placed on host by upstream
 * operators. So the serializer writes it to the output stream directly, along with a
 * lightweight metadata. (a serializable TableMeta).
 *
 * The deserialization path will read the metadata and serialized table back and move them
 * to device to rebuild the cudf table. During query planning, each GPU columnar shuffle
 * exchange is followed by a [[GpuCoalesceBatches]] that expects to receive the returned
 * batches, decompress them when needed, and coalesces the smaller shuffle partitions into
 * larger tables before sending them to downstream operators for further processing.
 *
 * @note The RAPIDS shuffle does not use this code.
 */
class GpuColumnarBatchSerializer(dataSize: GpuMetric,
    serTime: GpuMetric = NoopMetric,
    deserTime: GpuMetric = NoopMetric,
    isSerializedTable: Boolean = false,
    sparkTypes: Array[DataType] = Array.empty,
    bundleSize: Long = 0L) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance =
    new GpuColumnarBatchSerializerInstance(dataSize, serTime, deserTime,
      isSerializedTable, sparkTypes, bundleSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class GpuColumnarBatchSerializerInstance(dataSize: GpuMetric, serTime: GpuMetric,
    deserTime: GpuMetric, isSerializedTable: Boolean, sparkTypes: Array[DataType],
    bundleSize: Long) extends SerializerInstance {

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut = new DataOutputStream(new BufferedOutputStream(out))
    private[this] val tableSerializer = new PackedTableSerializer()

    private lazy val serializeBatch: ColumnarBatch => Unit = if (isSerializedTable) {
      serializeGpuBatch
    } else {
      serializeCpuBatch
    }

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      serTime.ns(withResource(value.asInstanceOf[ColumnarBatch])(serializeBatch))
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

    private def serializeCpuBatch(batch: ColumnarBatch): Unit = {
      val numRows = batch.numRows()
      val numCols = batch.numCols()
      if (numCols > 0) {
        withResource(new ArrayBuffer[AutoCloseable]()) { toClose =>
          var startRow = 0
          val cols = closeOnExcept(batch) { _ =>
            val toHostCol: SparkColumnVector => HostColumnVector = batch.column(0) match {
              case sliced: SlicedGpuColumnVector =>
                // We don't have control over ColumnarBatch to put in the slice, so we have
                // to do it for each column.  In this case we are using the first column.
                startRow = sliced.getStart
                col => col.asInstanceOf[SlicedGpuColumnVector].getBase
              case _: GpuColumnVector =>
                col => {
                  val hCol = col.asInstanceOf[GpuColumnVector].copyToHost()
                  toClose += hCol
                  hCol.getBase
                }
              case _: RapidsHostColumnVector =>
                col => col.asInstanceOf[RapidsHostColumnVector].getBase
            }
            (0 until numCols).map(i => toHostCol(batch.column(i))).toArray
          }
          dataSize += JCudfSerialization.getSerializedSizeInBytes(cols, startRow, numRows)
          withResource(new NvtxRange("Serialize Batch", NvtxColor.YELLOW)) { _ =>
            JCudfSerialization.writeToStream(cols, dOut, startRow, numRows)
          }
        }
      } else { // Rows only batch
        withResource(new NvtxRange("Serialize Row Only Batch", NvtxColor.YELLOW)) { _ =>
          JCudfSerialization.writeRowsToStream(dOut, numRows)
        }
      }
    }

    private def serializeGpuBatch(batch: ColumnarBatch): Unit = {
      val packedCol = if (batch.numCols() == 0) {
        val tableMeta = MetaUtils.buildDegenerateTableMeta(batch)
        new PackedTableHostColumnVector(tableMeta, null)
      } else {
        require(PackedTableHostColumnVector.isBatchPackedOnHost(batch))
        batch.column(0).asInstanceOf[PackedTableHostColumnVector]
      }
      dataSize += tableSerializer.writeToStream(packedCol, dOut)
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        if (isSerializedTable) {
          new PackedTableIterator(dIn, sparkTypes, bundleSize, deserTime)
        } else {
          new SerializedBatchIterator(dIn, deserTime)
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
        // This method should never be called by shuffle code.
        throw new UnsupportedOperationException
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

/**
 * A special `ColumnVector` that describes a serialized table read from shuffle.
 * This appears in a `ColumnarBatch` to pass serialized tables to [[GpuShuffleCoalesceExec]]
 * which should always appear in the query plan immediately after a shuffle.
 */
class SerializedTableColumn(
    val header: SerializedTableHeader,
    val hostBuffer: HostMemoryBuffer) extends GpuColumnVectorBase(NullType) {
  override def close(): Unit = {
    if (hostBuffer != null) {
      hostBuffer.close()
    }
  }

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")
}

object SerializedTableColumn {
  /**
   * Build a `ColumnarBatch` consisting of a single [[SerializedTableColumn]] describing
   * the specified serialized table.
   *
   * @param header header for the serialized table
   * @param hostBuffer host buffer containing the table data
   * @return columnar batch to be passed to [[GpuShuffleCoalesceExec]]
   */
  def from(
      header: SerializedTableHeader,
      hostBuffer: HostMemoryBuffer = null): ColumnarBatch = {
    val column = new SerializedTableColumn(header, hostBuffer)
    new ColumnarBatch(Array(column), header.getNumRows)
  }

  def getMemoryUsed(batch: ColumnarBatch): Long = {
    var sum: Long = 0
    if (batch.numCols == 1) {
      val cv = batch.column(0)
      cv match {
        case serializedTableColumn: SerializedTableColumn
            if serializedTableColumn.hostBuffer != null =>
          sum += serializedTableColumn.hostBuffer.getLength
        case _ =>
      }
    }
    sum
  }
}
