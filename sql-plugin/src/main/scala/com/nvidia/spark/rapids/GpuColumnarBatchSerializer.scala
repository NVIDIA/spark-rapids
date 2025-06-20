/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{Cuda, HostColumnVector, HostMemoryBuffer, JCudfSerialization, NvtxColor, NvtxRange}
import ai.rapids.cudf.JCudfSerialization.SerializedTableHeader
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRetryNoSplit, SizeProvider}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.jni.kudo.{KudoSerializer, KudoTableHeader, WriteInput}

import org.apache.spark.TaskContext
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.rapids.execution.GpuShuffleExchangeExecBase.{METRIC_DATA_SIZE, METRIC_SHUFFLE_DESER_STREAM_TIME, METRIC_SHUFFLE_SER_COPY_BUFFER_TIME, METRIC_SHUFFLE_SER_STREAM_TIME}
import org.apache.spark.sql.types.{DataType, NullType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Iterator that reads serialized tables from a stream.
 */
trait BaseSerializedTableIterator extends Iterator[(Int, ColumnarBatch)] {
  /**
   * Attempt to read the next batch size from the stream.
   * @return the length of the data to read, or None if the stream is closed or ended
   */
  def peekNextBatchSize(): Option[Long]
}

class SerializedBatchIterator(dIn: DataInputStream, deserTime: GpuMetric)
  extends BaseSerializedTableIterator {
  private[this] var nextHeader: Option[SerializedTableHeader] = None
  private[this] var streamClosed: Boolean = false

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      dIn.close()
      streamClosed = true
    }
  }

  override def peekNextBatchSize(): Option[Long] = deserTime.ns {
    if (streamClosed) {
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

  private def readNextBatch(): ColumnarBatch = deserTime.ns {
    withResource(new NvtxRange("Read Batch", NvtxColor.YELLOW)) { _ =>
      val header = nextHeader.get
      nextHeader = None
      if (header.getNumColumns > 0) {
        // This buffer will later be concatenated into another host buffer before being
        // sent to the GPU, so no need to use pinned memory for these buffers.
        closeOnExcept(
          HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
          JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
          SerializedTableColumn.from(header, hostBuffer)
        }
      } else {
        SerializedTableColumn.from(header)
      }
    }
  }

  override def hasNext: Boolean = {
    peekNextBatchSize()
    nextHeader.isDefined
  }

  override def next(): (Int, ColumnarBatch) = {
    if (!hasNext) {
      throw new NoSuchElementException("Walked off of the end...")
    }
    (0, readNextBatch())
  }
}

/**
 * Serializer for serializing `ColumnarBatch`s for use during normal shuffle.
 *
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
 * @note The RAPIDS shuffle does not use this code.
 */
class GpuColumnarBatchSerializer(metrics: Map[String, GpuMetric], dataTypes: Array[DataType],
    useKudo: Boolean, kudoMeasureBufferCopy: Boolean)
  extends Serializer with Serializable {

  private lazy val kudo = {
    if (useKudo && dataTypes.nonEmpty) {
      Some(new KudoSerializer(GpuColumnVector.from(dataTypes)))
    } else {
      None
    }
  }

  override def newInstance(): SerializerInstance = {
    if (useKudo) {
      new KudoSerializerInstance(metrics, dataTypes, kudo, kudoMeasureBufferCopy)
    } else {
      new GpuColumnarBatchSerializerInstance(metrics)
    }
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class GpuColumnarBatchSerializerInstance(metrics: Map[String, GpuMetric]) extends
  SerializerInstance {
  private val dataSize = metrics(METRIC_DATA_SIZE)
  private val serTime = metrics(METRIC_SHUFFLE_SER_STREAM_TIME)
  private val deserTime = metrics(METRIC_SHUFFLE_DESER_STREAM_TIME)


  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = serTime.ns {
      val batch = value.asInstanceOf[ColumnarBatch]
      val numColumns = batch.numCols()
      val columns: Array[HostColumnVector] = new Array(numColumns)
      val toClose = new ArrayBuffer[AutoCloseable](numColumns)
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

          dataSize += JCudfSerialization.getSerializedSizeInBytes(columns, startRow, numRows)
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
        new SerializedBatchIterator(dIn, deserTime)
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
    val hostBuffer: HostMemoryBuffer) extends GpuColumnVectorBase(NullType) with SizeProvider {
  override def close(): Unit = {
    if (hostBuffer != null) {
      hostBuffer.close()
    }
  }

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")

  override def sizeInBytes: Long = if (hostBuffer == null) {
    0L
  } else {
    hostBuffer.getLength
  }
}

object SerializedTableColumn {
  /**
   * Build a `ColumnarBatch` consisting of a single [[SerializedTableColumn]] describing
   * the specified serialized table.
   *
   * @param header     header for the serialized table
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
        case serializedTableColumn: SerializedTableColumn =>
          sum += Option(serializedTableColumn.hostBuffer).map(_.getLength).getOrElse(0L)
        case kudo: KudoSerializedTableColumn =>
          sum += Option(kudo.spillableKudoTable).map(_.length).getOrElse(0L)
        case _ =>
          throw new IllegalStateException(s"Unexpected column type: ${cv.getClass}" )
      }
    }
    sum
  }
}

/**
 * Serializer instance for serializing `ColumnarBatch`s for use during shuffle with
 * [[KudoSerializer]]
 *
 * @param dataSize  metric to track the size of the serialized data
 * @param dataTypes data types of the columns in the batch
 */
private class KudoSerializerInstance(
    val metrics: Map[String, GpuMetric],
    val dataTypes: Array[DataType],
    val kudo: Option[KudoSerializer],
    val measureBufferCopyTime: Boolean,
) extends SerializerInstance {
  private val dataSize = metrics(METRIC_DATA_SIZE)
  private val serTime = metrics(METRIC_SHUFFLE_SER_STREAM_TIME)
  private val serCopyBufferTime = metrics(METRIC_SHUFFLE_SER_COPY_BUFFER_TIME)
  private val deserTime = metrics(METRIC_SHUFFLE_DESER_STREAM_TIME)

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {

    override def writeValue[T: ClassTag](value: T): SerializationStream = serTime.ns {
      val batch = value.asInstanceOf[ColumnarBatch]
      val numColumns = batch.numCols()
      val columns: Array[HostColumnVector] = new Array(numColumns)
      withResource(new ArrayBuffer[AutoCloseable](numColumns)) { toClose =>
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
                  val cpu = gpu.copyToHostAsync(Cuda.DEFAULT_STREAM)
                  toClose += cpu
                  columns(i) = cpu.getBase
                case cpu: RapidsHostColumnVector =>
                  columns(i) = cpu.getBase
              }
            }

            Cuda.DEFAULT_STREAM.sync()
          }

          withResource(new NvtxRange("Serialize Batch", NvtxColor.YELLOW)) { _ =>
            val writeInput = WriteInput.builder
              .setColumns(columns)
              .setOutputStream(out)
              .setNumRows(numRows)
              .setRowOffset(startRow)
              .setMeasureCopyBufferTime(measureBufferCopyTime)
              .build
            val writeMetric = kudo
              .getOrElse(throw new IllegalStateException("Kudo serializer not initialized."))
              .writeToStreamWithMetrics(writeInput)

            dataSize += writeMetric.getWrittenBytes
            if (measureBufferCopyTime) {
              // These metrics will not show up in the UI if it's not modified
              serCopyBufferTime += writeMetric.getCopyBufferTime
            }
          }
        } else {
          withResource(new NvtxRange("Serialize Row Only Batch", NvtxColor.YELLOW)) { _ =>
            dataSize += KudoSerializer.writeRowCountToStream(out, numRows)
          }
        }
        this
      }
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
      out.flush()
    }

    override def close(): Unit = {
      out.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))

      override def asKeyValueIterator: Iterator[(Int, ColumnarBatch)] = {
        new KudoSerializedBatchIterator(dIn, deserTime)
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
 * A special `ColumnVector` that describes a serialized table read from shuffle using
 * [[KudoSerializer]].
 *
 * This appears in a `ColumnarBatch` to pass serialized tables to [[GpuShuffleCoalesceExec]]
 * which should always appear in the query plan immediately after a shuffle.
 */
case class KudoSerializedTableColumn(spillableKudoTable: SpillableKudoTable)
  extends GpuColumnVectorBase(NullType) with SizeProvider {
  override def close(): Unit = {
    if (spillableKudoTable != null) {
      spillableKudoTable.close()
    }
  }

  override def hasNull: Boolean = throw new IllegalStateException("should not be called")

  override def numNulls(): Int = throw new IllegalStateException("should not be called")

  override def sizeInBytes: Long = if (spillableKudoTable == null) {
    0L
  } else {
    spillableKudoTable.length
  }
}

object KudoSerializedTableColumn {
  /**
   * Build a `ColumnarBatch` consisting of a single [[KudoSerializedTableColumn]] describing
   * the specified serialized table.
   *
   * @param header the header of the kudo table
   * @param hostBuffer the buffer for the kudo table
   * @return columnar batch to be passed to [[GpuShuffleCoalesceExec]]
   */
  def from(header: KudoTableHeader, hostBuffer: HostMemoryBuffer): ColumnarBatch = {
    val kudoTable = SpillableKudoTable(header, hostBuffer)
    val column = new KudoSerializedTableColumn(kudoTable)
    new ColumnarBatch(Array(column), kudoTable.header.getNumRows)
  }
}

class KudoSerializedBatchIterator(dIn: DataInputStream, deserTime: GpuMetric)
  extends BaseSerializedTableIterator {
  private[this] var nextHeader: Option[KudoTableHeader] = None
  private[this] var streamClosed: Boolean = false

  // When KudoSerializedBatchIterator is often handling small kudo tables, it is more efficient
  // to use a shared HostMemoryBuffer to avoid the overhead of allocating and deallocating, check
  // SparkResourceAdaptor.cpuDeallocate to understand the overhead.

  // Used to track the first 10 batch sizes
  // and decide if to use a shared HostMemoryBuffer or not.
  private[this] val firstTenBatchesSizes = new ArrayBuffer[Long](10)
  private[this] var sharedBuffer: Option[HostMemoryBuffer] = None
  private[this] val sharedBufferTriggerSize: Int = 1 << 20 // 1MB
  private[this] val sharedBufferTotalSize: Int = 20 << 20 // 20MB
  private[this] var sharedBufferCurrentUse: Int = 0

  // Don't install the callback if in a unit test
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      Seq[AutoCloseable](
        () => dIn.close(),
        () => streamClosed = true,
        () => sharedBuffer.map(_.close())
      ).safeClose()
    }
  }

  override def peekNextBatchSize(): Option[Long] = deserTime.ns {
    if (streamClosed) {
      None
    } else {
      if (nextHeader.isEmpty) {
        withResource(new NvtxRange("Read Header", NvtxColor.YELLOW)) { _ =>
          val header = Option(KudoTableHeader.readFrom(dIn).orElse(null))
          if (header.isDefined) {
            nextHeader = header
          } else {
            dIn.close()
            streamClosed = true
            nextHeader = None
          }
        }
      }
      nextHeader.map(_.getTotalDataLen)
    }
  }

  private def allocateHostWithRetry(size: Int): HostMemoryBuffer = {
    withRetryNoSplit[HostMemoryBuffer] {
      // This buffer will later be concatenated into another host buffer before being
      // sent to the GPU, so no need to use pinned memory for these buffers.
      HostMemoryBuffer.allocate(size, false)
    }
  }

  private def readNextBatch(): ColumnarBatch = deserTime.ns {
    withResource(new NvtxRange("Read Batch", NvtxColor.YELLOW)) { _ =>
      val header = nextHeader.get
      nextHeader = None
      if (header.getNumColumns > 0) {

        // When allocation fails, will roll back to the beginning of this withRetryNoSplit,
        // with previous batches saved in HostCoalesceIteratorBase.serializedTables.
        // The previous batches should be able to be spilled by itself.
        val buffer = {
          if (sharedBuffer.isEmpty) {
            val allocated = allocateHostWithRetry(header.getTotalDataLen)

            if (firstTenBatchesSizes.length < 10) {
              firstTenBatchesSizes += header.getTotalDataLen
              if (firstTenBatchesSizes.length == 10) {
                // If we have 10 batches, we can decide if to use a shared buffer or not.
                val maxSize = firstTenBatchesSizes.max
                if (maxSize < sharedBufferTriggerSize) {
                  // If the max size of the first 10 batches is less than the threshold,
                  // we can use a shared buffer.
                  sharedBuffer = Some(allocateHostWithRetry(sharedBufferTotalSize))
                  sharedBufferCurrentUse = 0
                }
              }
            }

            allocated
          } else {
            if (header.getTotalDataLen > sharedBufferTotalSize / 2) {
              // Too big to use shared buffer, this should rarely happen since
              // first ten batches all much smaller.
              allocateHostWithRetry(header.getTotalDataLen)
            } else {
              if (sharedBufferCurrentUse + header.getTotalDataLen > sharedBufferTotalSize) {
                // If not enough room left, we need to allocate a new shared buffer.
                sharedBuffer.get.close()
                sharedBufferCurrentUse = 0
                sharedBuffer = Some(allocateHostWithRetry(sharedBufferTotalSize))
              }
              val ret = sharedBuffer.get.slice(sharedBufferCurrentUse,
                header.getTotalDataLen)
              sharedBufferCurrentUse += header.getTotalDataLen
              ret
            }
          }
        }

        closeOnExcept(buffer) { _ =>
          buffer.copyFromStream(0, dIn, header.getTotalDataLen)
          KudoSerializedTableColumn.from(header, buffer)
        }
      } else {
        KudoSerializedTableColumn.from(header, null)
      }
    }
  }

  override def hasNext: Boolean = {
    peekNextBatchSize()
    nextHeader.isDefined
  }

  override def next(): (Int, ColumnarBatch) = {
    if (!hasNext) {
      throw new NoSuchElementException("Walked off of the end...")
    }
    (0, readNextBatch())
  }
}
