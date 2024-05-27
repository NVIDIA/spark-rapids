/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

import java.io.{DataInputStream, DataOutputStream, EOFException}
import java.nio.ByteBuffer

import scala.collection.mutable

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.{AutoCloseableColumn, AutoCloseableSeq}
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.format.TableMeta

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

private sealed trait TableSerde {
  protected val P_MAGIC_NUM: Int = 0x43554447 // "CUDF".asInt + 1
  protected val P_VERSION: Int = 0
  protected val headerLen = 8 // the size in bytes of two Ints for a header

  // buffers for reuse, so it should be only one instance of this trait per thread.
  protected val tmpBuf = new Array[Byte](1024 * 64) // 64k
}

private[rapids] class PackedTableSerializer extends TableSerde {
  private def writeByteBufferToStream(bBuf: ByteBuffer, dOut: DataOutputStream): Unit = {
    // Write the buffer size first
    val bufLen = bBuf.capacity()
    dOut.writeLong(bufLen.toLong)
    if (bBuf.hasArray) {
      dOut.write(bBuf.array())
    } else { // Probably a direct buffer
      var leftLen = bufLen
      while (leftLen > 0) {
        val copyLen = Math.min(tmpBuf.length, leftLen)
        bBuf.get(tmpBuf, 0, copyLen)
        dOut.write(tmpBuf, 0, copyLen)
        leftLen -= copyLen
      }
    }
  }

  private def writeHostBufferToStream(hBuf: HostMemoryBuffer, dOut: DataOutputStream): Unit = {
    // Write the buffer size first
    val bufLen = hBuf.getLength
    dOut.writeLong(bufLen)
    var leftLen = bufLen
    var hOffset = 0L
    while (leftLen > 0L) {
      val copyLen = Math.min(tmpBuf.length, leftLen)
      hBuf.getBytes(tmpBuf, 0, hOffset, copyLen)
      dOut.write(tmpBuf, 0, copyLen.toInt)
      leftLen -= copyLen
      hOffset += copyLen
    }
  }

  private def writeProtocolHeader(dOut: DataOutputStream): Unit = {
    dOut.writeInt(P_MAGIC_NUM)
    dOut.writeInt(P_VERSION)
  }

  def writeToStream(hostTbl: PackedTableHostColumnVector, dOut: DataOutputStream): Long = {
    withResource(new NvtxRange("Serialize Host Table", NvtxColor.RED)) { _ =>
      // In the order of 1) header, 2) table metadata, 3) table data on host
      val metaBuf = hostTbl.getTableMeta.getByteBuffer
      val dataBuf = hostTbl.getTableBuffer
      var writtenLen = headerLen.toLong + metaBuf.capacity()
      writeProtocolHeader(dOut)
      writeByteBufferToStream(metaBuf, dOut)
      if (dataBuf != null) {
        writeHostBufferToStream(dataBuf, dOut)
        writtenLen += dataBuf.getLength
      }
      writtenLen
    }
  }
}

private[rapids] class PackedTableDeserializer extends TableSerde {
  private def readProtocolHeader(dIn: DataInputStream): Unit = {
    val magicNum = dIn.readInt()
    if (magicNum != P_MAGIC_NUM) {
      throw new IllegalStateException(s"Expected magic number $P_MAGIC_NUM for " +
        s"table serializer, but got $magicNum")
    }
    val version = dIn.readInt()
    if (version != P_VERSION) {
      throw new IllegalStateException(s"Version mismatch: expected $P_VERSION for " +
        s"table serializer, but got $version")
    }
  }

  private def readByteBufferFromStream(dIn: DataInputStream): ByteBuffer = {
    val bufLen = dIn.readLong().toInt
    val bufArray = new Array[Byte](bufLen)
    var readLen = 0
    // A single call to read(bufArray) can not always read the expected length. So
    // we do it here ourselves.
    do {
      val ret = dIn.read(bufArray, readLen, bufLen - readLen)
      if (ret < 0) {
        throw new EOFException()
      }
      readLen += ret
    } while (readLen < bufLen)
    ByteBuffer.wrap(bufArray)
  }

  private def readHostBufferFromStream(dIn: DataInputStream): HostMemoryBuffer = {
    val bufLen = dIn.readLong()
    closeOnExcept(HostMemoryBuffer.allocate(bufLen)) { hostBuf =>
      var leftLen = bufLen
      var hOffset = 0L
      while (leftLen > 0) {
        val copyLen = Math.min(tmpBuf.length, leftLen)
        val readLen = dIn.read(tmpBuf, 0, copyLen.toInt)
        if (readLen < 0) {
          throw new EOFException()
        }
        hostBuf.setBytes(hOffset, tmpBuf, 0, readLen)
        hOffset += readLen
        leftLen -= readLen
      }
      hostBuf
    }
  }

  def readFromStream(dIn: DataInputStream): PackedTableHostColumnVector = {
    withResource(new NvtxRange("Read Host Table", NvtxColor.ORANGE)) { _ =>
      // 1) read and check header
      readProtocolHeader(dIn)
      // 2) read table metadata
      val tableMeta = TableMeta.getRootAsTableMeta(readByteBufferFromStream(dIn))
      val hostDataBuf = if (tableMeta.packedMetaAsByteBuffer() == null) {
        // no packed metadata, must be a table with zero columns, so no buffer
        null
      } else {
        // 3) read table data
        readHostBufferFromStream(dIn)
      }
      new PackedTableHostColumnVector(tableMeta, hostDataBuf)
    }
  }
}

private[rapids] class PackedTableIterator(dIn: DataInputStream, sparkTypes: Array[DataType],
    bundleSize: Long, deserTime: GpuMetric) extends Iterator[(Int, ColumnarBatch)] {

  private val tableDeserializer = new PackedTableDeserializer
  private val bundleTargetSize = Math.max(bundleSize, 128 * 1024 * 1024L) // at least 128M
  private val readTables: mutable.ListBuffer[PackedTableHostColumnVector] =
    mutable.ListBuffer.empty

  private val curOffsetsAndMetas: mutable.Queue[(Long, TableMeta)] = mutable.Queue.empty
  private var curTablesDeviceBuf: Option[DeviceMemoryBuffer] = None
  private var closed = false
  Option(TaskContext.get()).foreach { tc =>
    onTaskCompletion(tc) {
      closeBuffer()
      if (!closed) {
        dIn.close()
      }
    }
  }

  private def closeBuffer(): Unit = {
    readTables.safeClose()
    readTables.clear()
    curTablesDeviceBuf.foreach(_.safeClose())
    curTablesDeviceBuf = None
  }

  override def hasNext: Boolean = {
    if (curOffsetsAndMetas.isEmpty) {
      tryReadNextBundle()
    }
    curOffsetsAndMetas.nonEmpty
  }

  override def next(): (Int, ColumnarBatch) = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }
    (0, nextBatchFromBundle())
  }

  private def tryReadNextBundle(): Unit = {
    if (closed) {
      return
    }
    assert(curOffsetsAndMetas.isEmpty)
    // IO operation is coming, so leave GPU for a while
    GpuSemaphore.releaseIfNecessary(TaskContext.get())
    var (accSize, accRows) = (0L, 0L)
    readTables.foreach { p =>
      curOffsetsAndMetas.enqueue((accSize, p.getTableMeta))
      accSize += (if (p.getTableBuffer != null) p.getTableBuffer.getLength else 0L)
      accRows += p.getTableMeta.rowCount()
    }
    try {
      deserTime.ns {
        while (!closed && accSize < bundleTargetSize && accRows < Int.MaxValue) {
          val p = withResource(new NvtxRange("Read Table", NvtxColor.ORANGE)) { _ =>
            tableDeserializer.readFromStream(dIn)
          }
          // Always cache the read table to the queue, even the total size may
          // go beyond the target size. But we stop reading the next one.
          readTables.append(p)
          val startPos = if (p.getTableBuffer != null) {
            val preAccSize = accSize
            accSize += p.getTableBuffer.getLength
            preAccSize
          } else {
            -1L // Indicate a rows-only batch. Since 0 is a valid for an empty buffer.
          }
          accRows += p.getTableMeta.rowCount()
          if (accSize <= bundleTargetSize && accRows <= Int.MaxValue) {
            // Take it to the current status only when no size and rows number overflow.
            curOffsetsAndMetas.enqueue((startPos, p.getTableMeta))
          }
        }
      }
    } catch {
      case _: EOFException => // we reach the end
        dIn.close()
        closed = true
    }
  }

  // Merge host buffers in the current bundle into a single big contiguous buffer.
  // It requires the buffered tables are NOT rows-only.
  private def getCurrentTablesHostBuf: HostMemoryBuffer = {
    val numCurTables = curOffsetsAndMetas.length
    withResource(readTables.take(numCurTables).toArray) { curTables =>
      readTables.remove(0, numCurTables)
      if (curTables.length == 1) {
        val ret = curTables.head.getTableBuffer
        curTables(0) = null
        ret
      } else {
        val totoSize = curTables.map(_.getTableBuffer.getLength).sum
        closeOnExcept(HostMemoryBuffer.allocate(totoSize)) { bigHostBuf =>
          curTables.zipWithIndex.foreach { case (p, idx) =>
            withResource(p) { _ =>
              curTables(idx) = null
              bigHostBuf.copyFromHostBuffer(curOffsetsAndMetas(idx)._1, p.getTableBuffer,
                0, p.getTableBuffer.getLength)
            }
          }
          bigHostBuf
        }
      }
    }
  }

  private def nextBatchFromBundle(): ColumnarBatch = {
    val (start, tableMeta) = curOffsetsAndMetas.head
    if (start < 0) {
      GpuSemaphore.acquireIfNecessary(TaskContext.get)
      // Rows-only batches. Also acquires GPU semaphore because the downstream
      // operators expect the batch producer already holds the semaphore and may
      // generate empty batches.
      deserTime.ns {
        val rowsNum = curOffsetsAndMetas.map(_._2.rowCount()).sum
        curOffsetsAndMetas.clear()
        new ColumnarBatch(Array.empty, rowsNum.toInt)
      }
    } else {
      if (curTablesDeviceBuf.isEmpty) {
        // Refresh the device buffer by lazily moving buffered small tables from host
        // with a single copying.
        curTablesDeviceBuf = withResource(getCurrentTablesHostBuf) { tablesHostBuf =>
          // Begin to use GPU
          GpuSemaphore.acquireIfNecessary(TaskContext.get)
          deserTime.ns {
            withResource(new NvtxRange("Table to Device", NvtxColor.RED)) { _ =>
              closeOnExcept(DeviceMemoryBuffer.allocate(tablesHostBuf.getLength)) { devBuf =>
                devBuf.copyFromHostBuffer(tablesHostBuf)
                Some(devBuf)
              }
            }
          }
        }
      }
      assert(curTablesDeviceBuf.isDefined, "The device buffer holding tables is missing")
      deserTime.ns {
        curOffsetsAndMetas.dequeue()
        val end = curOffsetsAndMetas.headOption.map(_._1)
          .getOrElse(curTablesDeviceBuf.get.getLength)
        val ret = withResource(curTablesDeviceBuf.get.slice(start, end - start)) { sliced =>
          withResource(new NvtxRange("Deserialize Table", NvtxColor.YELLOW)) { _ =>
            val bufferMeta = tableMeta.bufferMeta()
            if (bufferMeta == null || bufferMeta.codecBufferDescrsLength == 0) {
              MetaUtils.getBatchFromMeta(sliced, tableMeta, sparkTypes)
            } else {
              GpuCompressedColumnVector.from(sliced, tableMeta)
            }
          }
        }
        closeOnExcept(ret) { _ =>
          if (curOffsetsAndMetas.isEmpty) {
            // All the tables on the current buffer are consumed, close the current buffer
            curTablesDeviceBuf.foreach(_.safeClose())
            curTablesDeviceBuf = None
          }
        }
        ret
      }
    }
  }
}
