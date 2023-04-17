/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.execution.python

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rapids.shims.api.python.ShimBasePythonRunner
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

class BufferToStreamWriter(outputStream: DataOutputStream) extends HostBufferConsumer {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def handleBuffer(hostBuffer: HostMemoryBuffer, length: Long): Unit = {
    withResource(hostBuffer) { buffer =>
      var len = length
      var offset: Long = 0
      while(len > 0) {
        val toCopy = math.min(tempBuffer.length, len).toInt
        buffer.getBytes(tempBuffer, 0, offset, toCopy)
        outputStream.write(tempBuffer, 0, toCopy)
        len = len - toCopy
        offset = offset + toCopy
      }
    }
  }
}

class StreamToBufferProvider(inputStream: DataInputStream) extends HostBufferProvider {
  private[this] val tempBuffer = new Array[Byte](128 * 1024)

  override def readInto(hostBuffer: HostMemoryBuffer, length: Long): Long = {
    var amountLeft = length
    var totalRead : Long = 0
    while (amountLeft > 0) {
      val amountToRead = Math.min(tempBuffer.length, amountLeft).toInt
      val amountRead = inputStream.read(tempBuffer, 0, amountToRead)
      if (amountRead <= 0) {
        // Reached EOF
        amountLeft = 0
      } else {
        amountLeft -= amountRead
        hostBuffer.setBytes(totalRead, tempBuffer, 0, amountRead)
        totalRead += amountRead
      }
    }
    totalRead
  }
}

/**
 * A trait that can be mixed-in with `GpuPythonRunnerBase`. It implements the logic from
 * Python (Arrow) to GPU/JVM (ColumnarBatch).
 */
trait GpuPythonArrowOutput { _: GpuPythonRunnerBase[_] =>

  /**
   * Default to `Int.MaxValue` to try to read as many as possible.
   * Change it by calling `setMinReadTargetBatchSize` before a reading.
   */
  private var minReadTargetBatchSize: Int = Int.MaxValue

  /**
   * Update the expected batch size for next reading.
   */
  private[python] final def setMinReadTargetBatchSize(size: Int): Unit = {
    minReadTargetBatchSize = size
  }

  /** Convert the table received from the Python side to a batch. */
  protected def toBatch(table: Table): ColumnarBatch

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    newReaderIterator(stream, writerThread, startTime, env, worker, None, releasedOrClosed,
      context)
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writerThread: WriterThread,
      startTime: Long,
      env: SparkEnv,
      worker: Socket,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ShimReaderIterator(stream, writerThread, startTime, env, worker, pid, releasedOrClosed,
      context) {

      private[this] var arrowReader: StreamedTableReader = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (arrowReader != null) {
          arrowReader.close()
          arrowReader = null
        }
      }

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          // Because of batching and other things we have to be sure that we release the semaphore
          // before any operation that could block. This is because we are using multiple threads
          // for a single task and the GpuSemaphore might not wake up both threads associated with
          // the task, so a reader can be blocked waiting for data, while a writer is waiting on
          // the semaphore
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          if (arrowReader != null && batchLoaded) {
            // The GpuSemaphore is acquired in a callback
            val table =
              withResource(new NvtxRange("read python batch", NvtxColor.DARK_GREEN)) { _ =>
                arrowReader.getNextIfAvailable(minReadTargetBatchSize)
              }
            if (table == null) {
              batchLoaded = false
              arrowReader.close()
              arrowReader = null
              read()
            } else {
              withResource(table) { _ =>
                batchLoaded = true
                toBatch(table)
              }
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                val builder = ArrowIPCOptions.builder()
                builder.withCallback(() =>
                  GpuSemaphore.acquireIfNecessary(TaskContext.get()))
                arrowReader = Table.readArrowIPCChunked(builder.build(),
                  new StreamToBufferProvider(stream))
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }
}

/**
 * Base class of GPU Python runners who will be mixed with GpuPythonArrowOutput
 * to produce columnar batches.
 */
abstract class GpuPythonRunnerBase[IN](
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]])
  extends ShimBasePythonRunner[IN, ColumnarBatch](funcs, evalType, argOffsets)

/**
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
abstract class GpuArrowPythonRunnerBase(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    onDataWriteFinished: () => Unit = null)
  extends GpuPythonRunnerBase[ColumnarBatch](funcs, evalType, argOffsets)
    with GpuPythonArrowOutput {

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
        s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val writer = {
          val builder = ArrowIPCWriterOptions.builder()
          builder.withMaxChunkSize(batchSize)
          builder.withCallback((table: Table) => {
            table.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
          })
          // Flatten the names of nested struct columns, required by cudf arrow IPC writer.
          GpuArrowPythonRunner.flattenNames(pythonInSchema).foreach { case (name, nullable) =>
              if (nullable) {
                builder.withColumnNames(name)
              } else {
                builder.withNotNullableColumnNames(name)
              }
          }
          Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
        }

        Utils.tryWithSafeFinally {
          while(inputIterator.hasNext) {
            val table = withResource(inputIterator.next()) { nextBatch =>
              GpuColumnVector.from(nextBatch)
            }
            withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
              // The callback will handle closing table and releasing the semaphore
              writer.write(table)
            }
          }
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        } {
          writer.close()
          dataOut.flush()
          if (onDataWriteFinished != null) onDataWriteFinished()
        }
      }
    }
  }
}

class GpuArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    pythonOutSchema: StructType,
    onDataWriteFinished: () => Unit = null)
  extends GpuArrowPythonRunnerBase(
    funcs,
    evalType,
    argOffsets,
    pythonInSchema,
    timeZoneId,
    conf,
    batchSize,
    onDataWriteFinished) {

  def toBatch(table: Table): ColumnarBatch = {
    GpuColumnVector.from(table, GpuColumnVector.extractTypes(pythonOutSchema))
  }
}

object GpuArrowPythonRunner {
  def flattenNames(d: DataType, nullable: Boolean = true): Seq[(String, Boolean)] =
    d match {
      case s: StructType =>
        s.flatMap(sf => Seq((sf.name, sf.nullable)) ++ flattenNames(sf.dataType, sf.nullable))
      case m: MapType =>
        flattenNames(m.keyType, nullable) ++ flattenNames(m.valueType, nullable)
      case a: ArrayType => flattenNames(a.elementType, nullable)
      case _ => Nil
    }
}
