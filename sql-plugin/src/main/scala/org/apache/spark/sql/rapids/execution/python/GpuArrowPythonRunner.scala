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

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rapids.shims.api.python.ShimBasePythonRunner
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.python.shims.{GpuArrowPythonRunner, GpuPythonArrowOutput}
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
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
    pythonOutSchema: StructType = null,
    onDataWriteFinished: () => Unit = null)
  extends GpuPythonRunnerBase[ColumnarBatch](funcs, evalType, argOffsets)
    with GpuPythonArrowOutput {

  def toBatch(table: Table): ColumnarBatch = {
    GpuColumnVector.from(table, GpuColumnVector.extractTypes(pythonOutSchema))
  }

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
        s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  protected class RapidsWriter(
      env: SparkEnv,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext) {

    def writeCommand(dataOut: DataOutputStream): Unit = {

      // Write config for the worker as a number of key -> value pairs of strings
      dataOut.writeInt(conf.size)
      for ((k, v) <- conf) {
        PythonRDD.writeUTF(k, dataOut)
        PythonRDD.writeUTF(v, dataOut)
      }

      PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
    }

    def writeInputToStream(dataOut: DataOutputStream): Boolean = {
      if (inputIterator.nonEmpty) {
        writeNonEmptyIteratorOnGpu(dataOut)
      } else { // Partition is empty.
        // In this case CPU will still send the schema to Python workers by calling
        // the "start" API of the Java Arrow writer, but GPU will send out nothing,
        // leading to the IPC error. And it is not easy to do as what Spark does on
        // GPU, because the C++ Arrow writer used by GPU will only send out the schema
        // iff there is some data. Besides, it does not expose a "start" API to do this.
        // So here we leverage the Java Arrow writer to do similar things as Spark.
        // It is OK because sending out schema has nothing to do with GPU.
        writeEmptyIteratorOnCpu(dataOut)
        // Returning false because nothing was written
        false
      }
    }

    private def writeNonEmptyIteratorOnGpu(dataOut: DataOutputStream): Boolean = {
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

      var wrote = false
      Utils.tryWithSafeFinally {
        while (inputIterator.hasNext) {
          wrote = false
          val table = withResource(inputIterator.next()) { nextBatch =>
            GpuColumnVector.from(nextBatch)
          }
          withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
            // The callback will handle closing table and releasing the semaphore
            writer.write(table)
            wrote = true
          }
        }
        // The iterator can grab the semaphore even on an empty batch
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      } {
        writer.close()
        dataOut.flush()
        if (onDataWriteFinished != null) onDataWriteFinished()
      }
      wrote
    }

    private def writeEmptyIteratorOnCpu(dataOut: DataOutputStream): Unit = {
      // most code is copied from Spark
      val arrowSchema = ArrowUtilsShim.toArrowSchema(pythonInSchema, timeZoneId)
      val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdout writer for empty partition", 0, Long.MaxValue)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)

      Utils.tryWithSafeFinally {
        val writer = new ArrowStreamWriter(root, null, dataOut)
        writer.start()
        // No data to write
        writer.end()
        // The iterator can grab the semaphore even on an empty batch
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
      } {
        root.close()
        allocator.close()
        if (onDataWriteFinished != null) onDataWriteFinished()
      }
    }
  }
}
