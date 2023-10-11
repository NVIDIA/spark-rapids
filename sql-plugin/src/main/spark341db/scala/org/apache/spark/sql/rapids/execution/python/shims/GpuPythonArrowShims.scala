/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.{DataInputStream, DataOutputStream}
import java.util.concurrent.atomic.AtomicBoolean

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rapids.shims.api.python.ShimBasePythonRunner
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

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
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      releasedOrClosed: AtomicBoolean,
      context: TaskContext
  ): Iterator[ColumnarBatch] = {
    newReaderIterator(stream, writer, startTime, env, worker, None, releasedOrClosed,
      context)
  }

  protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {

    new ShimReaderIterator(stream, writer, startTime, env, worker, pid, releasedOrClosed,
      context) {

      private[this] var arrowReader: StreamedTableReader = _

      onTaskCompletion(context) {
        if (arrowReader != null) {
          arrowReader.close()
          arrowReader = null
        }
      }

      private var batchLoaded = true

      protected override def read(): ColumnarBatch = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
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

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
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
        }
      }

      private def writeNonEmptyIteratorOnGpu(dataOut: DataOutputStream): Unit = {
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
          while(inputIterator.hasNext) {
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
}
