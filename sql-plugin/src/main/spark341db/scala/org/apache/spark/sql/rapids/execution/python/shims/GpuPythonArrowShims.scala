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
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class GpuArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    pythonOutSchema: StructType = null,
    onDataWriteFinished: () => Unit = null)
  extends GpuArrowPythonRunnerBase(funcs, evalType, argOffsets, pythonInSchema, timeZoneId,
    conf, batchSize, pythonOutSchema, onDataWriteFinished) {

  protected override def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      val workerImpl = new RapidsWriter(env, inputIterator, partitionIndex, context)

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        workerImpl.writeCommand(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        workerImpl.writeInputToStream(dataOut)
      }
    }
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
