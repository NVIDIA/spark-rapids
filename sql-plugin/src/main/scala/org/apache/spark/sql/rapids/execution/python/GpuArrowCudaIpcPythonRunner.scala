/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
package org.apache.spark.sql.rapids.execution.python

import java.io.DataOutputStream
import java.net.Socket

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Similar to `GpuArrowPythonRunner`, but exchange CUDA IPC meta data with Python worker
 * via Arrow stream instead of the whole real data.
 */
class GpuArrowCudaIpcPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    semWait: GpuMetric,
    onDataWriteFinished: () => Unit,
    pythonOutSchema: StructType,
    minReadTargetBatchSize: Int = 1)
  extends GpuArrowPythonRunner(funcs, evalType, argOffsets, pythonInSchema,
    timeZoneId, conf, batchSize, semWait, onDataWriteFinished,
    pythonOutSchema, minReadTargetBatchSize) {

  private val toBeClosed: ArrayBuffer[AutoCloseable] = ArrayBuffer.empty[AutoCloseable]

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
        // create schema with only 1 binary field in which will be filled with IPCMessage
        val ipcBinarySchema = StructType(
          StructField("in_struct",
            StructType(
              StructField("ipcMessage", BinaryType, false) :: Nil)) :: Nil)

        val arrowSchema = ArrowUtils.toArrowSchema(ipcBinarySchema, timeZoneId)

        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        val ipcNames = pythonInSchema.names

        context.addTaskCompletionListener[Unit](_ => toBeClosed.foreach(_.close()))

        Utils.tryWithSafeFinally {
          val arrowWriter = ArrowWriter.create(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          while (inputIterator.hasNext) {
            withResource(inputIterator.next()) { columnBatch =>
              val table = GpuColumnVector.from(columnBatch)
              val option = IPCWriterOptions.builder().withColumnNames(ipcNames: _*).build()
              val ipcMessage = table.exportIPC(option)
              val genericInternalRow = new GenericInternalRow(Array[Any](ipcMessage.getMessage))
              // There is only one row containing the ipc information in each pandas.DataFrame
              arrowWriter.write(InternalRow(genericInternalRow))
              arrowWriter.finish()
              writer.writeBatch()
              arrowWriter.reset()
              // TODO what if exception happens in the second iterator, we already put the first
              // ColumnBatch into the toBeClosed? We should not use Utils.tryWithSafeFinally,
              // instead, we should use try {} catch {} finally {}
              toBeClosed.append(table)
            }
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }

}

object GpuArrowPythonRunnerChooser {
  def apply(
      zeroCopy: Boolean,
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      pythonInSchema: StructType,
      timeZoneId: String,
      conf: Map[String, String],
      batchSize: Long,
      semWait: GpuMetric,
      onDataWriteFinished: () => Unit,
      pythonOutSchema: StructType,
      minReadTargetBatchSize: Int = 1): GpuArrowPythonRunner = {
    if (zeroCopy) {
      new GpuArrowCudaIpcPythonRunner(
        funcs,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        argOffsets,
        pythonInSchema,
        timeZoneId,
        conf,
        batchSize,
        semWait,
        onDataWriteFinished,
        pythonOutSchema,
        // We can not assert the result batch from Python has the same row number with the
        // input batch. Because Map Pandas UDF allows the output of arbitrary length
        // and columns.
        // Then try to read as many as possible by specifying `minReadTargetBatchSize` as
        // `Int.MaxValue` here.
        Int.MaxValue)
    } else {
      new GpuArrowPythonRunner(
        funcs,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        argOffsets,
        pythonInSchema,
        timeZoneId,
        conf,
        batchSize,
        semWait,
        onDataWriteFinished,
        pythonOutSchema,
        // We can not assert the result batch from Python has the same row number with the
        // input batch. Because Map Pandas UDF allows the output of arbitrary length
        // and columns.
        // Then try to read as many as possible by specifying `minReadTargetBatchSize` as
        // `Int.MaxValue` here.
        Int.MaxValue)
    }
  }
}

