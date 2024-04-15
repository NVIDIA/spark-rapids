/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream
import java.net.Socket

import com.nvidia.spark.rapids.GpuSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.rapids.execution.python.{GpuArrowPythonWriter, GpuPythonRunnerCommon}
import org.apache.spark.sql.rapids.shims.ArrowUtilsShim
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

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
    maxBatchSize: Long,
    override val pythonOutSchema: StructType,
    jobArtifactUUID: Option[String] = None)
  extends GpuBasePythonRunner[ColumnarBatch](funcs, evalType, argOffsets, jobArtifactUUID)
    with GpuArrowPythonOutput with GpuPythonRunnerCommon {

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[ColumnarBatch],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new WriterThread(env, worker, inputIterator, partitionIndex, context) {

      val arrowWriter = new GpuArrowPythonWriter(pythonInSchema, maxBatchSize) {
        override protected def writeUDFs(dataOut: DataOutputStream): Unit = {
          PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
        }
      }
      val isInputNonEmpty = inputIterator.nonEmpty
      lazy val arrowSchema = ArrowUtilsShim.toArrowSchema(pythonInSchema, timeZoneId)

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        arrowWriter.writeCommand(dataOut, conf)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        if (isInputNonEmpty) {
          arrowWriter.start(dataOut)
          Utils.tryWithSafeFinally {
            while (inputIterator.hasNext) {
              arrowWriter.writeAndClose(inputIterator.next())
            }
          } {
            arrowWriter.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
            dataOut.flush()
          }
        } else {
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
          arrowWriter.writeEmptyIteratorOnCpu(dataOut, arrowSchema)
        }
      }
    }
  }
}
