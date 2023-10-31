/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.rapids.execution.python._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Group Map UDF specific serializer for Databricks because they have a special GroupUDFSerializer.
 * The main difference here from the GpuArrowPythonRunner is that it creates a new Arrow
 * Stream for each grouped data.
 * The overall flow is:
 *   - send a 1 to indicate more data is coming
 *   - create a new Arrow Stream for each grouped data
 *   - send the schema
 *   - send that group of data
 *   - close that Arrow stream
 *   - Repeat starting at sending 1 if more data, otherwise send a 0 to indicate no
 *     more data being sent.
 */
class GpuGroupUDFArrowPythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    pythonInSchema: StructType,
    timeZoneId: String,
    conf: Map[String, String],
    batchSize: Long,
    pythonOutSchema: StructType)
  extends GpuPythonRunnerBase[ColumnarBatch](funcs, evalType, argOffsets)
    with GpuPythonArrowOutput {

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
        var wrote = false
        // write out number of columns
        Utils.tryWithSafeFinally {
          val builder = ArrowIPCWriterOptions.builder()
          builder.withMaxChunkSize(batchSize)
          builder.withCallback((table: Table) => {
            table.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
          })
          // Flatten the names of nested struct columns, required by cudf Arrow IPC writer.
          GpuArrowPythonRunner.flattenNames(pythonInSchema).foreach { case (name, nullable) =>
              if (nullable) {
                builder.withColumnNames(name)
              } else {
                builder.withNotNullableColumnNames(name)
              }
          }
          while(inputIterator.hasNext) {
            wrote = false
            val writer = {
              // write 1 out to indicate there is more to read
              dataOut.writeInt(1)
              Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
            }
            val table = withResource(inputIterator.next()) { nextBatch =>
              GpuColumnVector.from(nextBatch)
            }
            withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
              // The callback will handle closing table and releasing the semaphore
              writer.write(table)
              wrote = true
            }
            writer.close()
            dataOut.flush()
          }
          // indicate not to read more
          // The iterator can grab the semaphore even on an empty batch
          GpuSemaphore.releaseIfNecessary(TaskContext.get())
        } {
          // tell serializer we are done
          dataOut.writeInt(0)
          dataOut.flush()
        }
        wrote
      }
    }
  }

  def toBatch(table: Table): ColumnarBatch = {
    GpuColumnVector.from(table, GpuColumnVector.extractTypes(pythonOutSchema))
  }
}
