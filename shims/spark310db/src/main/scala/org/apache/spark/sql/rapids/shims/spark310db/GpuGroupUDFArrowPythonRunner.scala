/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python.shims.spark310db

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.execution.python.GpuPythonHelper
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils
import org.apache.spark.sql.rapids.execution.python.GpuPythonUDF

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
    onDataWriteFinished: () => Unit,
    override val pythonOutSchema: StructType,
    minReadTargetBatchSize: Int)
    extends GpuArrowPythonRunner(funcs, evalType, argOffsets, pythonInSchema, timeZoneId, conf, batchSize, onDataWriteFinished, pythonOutSchema, minReadTargetBatchSize) {

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
        // write out number of columns
        Utils.tryWithSafeFinally {
          val builder = ArrowIPCWriterOptions.builder()
          builder.withMaxChunkSize(batchSize)
          builder.withCallback((table: Table) => {
            table.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
          })
          // Flatten the names of nested struct columns, required by cudf Arrow IPC writer.
          flattenNames(pythonInSchema).foreach { case (name, nullable) =>
              if (nullable) {
                builder.withColumnNames(name)
              } else {
                builder.withNotNullableColumnNames(name)
              }
          }
          while(inputIterator.hasNext) {
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
          if (onDataWriteFinished != null) onDataWriteFinished()
        }
      }

      private def flattenNames(d: DataType, nullable: Boolean=true): Seq[(String, Boolean)] =
        d match {
          case s: StructType =>
            s.flatMap(sf => Seq((sf.name, sf.nullable)) ++ flattenNames(sf.dataType, sf.nullable))
          case m: MapType =>
            flattenNames(m.keyType, nullable) ++ flattenNames(m.valueType, nullable)
          case a: ArrayType => flattenNames(a.elementType, nullable)
          case _ => Nil
      }
    }
  }
}
