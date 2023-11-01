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

import org.apache.spark.api.python._
import org.apache.spark.sql.rapids.execution.python.shims.GpuArrowPythonRunnerBase
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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
