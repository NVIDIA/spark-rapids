/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.Table
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.rapids.execution.{SerializeBatchDeserializeHostBuffer, SerializeConcatHostBuffersDeserializeBatch}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class SerializationSuite extends FunSuite with Arm {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType))
    }
  }

  private def createDeserializedHostBuffer(
      batch: ColumnarBatch): SerializeBatchDeserializeHostBuffer = {
    withResource(new SerializeBatchDeserializeHostBuffer(batch)) { obj =>
      // Return a deserialized form of the object as if it was read on the driver
      SerializationUtils.clone(obj)
    }
  }

  test("SerializeConcatHostBuffersDeserializeBatch driver serialize after deserialize") {
    val hostBatch = withResource(buildBatch()) { gpuBatch =>
      val attrs = GpuColumnVector.extractTypes(gpuBatch).map(t => AttributeReference("", t)())
      val buffer = createDeserializedHostBuffer(gpuBatch)
      new SerializeConcatHostBuffersDeserializeBatch(Array(buffer), attrs)
    }
    withResource(hostBatch) { _ =>
      // clone via serialization without manifesting the GPU batch
      withResource(SerializationUtils.clone(hostBatch)) { clonedObj =>
        assertResult(hostBatch.dataSize)(clonedObj.dataSize)
        assertResult(hostBatch.numRows)(clonedObj.numRows)

        // try to clone it again from the cloned object
        SerializationUtils.clone(clonedObj).close()
      }
    }
  }

  test("SerializeConcatHostBuffersDeserializeBatch executor serialize after deserialize") {
    withResource(buildBatch()) { gpuExpected =>
      val attrs = GpuColumnVector.extractTypes(gpuExpected).map(t => AttributeReference("", t)())
      val buffer = createDeserializedHostBuffer(gpuExpected)
      val hostBatch = new SerializeConcatHostBuffersDeserializeBatch(Array(buffer), attrs)
      withResource(hostBatch) { _ =>
        val gpuBatch = hostBatch.batch
        TestUtils.compareBatches(gpuExpected, gpuBatch)
        // clone via serialization after manifesting the GPU batch
        withResource(SerializationUtils.clone(hostBatch)) { clonedObj =>
          val gpuClonedBatch = clonedObj.batch
          TestUtils.compareBatches(gpuExpected, gpuClonedBatch)
          // try to clone it again from the cloned object
          SerializationUtils.clone(clonedObj).close()
        }
      }
    }
  }
}
