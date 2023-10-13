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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector.GpuArrowColumnarBatchBuilder
import com.nvidia.spark.rapids.jni.RmmSpark
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

class HostColumnToGpuRetrySuite extends RmmSparkRetrySuiteBase {

  private val schema = StructType(Seq(StructField("a", IntegerType)))
  private val NUM_ROWS = 50

  private def buildArrowIntColumn(): ColumnVector = {
    val intVector = new IntVector("intVector", new RootAllocator())
    intVector.allocateNew(NUM_ROWS)
    (0 until NUM_ROWS).foreach { pos =>
      intVector.set(pos, pos * 10)
    }
    new ArrowColumnVector(intVector)
  }

  test("Arrow column builder with retry OOM") {
    val batch = withResource(new GpuArrowColumnarBatchBuilder(schema)) { builder =>
      withResource(buildArrowIntColumn()) { arrowColumn =>
        builder.copyColumnar(arrowColumn, 0, NUM_ROWS)
      }
      RmmSpark.forceRetryOOM(RmmSpark.getCurrentThreadId)
      RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch] {
        builder.tryBuild(NUM_ROWS)
      }
    }
    withResource(batch) { _ =>
      assertResult(NUM_ROWS)(batch.numRows())
      assertResult(1)(batch.numCols())
      withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostCol =>
        withResource(buildArrowIntColumn()) { arrowCol =>
          (0 until NUM_ROWS).foreach { pos =>
            assert(hostCol.getInt(pos) == arrowCol.getInt(pos))
          }
        }
      }
    }
  }

}
