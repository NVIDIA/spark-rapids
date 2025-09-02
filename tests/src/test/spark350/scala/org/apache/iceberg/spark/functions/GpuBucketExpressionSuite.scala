/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
spark-rapids-shim-json-lines ***/
package org.apache.iceberg.spark.functions

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.rapids.{FuzzerUtils, GpuBoundReference, GpuColumnVector, GpuLiteral, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.FuzzerUtils.createSchema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.types.{DataType, DataTypes}

class GpuBucketExpressionSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var seed = 2L

  override def beforeAll(): Unit = {
    seed = System.currentTimeMillis()
    println(s"Random seed set to $seed")
  }

  test("Int bucket function") {
    testBucketFunction[Int](DataTypes.IntegerType, 1000,
      (col, row) => col.getInt(row), BucketFunction.BucketInt.invoke)
  }

  test("Long bucket function") {
    testBucketFunction[Long](DataTypes.LongType, 2000,
      (col, row) => col.getLong(row), BucketFunction.BucketLong.invoke)
  }

  private def testBucketFunction[T](dataType: DataType,
    modValue: Int,
    colEval: (HostColumnVector, Int) => T,
    icebergBucket: (Int, T) => Int): Unit = {
    // TODO: Fix this when spark rapids jni hash function supports nullable
    val schema = createSchema(Seq(dataType), nullable = false)
    withResource(FuzzerUtils.createColumnarBatch(schema, 1000, seed = seed)) { batch =>
      val inputRefExpr = GpuBoundReference(0, dataType, nullable = false)(ExprId(0), "input0")
      val modExpr = GpuLiteral.create(modValue)
      val expr = GpuBucketExpression(modExpr, inputRefExpr)
      withResource(expr.columnarEvalAny(batch).asInstanceOf[GpuColumnVector]) { gpuResult =>
        withResource(gpuResult.getBase.copyToHost()) { hostResult =>
          withResource(icebergEval(batch.column(0).asInstanceOf[GpuColumnVector],
            modValue, colEval, icebergBucket)) { expected =>
            TestUtils.compareColumns(expected, hostResult)
          }
        }
      }
    }
  }

  private def icebergEval[T](col: GpuColumnVector, modValue: Int,
    colEval: (HostColumnVector, Int) => T,
    icebergBucket: (Int, T) => Int): HostColumnVector = {
    withResource(col.getBase.copyToHost()) { hostCol =>
      val builder = HostColumnVector.builder(DType.INT32, hostCol.getRowCount.toInt)

      for (row <- 0 until hostCol.getRowCount.toInt) {
        if (hostCol.isNull(row)) {
          builder.appendNull()
        } else {
          val v = colEval(hostCol, row)
          builder.append(icebergBucket(modValue, v))
        }
      }
      builder.build()
    }
  }
}
