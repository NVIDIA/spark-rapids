/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark.unit

import ai.rapids.cudf.ColumnVector
import ai.rapids.spark.{GpuBoundReference, GpuLiteral, GpuUnitTests}

import org.apache.spark.sql.rapids.GpuPmod
import org.apache.spark.sql.vectorized.ColumnarBatch

class BinaryOpUnitTest extends GpuUnitTests {

  test("test pmod") {
    testNumericDataTypes { case (convertVector, convertScalar) =>
      withResource(convertVector(ColumnVector.fromBoxedInts(2, 0, 1, null))) { expected0 =>
        withResource(convertVector(ColumnVector.fromBoxedInts(2, null, 0, null))) {
          expected1 =>
          withResource(convertVector(ColumnVector.fromBoxedInts(3, 0, 3, null))) {
            expected2 =>

            withResource(convertVector(ColumnVector.fromBoxedInts(-7, 3, 4, null))) {
              vector0 =>
              withResource(convertVector(ColumnVector.fromBoxedInts(3, 0, 1, null))) {
                vector1 =>

                val batch = new ColumnarBatch(List(vector0, vector1).toArray, 4)
                val dataType = getSparkType(vector0.getBase.getType)

                //lhs = vector rhs = scalar
                val expressionVector = GpuBoundReference(0, dataType, true)
                val expressionScalar = GpuLiteral(convertScalar(3), dataType)
                checkEvaluation(GpuPmod(expressionVector, expressionScalar), expected0, batch)

                //lhs = vector rhs = vector
                val expressionVector1 = GpuBoundReference(1, dataType, true)
                checkEvaluation(GpuPmod(expressionVector, expressionVector1), expected1, batch)

                //lhs = scalar rhs = vector
                checkEvaluation(GpuPmod(expressionScalar, expressionVector), expected2, batch)
              }
            }
          }
        }
      }
    }
  }
}
