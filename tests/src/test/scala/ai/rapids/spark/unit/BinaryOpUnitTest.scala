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
import ai.rapids.spark.{GpuLiteral, GpuUnitTests}

import org.apache.spark.sql.rapids.GpuPmod

class BinaryOpUnitTest extends GpuUnitTests {

  test("test pmod") {
    testNumericDataTypes { c =>
      c match {
        case (convert, convertS) =>
          withResource(convert(ColumnVector.fromBoxedInts(2, 0, 1, null))) { expected0 =>
            withResource(convert(ColumnVector.fromBoxedInts(2, null, 0, null))) { expected1 =>
              withResource(convert(ColumnVector.fromBoxedInts(3, 0, 3, null))) { expected2 =>
                //vector 0
                val vector0 = convert(ColumnVector.fromBoxedInts(-7, 3, 4, null))
                // assign to new handles for code readability
                val vector0_1 = vector0.incRefCount()
                val vector0_2 = vector0.incRefCount()

                //vector 1
                val vector1 = convert(ColumnVector.fromBoxedInts(3, 0, 1, null))

                //saving dataType to a variable for convenience
                val dataType = getSparkType(vector0.getBase.getType)

                //lhs = vector rhs = scalar
                val expr0 = GpuLiteral(vector0, dataType)
                val expr_s = GpuLiteral(convertS(3), dataType)
                checkEvaluation(GpuPmod(expr0, expr_s), expected0)

                //lhs = vector rhs = vector
                val expr0_1 = GpuLiteral(vector0_1, dataType)
                val expr1 = GpuLiteral(vector1, dataType)
                checkEvaluation(GpuPmod(expr0_1, expr1), expected1)

                //lhs = scalar rhs = vector
                val expr0_2 = GpuLiteral(vector0_2, dataType)
                checkEvaluation(GpuPmod(expr_s, expr0_2), expected2)
              }
            }
          }
      }
    }
  }
}
