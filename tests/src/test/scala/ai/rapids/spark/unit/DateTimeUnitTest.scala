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
import ai.rapids.spark.{GpuColumnVector, GpuLiteral, GpuUnitTests}
import org.apache.spark.sql.rapids.{GpuDateAdd, GpuDateSub}

class DateTimeUnitTest extends GpuUnitTests {
  val TIMES_DAY = Array(-1528, //1965-10-26
    17716, //2018-07-04
    19382, //2023-01-25
    -13528, 1716) //2018-07-04

  test("test date_add") {
    withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1498, 17746, 19412,
      -13498, 1746))) { expected0 =>
      withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1526, 17748, 19388,
        -13519, 1722))) { expected1 =>
        withResource(GpuColumnVector.from(ColumnVector.daysFromInts(17676, 17706, 17680,
          17683, 17680))) { expected2 =>

          // vector 0
          val dates0 = GpuColumnVector.from(ColumnVector.daysFromInts(TIMES_DAY: _*))
          // assign to new handles for code readability
          val dates0_1 = dates0.incRefCount()

          //vector 1
          val days0 = GpuColumnVector.from(ColumnVector.fromInts(2, 32, 6, 9, 6))
          // assign to new handles for code readability
          val days0_1 = days0.incRefCount()

          // saving types for convenience
          val lhsType = getSparkType(dates0.getBase.getType)
          val rhsType = getSparkType(days0.getBase.getType)

          val daysExprV0 = GpuLiteral(days0_1, rhsType)
          val datesS = GpuLiteral(17674, lhsType)

          // lhs = vector, rhs = scalar
          val datesExprV = GpuLiteral(dates0, lhsType)
          val daysS = GpuLiteral(30, rhsType)
          checkEvaluation(GpuDateAdd(datesExprV, daysS), expected0)

          // lhs = vector, rhs = vector
          val daysExprV = GpuLiteral(days0, rhsType)
          val datesExprV_1 = GpuLiteral(dates0_1, lhsType)
          checkEvaluation(GpuDateAdd(datesExprV_1, daysExprV), expected1)

          // lhs = scalar, rhs = vector
          checkEvaluation(GpuDateAdd(datesS, daysExprV0), expected2)
        }
      }
    }
  }

  test("test date_sub") {
    withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1558, 17686, 19352, -13558,
      1686))) { expected0 =>
      withResource(GpuColumnVector.from(ColumnVector.daysFromInts(-1530, 17684, 19376,
        -13537, 1710))) { expected1 =>
        withResource(GpuColumnVector.from(ColumnVector.daysFromInts(17672, 17642, 17668,
          17665, 17668))) { expected2 =>
          // vector 0
          val dates0 = GpuColumnVector.from(ColumnVector.daysFromInts(TIMES_DAY: _*))
          // assign to new handles for code readability
          val dates0_1 = dates0.incRefCount()

          //vector 1
          val days0 = GpuColumnVector.from(ColumnVector.fromInts(2, 32, 6, 9, 6))
          // assign to new handles for code readability
          val days0_1 = days0.incRefCount()

          // saving types for convenience
          val lhsType = getSparkType(dates0.getBase.getType)
          val rhsType = getSparkType(days0.getBase.getType)

          val daysExprV0 = GpuLiteral(days0_1, rhsType)
          val datesS = GpuLiteral(17674, lhsType)

          // lhs = vector, rhs = scalar
          val datesExprV = GpuLiteral(dates0, lhsType)
          val daysS = GpuLiteral(30, rhsType)
          checkEvaluation(GpuDateSub(datesExprV, daysS), expected0)

          // lhs = vector, rhs = vector
          val daysExprV = GpuLiteral(days0, rhsType)
          val datesExprV_1 = GpuLiteral(dates0_1, lhsType)
          checkEvaluation(GpuDateSub(datesExprV_1, daysExprV), expected1)

          // lhs = scalar, rhs = vector
          checkEvaluation(GpuDateSub(datesS, daysExprV0), expected2)
        }
      }
    }
  }

}
