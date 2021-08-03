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

package com.nvidia.spark.rapids.unit

import ai.rapids.cudf.{ColumnVector, HostColumnVector}
import com.nvidia.spark.rapids.{GpuUnitTests, GpuColumnVector, GpuScalar, CudfTestHelper}
import org.apache.spark.sql.rapids.GpuStringRepeat
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DataTypes

class StringUnitTest extends GpuUnitTests {
  test("Test StringRepeat with scalar string and scalar repeatTimes") {
    // Test repeat(NULL|str, NULL).
    withResource(GpuScalar(null, DataTypes.IntegerType)) { nullInt =>
      val doTest = (strScalar: GpuScalar) => {
        withResource(GpuStringRepeat(null, null).doColumnar(1, strScalar, nullInt)) { result =>
          withResource(result.copyToHost()) { hostResult =>
            assertResult(hostResult.getRowCount)(1)
            assertResult(hostResult.isNull(0))(true)
          }
        }
      }

      withResource(GpuScalar(null, DataTypes.StringType)) { nullStr => doTest(nullStr) }
      withResource(GpuScalar("abc123", DataTypes.StringType)) { str => doTest(str) }
    }

    // Test repeat(NULL, intVal).
    withResource(GpuScalar(null, DataTypes.StringType)) { nullStr =>
      val doTest = (intVal: Int) =>
        withResource(GpuScalar(intVal, DataTypes.IntegerType)) { intScalar =>
          withResource(GpuStringRepeat(null, null).doColumnar(1, nullStr, intScalar)) { result =>
            withResource(result.copyToHost()) { hostResult =>
              assertResult(hostResult.getRowCount)(1)
              assertResult(hostResult.isNull(0))(true)
            }
          }
        }

      doTest(-1)
      doTest(0)
      doTest(1)
    }

    // Test repeat(str, intVal).
    withResource(GpuScalar("abc123", DataTypes.StringType)) { strScalar =>
      val doTest = (intVal: Int, expectedStr: String) =>
        withResource(GpuScalar(intVal, DataTypes.IntegerType)) { intScalar =>
          withResource(GpuStringRepeat(null, null).doColumnar(1, strScalar, intScalar)) {
            result =>
              withResource(result.copyToHost()) { hostResult =>
                assertResult(hostResult.getRowCount)(1)
                assertResult(hostResult.isNull(0))(false)
                assertResult(hostResult.getJavaString(0))(expectedStr)
              }
          }
        }

      doTest(-1, "")
      doTest(0, "")
      doTest(1, "abc123")
      doTest(2, "abc123abc123")
      doTest(3, "abc123abc123abc123")
    }
  }

  test("Test StringRepeat with scalar string and column repeatTimes") {
    val intCol = ColumnVector.fromBoxedInts(-3, null, -1, 0, 1, 2, null)

    withResource(GpuColumnVector.from(intCol, DataTypes.IntegerType)) { intGpuColumn =>
      // Test repeat(NULL, intVal).
      withResource(GpuScalar(null, DataTypes.StringType)) { nullStr =>
        withResource(GpuStringRepeat(null, null).doColumnar(nullStr, intGpuColumn)) { result =>
          withResource(result.copyToHost()) { hostResult =>
            (0 until hostResult.getRowCount.asInstanceOf[Int]).foreach { i =>
              assertResult(hostResult.isNull(0))(true)
            }
          }
        }
      }

      // Test repeat(str, intVal).
      withResource(GpuScalar("abc123", DataTypes.StringType)) { strScalar =>
        withResource(GpuStringRepeat(null, null).doColumnar(strScalar, intGpuColumn)) { result =>
          withResource(result.copyToHost()) { hostResult =>
            CudfTestHelper.assertColumnsAreEqual(
              HostColumnVector.fromStrings("", null, "", "", "abc123", "abc123abc123", null),
              hostResult,
              "Repeated strings")
          }
        }
      }
    }
  }

  test("Test StringRepeat with column strings and scalar repeatTimes") {
    val strsCol = ColumnVector.fromStrings(null, "a", "", "123", "éá")

    withResource(GpuColumnVector.from(strsCol, DataTypes.StringType)) { strGpuColumn =>
      // Test repeat(strs, NULL).
      withResource(GpuScalar(null, DataTypes.IntegerType)) { nullInt =>
        withResource(GpuStringRepeat(null, null).doColumnar(strGpuColumn, nullInt)) { result =>
          withResource(result.copyToHost()) { hostResult =>
            (0 until hostResult.getRowCount.asInstanceOf[Int]).foreach { i =>
              assertResult(hostResult.isNull(0))(true)
            }
          }
        }
      }

      // Test repeat(strs, intVal).
      val doTest = (intVal: Int, expectedStrs : HostColumnVector) =>
        withResource(GpuScalar(intVal, DataTypes.IntegerType)) { intScalar =>
          withResource(GpuStringRepeat(null, null).doColumnar(strGpuColumn, intScalar)) { result =>
            withResource(result.copyToHost()) { hostResult =>
              CudfTestHelper.assertColumnsAreEqual(expectedStrs, hostResult, "Repeated strings")
            }
          }
        }

      doTest(-10, HostColumnVector.fromStrings(null, "", "", "", ""))
      doTest(0, HostColumnVector.fromStrings(null, "", "", "", ""))
      doTest(2, HostColumnVector.fromStrings(null, "aa", "", "123123", "éáéá"))
    }
  }

  test("Test StringRepeat with column strings and column repeatTimes") {
    val strsCol = ColumnVector.fromStrings(null, "a", "", "123", "éá", "abc")
    val intCol  = ColumnVector.fromBoxedInts(1, null, -10, 0, 2, 1)

    withResource(GpuColumnVector.from(strsCol, DataTypes.StringType)) { strGpuColumn =>
      withResource(GpuColumnVector.from(intCol, DataTypes.IntegerType)) { intGpuColumn =>
        withResource(GpuStringRepeat(null, null).doColumnar(strGpuColumn, intGpuColumn)) { result =>
          withResource(result.copyToHost()) { hostResult =>
            CudfTestHelper.assertColumnsAreEqual(
              HostColumnVector.fromStrings(null, null, "", "", "éáéá", "abc"),
              hostResult,
              "Repeated strings")
          }
        }
      }
    }
  }

}
