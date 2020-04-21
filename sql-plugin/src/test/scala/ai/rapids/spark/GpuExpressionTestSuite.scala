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

package ai.rapids.spark

import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

abstract class GpuExpressionTestSuite extends SparkQueryCompareTestSuite {

  /**
   * Evaluate the GpuExpression and compare the results to the provided function.
   *
   * @param inputExpr GpuExpression under test
   * @param expectedFun Function that produces expected results
   * @param schema Schema to use for generated data
   * @param rowCount Number of rows of random to generate
   * @param maxFloatDiff Maximum acceptable difference between expected and actual results
   */
  def checkEvaluateGpuUnaryExpression(inputExpr: GpuExpression,
    inputType: DataType,
    outputType: DataType,
    expectedFun: Any => Option[Any],
    schema: StructType,
    rowCount: Int = 50,
    seed: Long = 0,
    maxFloatDiff: Double = 0.00001): Unit = {

    // generate batch
    withResource(FuzzerUtils.createColumnarBatch(schema, rowCount, seed = seed)) { batch =>
      // evaluate expression
      withResource(inputExpr.columnarEval(batch).asInstanceOf[GpuColumnVector]) { result =>
        // bring gpu data onto host
        withResource(batch.column(0).asInstanceOf[GpuColumnVector].copyToHost()) { hostInput =>
          withResource(result.copyToHost()) { hostResult =>
            // compare results
            assert(result.getRowCount == rowCount)
            for (i <- 0 until result.getRowCount.toInt) {
              val inputValue = getAs(hostInput, i, inputType)
              val actual = getAs(hostResult, i, outputType).orNull
              val expected = inputValue.flatMap(v => expectedFun(v)).orNull
              if (!compare(expected, actual, maxFloatDiff)) {
                throw new IllegalStateException(s"Expected: $expected. Actual: $actual. Input value: $inputValue")
              }
            }
          }
        }
      }
    }
  }

  private def getAs(column: RapidsHostColumnVector, index: Int, dataType: DataType): Option[Any] = {
    if (column.isNullAt(index)) {
      None
    } else {
      Some(dataType match {
        case DataTypes.ByteType => column.getByte(index)
        case DataTypes.ShortType => column.getShort(index)
        case DataTypes.IntegerType => column.getInt(index)
        case DataTypes.LongType => column.getLong(index)
        case DataTypes.FloatType => column.getFloat(index)
        case DataTypes.DoubleType => column.getDouble(index)
        case DataTypes.StringType => column.getUTF8String(index).toString
      })
    }

  }
}
