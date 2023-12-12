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

/*** spark-rapids-shim-json-lines
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "321db"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "350"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import ai.rapids.cudf._
import com.nvidia.spark.rapids.Arm._
import com.nvidia.spark.rapids.BoolUtils.isAllValidTrue

import org.apache.spark.sql.rapids.GpuSequenceUtil
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH

object ComputeSequenceSizes {
  /**
   * Compute the size of each sequence according to 'start', 'stop' and 'step'.
   * A row (Row[start, stop, step]) contains at least one null element will produce
   * a null in the output.
   *
   * The returned column should be closed.
   */
  def apply(
      start: ColumnVector,
      stop: ColumnVector,
      step: ColumnVector): ColumnVector = {
    GpuSequenceUtil.checkSequenceInputs(start, stop, step)
    // Spark's algorithm to get the length (aka size)
    // ``` Scala
    //   size = if (start == stop) 1L else 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
    //   require(size <= MAX_ROUNDED_ARRAY_LENGTH,
    //       s"Too long sequence: $len. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
    //   size.toInt
    // ```
    val sizeAsLong = withResource(Scalar.fromLong(1L)) { one =>
      val diff = withResource(stop.castTo(DType.INT64)) { stopAsLong =>
        withResource(start.castTo(DType.INT64)) { startAsLong =>
          stopAsLong.sub(startAsLong)
        }
      }
      val quotient = withResource(diff) { _ =>
        withResource(step.castTo(DType.INT64)) { stepAsLong =>
          diff.div(stepAsLong)
        }
      }
      // actualSize = 1L + (stop.toLong - start.toLong) / estimatedStep.toLong
      val actualSize = withResource(quotient) { quotient =>
        quotient.add(one, DType.INT64)
      }
      withResource(actualSize) { _ =>
        val mergedEquals = withResource(start.equalTo(stop)) { equals =>
          if (step.hasNulls) {
            // Also set the row to null where step is null.
            equals.mergeAndSetValidity(BinaryOp.BITWISE_AND, equals, step)
          } else {
            equals.incRefCount()
          }
        }
        withResource(mergedEquals) { _ =>
          mergedEquals.ifElse(one, actualSize)
        }
      }
    }

    withResource(sizeAsLong) { _ =>
      // check max size
      withResource(Scalar.fromInt(MAX_ROUNDED_ARRAY_LENGTH)) { maxLen =>
        withResource(sizeAsLong.lessOrEqualTo(maxLen)) { allValid =>
          require(isAllValidTrue(allValid),
            s"Too long sequence found. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")
        }
      }
      // cast to int and return
      sizeAsLong.castTo(DType.INT32)
    }
  }
}