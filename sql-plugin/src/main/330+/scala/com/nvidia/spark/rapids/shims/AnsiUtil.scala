/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{Arm, BoolUtils, FloatUtils, GpuColumnVector}

import org.apache.spark.ShimTrampolineUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

object AnsiUtil extends Arm {

  /**
   * Spark 330+ supports Ansi cast from float/double to timestamp
   * if have Nan or Infinity, throw an exception
   * if (Math.floor(x) > Long.Max && Math.ceil(x) < Long.min), throw an exception
   */
  def supportsAnsiCastFloatToTimestamp(): Boolean = true

  def castFloatToTimestampAnsi(floatInput: ColumnView, toType: DataType): ColumnVector = {
    if(floatInput.getType.equals(DType.FLOAT32)) {
      withResource(floatInput.castTo(DType.FLOAT64)) { d =>
        castDoubleToTimestampAnsi(d, toType)
      }
    } else {
      castDoubleToTimestampAnsi(floatInput, toType)
    }
  }

  private def castDoubleToTimestampAnsi(doubleInput: ColumnView, toType: DataType): ColumnVector = {
    val msg = s"The column contains at least a single value that is " +
        s"NaN, Infinity or out-of-range values. To return NULL instead, use 'try_cast'. " +
        s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error."

    // These are the arguments required by SparkDateTimeException class to create error message.
    val errorClass= "CAST_INVALID_INPUT"
    val messageParameters = Array("Nan/Infinity", "DOUBLE", "TIMESTAMP", SQLConf.ANSI_ENABLED.key)

    def throwSparkDateTimeException: Unit = {
      throw ShimTrampolineUtil.dateTimeException(errorClass, messageParameters)
    }

    def throwOverflowException: Unit = {
      throw new ArithmeticException(msg)
    }

    withResource(doubleInput.isNan) { hasNan =>
      if (BoolUtils.isAnyValidTrue(hasNan)) {
        throwSparkDateTimeException
      }
    }

    // check nan
    withResource(FloatUtils.getInfinityVector(doubleInput.getType)) { inf =>
      withResource(doubleInput.contains(inf)) { hasInf =>
        if (BoolUtils.isAnyValidTrue(hasInf)) {
          throwSparkDateTimeException
        }
      }
    }

    // check max value
    withResource(Scalar.fromLong(1000000L)) { microsPerSecondS =>
      withResource(doubleInput.mul(microsPerSecondS)) { mul =>
        //      if (Math.floor(x) <= Long.Max && Math.ceil(x) >= Long.min) {
        //        x.toLong
        //      else
        //        SparkArithmeticException
        withResource(Scalar.fromLong(Long.MaxValue)) { maxLongS =>
          withResource(mul.floor()) { floorCv =>
            withResource(floorCv.greaterThan(maxLongS)) { invalid =>
              if (BoolUtils.isAnyValidTrue(invalid)) {
                throwOverflowException
              }
            }
          }
        }

        // check min value
        withResource(Scalar.fromLong(Long.MinValue)) { minLongS =>
          withResource(mul.ceil()) { ceil =>
            withResource(ceil.lessThan(minLongS)) { invalid =>
              if (BoolUtils.isAnyValidTrue(invalid)) {
                throwOverflowException
              }
            }
          }
        }

        withResource(mul.castTo(DType.INT64)) { inputTimesMicrosCv =>
            inputTimesMicrosCv.castTo(GpuColumnVector.getNonNestedRapidsType(toType))
        }
      }
    }
  }
}
