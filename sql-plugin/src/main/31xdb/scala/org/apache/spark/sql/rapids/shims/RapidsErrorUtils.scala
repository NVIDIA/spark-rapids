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

package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector}
import com.nvidia.spark.rapids.{Arm, GpuColumnVector}

import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{DataType, DecimalType}

object RapidsErrorUtils extends Arm {
  def invalidArrayIndexError(index: Int, numElements: Int,
      isElementAtF: Boolean = false): ArrayIndexOutOfBoundsException = {
    // Follow the Spark string format before 3.3.0
    new ArrayIndexOutOfBoundsException(s"Invalid index: $index, numElements: $numElements")
  }

  def mapKeyNotExistError(
      key: String,
      keyType: DataType,
      origin: Origin): NoSuchElementException = {
    // Follow the Spark string format before 3.3.0
    new NoSuchElementException(s"Key $key does not exist.")
  }

  def sqlArrayIndexNotStartAtOneError(): ArrayIndexOutOfBoundsException = {
    new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
  }

  def divByZeroError(origin: Origin): ArithmeticException = {
    new ArithmeticException("divide by zero")
  }

  def divOverflowError(origin: Origin): ArithmeticException = {
    new ArithmeticException("Overflow in integral divide.")
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: String = ""): ArithmeticException = {
    new ArithmeticException(message)
  }

  /**
   * Wrapper of the `cannotChangeDecimalPrecisionError` in Spark.
   *
   * @param values A decimal column which contains values that try to cast.
   * @param outOfBounds A boolean column that indicates which value cannot be casted. 
   * Users must make sure that there is at least one `true` in this column.
   * @param fromType The current decimal type.
   * @param toType The type to cast.
   * @param context The error context, default value is "".
   */
  def cannotChangeDecimalPrecisionError(      
      values: GpuColumnVector,
      outOfBounds: ColumnVector,
      fromType: DecimalType,
      toType: DecimalType,
      context: String = ""): ArithmeticException = {
    val row_id = withResource(outOfBounds.copyToHost()) {hcv =>
      (0.toLong until outOfBounds.getRowCount())
        .find(i => !hcv.isNull(i) && hcv.getBoolean(i))
        .get
    }
    val value = withResource(values.copyToHost()){hcv =>  
      hcv.getDecimal(row_id.toInt, fromType.precision, fromType.scale)
    }
    new ArithmeticException(s"${value.toDebugString} cannot be represented as " +
      s"Decimal(${toType.precision}, ${toType.scale}).")
  }
}
