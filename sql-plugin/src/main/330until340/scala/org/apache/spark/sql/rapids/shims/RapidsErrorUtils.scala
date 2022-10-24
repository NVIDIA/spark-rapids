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

import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}

object RapidsErrorUtils extends RapidsErrorUtilsFor330plus {

  def mapKeyNotExistError(
      key: String,
      keyType: DataType,
      origin: Origin): NoSuchElementException = {
    QueryExecutionErrors.mapKeyNotExistError(key, keyType, origin.context)
  }

  def invalidArrayIndexError(index: Int, numElements: Int,
      isElementAtF: Boolean = false): ArrayIndexOutOfBoundsException = {
    if (isElementAtF) {
      QueryExecutionErrors.invalidElementAtIndexError(index, numElements)
    } else {
      QueryExecutionErrors.invalidArrayIndexError(index, numElements)
    }
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: String = ""): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(message, hint, errorContext)
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      toType: DecimalType,
      context: String = ""): ArithmeticException = {
    QueryExecutionErrors.cannotChangeDecimalPrecisionError(
      value, toType.precision, toType.scale, context
    )
  }

  def overflowInIntegralDivideError(context: String = ""): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(
      "Overflow in integral divide", "try_divide", context
    )
  }
}
