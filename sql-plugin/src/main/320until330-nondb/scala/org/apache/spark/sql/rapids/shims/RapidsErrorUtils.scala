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

object RapidsErrorUtils {
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
    QueryExecutionErrors.divideByZeroError()
  }

  def divOverflowError(origin: Origin): ArithmeticException = {
    QueryExecutionErrors.overflowInIntegralDivideError()
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: String = ""): ArithmeticException = {
    new ArithmeticException(message)
  }

  def cannotChangeDecimalPrecisionError(      
      value: Decimal,
      toType: DecimalType,
      context: String = ""): ArithmeticException = {
    QueryExecutionErrors.cannotChangeDecimalPrecisionError(
      value, toType.precision, toType.scale
    )
  }

  def overflowInIntegralDivideError(context: String = ""): ArithmeticException = {
    QueryExecutionErrors.overflowInIntegralDivideError()
  }

  def foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName: String, matchedFields: String): Throwable = {
    QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName, matchedFields)
  }
}
