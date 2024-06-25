/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}

object RapidsErrorUtils extends RapidsQueryErrorUtils {
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

  def sqlArrayIndexNotStartAtOneError(): RuntimeException = {
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

  def cannotChangeDecimalPrecisionError(      
      value: Decimal,
      toType: DecimalType,
      context: String = ""): ArithmeticException = {
    new ArithmeticException(s"${value.toDebugString} cannot be represented as " +
      s"Decimal(${toType.precision}, ${toType.scale}).")
  }

  def overflowInIntegralDivideError(context: String = ""): ArithmeticException = {
    new ArithmeticException("Overflow in integral divide.")
  }

  def foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName: String, matchedFields: String): Throwable = {
    new RuntimeException(s"""Found duplicate field(s) "$requiredFieldName": """ +
        s"$matchedFields in case-insensitive mode")
  }

  def tableIdentifierExistsError(tableIdentifier: TableIdentifier): Throwable = {
    throw new AnalysisException(s"$tableIdentifier already exists.")
  }
}
