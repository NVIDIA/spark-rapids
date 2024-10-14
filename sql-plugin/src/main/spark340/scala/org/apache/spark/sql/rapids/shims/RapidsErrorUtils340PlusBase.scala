/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkDateTimeException
import org.apache.spark.sql.catalyst.trees.{Origin, SQLQueryContext}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}

trait RapidsErrorUtils340PlusBase extends RapidsErrorUtilsFor330plus with RapidsQueryErrorUtils {

  def mapKeyNotExistError(
      key: String,
      keyType: DataType,
      origin: Origin): NoSuchElementException = {
    throw new UnsupportedOperationException(
      "`mapKeyNotExistError` has been removed since Spark 3.4.0. "
    )
  }

  def invalidArrayIndexError(
      index: Int,
      numElements: Int,
      isElementAtF: Boolean = false,
      context: SQLQueryContext = null): ArrayIndexOutOfBoundsException = {
    if (isElementAtF) {
      QueryExecutionErrors.invalidElementAtIndexError(index, numElements, context)
    } else {
      QueryExecutionErrors.invalidArrayIndexError(index, numElements, context)
    }
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: SQLQueryContext = null): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(message, hint, errorContext)
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      toType: DecimalType,
      context: SQLQueryContext = null): ArithmeticException = {
    QueryExecutionErrors.cannotChangeDecimalPrecisionError(
      value, toType.precision, toType.scale, context
    )
  }

  def overflowInIntegralDivideError(context: SQLQueryContext = null): ArithmeticException = {
    QueryExecutionErrors.arithmeticOverflowError(
      "Overflow in integral divide", "try_divide", context
    )
  }

  def sparkDateTimeException(infOrNan: String): SparkDateTimeException = {
    // These are the arguments required by SparkDateTimeException class to create error message.
    val errorClass = "CAST_INVALID_INPUT"
    val messageParameters = Map("expression" -> infOrNan, "sourceType" -> "DOUBLE",
      "targetType" -> "TIMESTAMP", "ansiConfig" -> SQLConf.ANSI_ENABLED.key)
    SparkDateTimeExceptionShims.newSparkDateTimeException(errorClass, messageParameters,
      Array.empty, "")
  }

  def sqlArrayIndexNotStartAtOneError(): RuntimeException = {
    QueryExecutionErrors.invalidIndexOfZeroError(context = null)
  }

  override def intervalDivByZeroError(origin: Origin): ArithmeticException = {
    QueryExecutionErrors.intervalDividedByZeroError(origin.context)
  }
}
