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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimUnaryExpression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.rapids.shims.RapidsErrorUtils
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Handle the new Expression added in Spark-3.3.1+.
 * Casting a numeric value as another numeric type in store assignment. It captures the arithmetic
 * exception thrown by Cast and shows a relevant error message.
 * The current implementation in Spark creates a child of type Cast with AnsiEnabled set to true.
 *
 * The GPU equivalent columnar evaluation is a delegation to the child's columnar evaluation which
 * is a GpuCast. In order to match Spark's Exception, we need to catch the Exception passing it to
 * QueryExecutionError.
 * The calculation of the sideEffect is delegated to the child Expression "GpuCast".
 */
case class GpuCheckOverflowInTableInsert(child: Expression, columnName: String)
  extends ShimUnaryExpression with GpuExpression {

  override def dataType: DataType = child.dataType

  override def columnarEval(batch: ColumnarBatch): Any = {
    try {
      child.columnarEval(batch)
    } catch {
      // Safeguard making sure that we are not handling all exceptions as Overflow.
      // Converting any exception to Overflow would cause incorrect behavior and misleading
      // stack trace errors.
      case e: Exception if isOverflow(e) =>
        throw RapidsErrorUtils.castCausesOverflowInTableInsert(
          child.asInstanceOf[GpuCast].child.dataType, child.dataType, columnName)
    }
  }

  private def isOverflow(e: Exception): Boolean = {
    // GpuCast throws three different exceptions: Arithmetic, NumberFormat, and IllegalState.
    // There are three different error messages: INVALID_INPUT, OVERFLOW, and INVALID_NUMBER.
    // Currently, we treat Arithmetic and IllegalState as an overflow.
    e match {
      case _: ArithmeticException | _: IllegalStateException =>
        child.isInstanceOf[GpuCast] &&
          (e.getMessage.contains(GpuCast.INVALID_INPUT_MESSAGE)
            || e.getMessage.contains(GpuCast.OVERFLOW_MESSAGE))
      case _ => false
    }
  }

  override def toString: String = s"GpuCheckOverflowInTableInsert($child, $columnName)"
}
