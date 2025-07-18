/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "330db"}
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "344"}
{"spark": "350"}
{"spark": "350db143"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.spark.sql.catalyst.expressions.{Add, Divide, EvalMode, Expression, Multiply, Remainder, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}

object TryModeShim {
  /**
   * Check if an expression is in TRY mode.
   */
  def isTryMode(expr: Expression): Boolean = {
    expr match {
      case add: Add => add.evalMode == EvalMode.TRY
      case sub: Subtract => sub.evalMode == EvalMode.TRY
      case mul: Multiply => mul.evalMode == EvalMode.TRY
      case div: Divide => div.evalMode == EvalMode.TRY
      case mod: Remainder => mod.evalMode == EvalMode.TRY
      case avg: Average => avg.evalMode == EvalMode.TRY
      case sum: Sum => sum.evalMode == EvalMode.TRY
      case _ => throw new RuntimeException(s"Unsupported expression $expr in TRY mode")
    }
  }
}
