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
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.Spark350PlusNonDBShims

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.rapids.shims.InvokeExprMeta

object SparkShimImpl extends Spark350PlusNonDBShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val shimExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Literal](
        "Holds a static value from the query",
        ExprChecks.projectAndAst(
          TypeSig.astTypes,
          (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.CALENDAR
            + TypeSig.BINARY + TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT
            + TypeSig.ansiIntervals + TypeSig.OBJECT)
            .nested(TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.BINARY +
              TypeSig.ARRAY + TypeSig.MAP + TypeSig.STRUCT),
          TypeSig.all),
        (lit, conf, p, r) => new LiteralExprMeta(lit, conf, p, r)),
      GpuOverrides.expr[Invoke](
        "Calls the specified function on an object. This is a wrapper to other expressions, so " +
          "can not know the details in advance. E.g.: between is replaced by " +
          "And(GreaterThanOrEqual(ref, lower), LessThanOrEqual(ref, upper);  StructToJson is " +
          "replaced by Invoke(Literal(StructToJsonEvaluator), evaluate, string_type, arguments)",
        // Does not know Invoke wrap what expression, so use lenient checks.
        // `InvokeExprMeta` is responding to do the checking case by case
        ExprChecks.projectOnly(TypeSig.all, TypeSig.all),
        (invoke, conf, p, r) => new InvokeExprMeta(invoke, conf, p, r))
      .note("Please ignore the supported types: It's a dynamic expression, the supported types " +
        "are not deterministic.")
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ shimExprs
  }
}
