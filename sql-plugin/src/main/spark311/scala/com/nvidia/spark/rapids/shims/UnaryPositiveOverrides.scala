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
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
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
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuExpression, TypeSig, UnaryAstExprMeta}
import com.nvidia.spark.rapids.GpuOverrides.expr

import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryPositive}
import org.apache.spark.sql.rapids.GpuUnaryPositive

object UnaryPositiveOverrides {
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
    expr[UnaryPositive](
      "A numeric value with a + in front of it",
      ExprChecks.unaryProjectAndAstInputMatchesOutput(
        TypeSig.astTypes + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.gpuNumeric + GpuTypeShims.additionalArithmeticSupportedTypes,
        TypeSig.numericAndInterval),
      (a, conf, p, r) => new UnaryAstExprMeta[UnaryPositive](a, conf, p, r) {
        override def convertToGpu(child: Expression): GpuExpression = GpuUnaryPositive(child)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
}
