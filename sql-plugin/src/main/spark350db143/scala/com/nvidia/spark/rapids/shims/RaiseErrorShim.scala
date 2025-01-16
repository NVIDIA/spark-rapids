/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
{"spark": "350db143"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprRule, GpuOverrides}
import com.nvidia.spark.rapids.{ExprChecks, GpuExpression, TypeSig, BinaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, RaiseError}
import org.apache.spark.sql.rapids.shims.GpuRaiseError

object RaiseErrorShim {
  val exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(GpuOverrides.expr[RaiseError](
      "Throw an exception",
      ExprChecks.binaryProject(
        TypeSig.NULL, TypeSig.NULL,
        ("errorClass", TypeSig.STRING, TypeSig.STRING),
        ("errorParams", TypeSig.MAP.nested(TypeSig.STRING), TypeSig.MAP.nested(TypeSig.STRING)),
      ),
      (a, conf, p, r) => new BinaryExprMeta[RaiseError](a, conf, p, r) {

        override def tagExprForGpu(): Unit = {
          // In Databricks 14.3 and Spark 4.0, RaiseError forwards the lhs expression (i.e. the error-class)
          // as a scalar value.  A vector/column here would be surprising.
          a.errorClass match {
            case _: Literal => // Supported.
            case _ => willNotWorkOnGpu(s"expected error-class to be a STRING literal")
          }
        }

        override def convertToGpu(lhsErrorClass: Expression, rhsErrorParams: Expression): GpuExpression =
          GpuRaiseError(lhsErrorClass, rhsErrorParams)
      })).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
