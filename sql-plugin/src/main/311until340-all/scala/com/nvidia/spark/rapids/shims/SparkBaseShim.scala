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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids.{ExprChecks, ExprRule, GpuExpression, GpuOverrides, GpuPromotePrecision, SparkShims, TypeSig, UnaryExprMeta}

import org.apache.spark.sql.catalyst.expressions.{Expression, PromotePrecision}

/**
 * Base class of every Shim
 */
trait SparkBaseShim extends SparkShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    Seq(
      GpuOverrides.expr[PromotePrecision](
        "PromotePrecision before arithmetic operations between DecimalType data",
        ExprChecks.unaryProjectInputMatchesOutput(TypeSig.DECIMAL_128,
          TypeSig.DECIMAL_128),
        (a, conf, p, r) => new UnaryExprMeta[PromotePrecision](a, conf, p, r) {
          override def convertToGpu(child: Expression): GpuExpression = GpuPromotePrecision(child)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
  }
}
