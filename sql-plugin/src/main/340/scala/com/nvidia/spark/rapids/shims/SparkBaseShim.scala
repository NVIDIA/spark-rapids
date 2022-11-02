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

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.{DecimalUtil, ExprChecks, ExprMeta, ExprRule, GpuCheckOverflow, GpuExpression, SparkShims, TypeSig}
import com.nvidia.spark.rapids.GpuOverrides.expr

import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Divide, Expression, Multiply}
import org.apache.spark.sql.rapids.{GpuDecimalDivide, GpuDecimalMultiply}

/**
 * Base class of every Shim
 */
trait SparkBaseShim extends SparkShims {
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    // return an empty Map
    Map.empty[Class[_ <: Expression], ExprRule[_ <: Expression]]
  }
}
