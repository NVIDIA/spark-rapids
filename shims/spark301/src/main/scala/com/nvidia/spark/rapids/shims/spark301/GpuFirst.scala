/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark301

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.FirstLast
import org.apache.spark.sql.rapids.GpuFirstBase

/**
 * Parameters to GpuFirst changed in Spark 3.0.1
 */
case class GpuFirst(child: Expression, ignoreNulls: Boolean) extends GpuFirstBase(child) {
  def this(child: Expression) = this(child, false)
  def this(child: Expression, ignoreNullsExpr: Expression) = {
    this(child, FirstLast.validateIgnoreNullExpr(ignoreNullsExpr, "last"))
  }
  override def children: Seq[Expression] = child :: Nil

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }
}

