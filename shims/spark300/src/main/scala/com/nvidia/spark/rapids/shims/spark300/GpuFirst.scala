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

package com.nvidia.spark.rapids.shims.spark300

import com.nvidia.spark.rapids.GpuLiteral

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.rapids.GpuFirstBase

/**
 * Parameters to GpuFirst changed in 3.0.1
 */
case class GpuFirst(child: Expression, ignoreNullsExpr: Expression) extends GpuFirstBase(child) {
  override def children: Seq[Expression] = child :: ignoreNullsExpr :: Nil

  override val ignoreNulls: Boolean = ignoreNullsExpr match {
    case l: Literal => l.value.asInstanceOf[Boolean]
    case l: GpuLiteral => l.value.asInstanceOf[Boolean]
    case _ => throw new IllegalArgumentException(
      s"$this should only receive literals for ignoreNulls expression")
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!ignoreNullsExpr.foldable) {
      TypeCheckFailure(s"The second argument of GpuFirst must be a boolean literal, but " +
        s"got: ${ignoreNullsExpr.sql}")
    } else {
      TypeCheckSuccess
    }
  }
}

