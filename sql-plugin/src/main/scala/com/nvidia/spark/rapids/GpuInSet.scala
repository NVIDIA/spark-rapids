/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}

case class GpuInSet(
    child: Expression,
    list: Seq[Any]) extends GpuUnaryExpression with Predicate {
  require(list != null, "list should not be null")

  override def nullable: Boolean = child.nullable || list.contains(null)

  override def doColumnar(haystack: GpuColumnVector): ColumnVector = {
    withResource(buildNeedles) { needles =>
      haystack.getBase.contains(needles)
    }
  }

  private def buildNeedles: ColumnVector =
    GpuScalar.columnVectorFromLiterals(list, child.dataType)

  override def toString: String = {
    val listString = list
        .map(elem => Literal(elem, child.dataType).toString)
        // Sort elements for deterministic behaviours
        .sorted
        .mkString(", ")
    s"$child INSET $listString"
  }

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = list
        .map(elem => Literal(elem, child.dataType).sql)
        // Sort elements for deterministic behaviours
        .sorted
        .mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}
