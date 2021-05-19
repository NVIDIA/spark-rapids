/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Predicate}

case class GpuInSet(
    child: Expression,
    list: Seq[Any]) extends GpuUnaryExpression with Predicate {
  @transient private[this] lazy val _needles: ThreadLocal[ColumnVector] =
    new ThreadLocal[ColumnVector]

  require(list != null, "list should not be null")

  override def nullable: Boolean = child.nullable || list.contains(null)

  override def doColumnar(haystack: GpuColumnVector): ColumnVector = {
    val needles = getNeedles
    haystack.getBase.contains(needles)
  }

  private def getNeedles: ColumnVector = {
    var needleVec = _needles.get
    if (needleVec == null) {
      needleVec = buildNeedles
      _needles.set(needleVec)
      TaskContext.get.addTaskCompletionListener[Unit](_ => _needles.get.close())
    }
    needleVec
  }

  private def buildNeedles: ColumnVector =
    GpuScalar.columnVectorFromLiterals(list, child.dataType)

  override def toString: String = s"$child INSET ${list.mkString("(", ",", ")")}"

  override def sql: String = {
    val valueSQL = child.sql
    val listSQL = list.map(Literal(_).sql).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }
}
