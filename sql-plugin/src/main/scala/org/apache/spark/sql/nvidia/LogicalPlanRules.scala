/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package org.apache.spark.sql.nvidia

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

object LogicalPlanRules {
  private val dfUDFEnabledKey = "spark.rapids.sql.dfudf.enabled"

  private def toBoolean(value: String, key: String): Boolean = {
    try {
      value.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $value")
    }
  }

  private def isDFUDFEnabled(conf: SQLConf): Boolean = {
    val value = conf.getConfString(dfUDFEnabledKey, null)
    if (value == null) {
      true
    } else {
      toBoolean(value, dfUDFEnabledKey)
    }
  }

  @transient private[this] lazy val dfUDFShimsModule = {
    Class.forName("org.apache.spark.sql.nvidia.DFUDFShims" + "$")
      .getField("MODULE" + "$")
      .get(null)
  }

  @transient private[this] lazy val exprToColumnMethod =
    dfUDFShimsModule.getClass.getMethod("exprToColumn", classOf[Expression])

  @transient private[this] lazy val columnToExprMethod =
    dfUDFShimsModule.getClass.getMethod("columnToExpr", classOf[Column])

  private def exprToColumn(expr: Expression): Column =
    exprToColumnMethod.invoke(dfUDFShimsModule, expr).asInstanceOf[Column]

  private def columnToExpr(column: Column): Expression =
    columnToExprMethod.invoke(dfUDFShimsModule, column).asInstanceOf[Expression]
}

case class LogicalPlanRules() extends Rule[LogicalPlan] {
  val replacePartialFunc: PartialFunction[Expression, Expression] = {
    case f: ScalaUDF if DFUDF.getDFUDF(f.function).isDefined =>
      DFUDF.getDFUDF(f.function).map {
        dfudf => LogicalPlanRules.columnToExpr(
          dfudf(f.children.map(LogicalPlanRules.exprToColumn(_)).toArray))
      }.getOrElse{
        throw new IllegalStateException("Inconsistent results when extracting df_udf")
      }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (LogicalPlanRules.isDFUDFEnabled(plan.conf)) {
      plan.transformExpressions(replacePartialFunc)
    } else {
      plan
    }
  }
}
