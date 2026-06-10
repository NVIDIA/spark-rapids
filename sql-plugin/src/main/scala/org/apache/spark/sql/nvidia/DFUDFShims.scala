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

import java.lang.reflect.Method

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object DFUDFShims {
  private[this] lazy val classicColumnNodeConverter: Option[AnyRef] =
    moduleFor("org.apache.spark.sql.classic.ColumnNodeToExpressionConverter$")

  private[this] lazy val classicExpressionUtils: Option[AnyRef] =
    moduleFor("org.apache.spark.sql.classic.ExpressionUtils$")

  private[this] lazy val columnNodeMethod: Method =
    classOf[Column].getMethod("node")

  private[this] lazy val classicColumnNodeApplyMethod: Method =
    classicColumnNodeConverter.get.getClass.getMethod("apply", columnNodeMethod.getReturnType)

  private[this] lazy val classicExpressionColumnMethod: Method =
    classicExpressionUtils.get.getClass.getMethod("column", classOf[Expression])

  private[this] lazy val columnExprMethod: Method =
    classOf[Column].getMethod("expr")

  private[this] lazy val columnModule: AnyRef =
    moduleFor("org.apache.spark.sql.Column$").get

  private[this] lazy val columnApplyMethod: Method =
    columnModule.getClass.getMethod("apply", classOf[Expression])

  def columnToExpr(c: Column): Expression = {
    classicColumnNodeConverter match {
      case Some(converter) =>
        val node = columnNodeMethod.invoke(c)
        classicColumnNodeApplyMethod.invoke(converter, node).asInstanceOf[Expression]
      case None =>
        columnExprMethod.invoke(c).asInstanceOf[Expression]
    }
  }

  def exprToColumn(e: Expression): Column = {
    classicExpressionUtils match {
      case Some(expressionUtils) =>
        classicExpressionColumnMethod.invoke(expressionUtils, e).asInstanceOf[Column]
      case None =>
        columnApplyMethod.invoke(columnModule, e).asInstanceOf[Column]
    }
  }

  private def moduleFor(className: String): Option[AnyRef] = {
    try {
      Some(Class.forName(className).getField("MODULE" + "$").get(null).asInstanceOf[AnyRef])
    } catch {
      case _: ClassNotFoundException => None
    }
  }
}
