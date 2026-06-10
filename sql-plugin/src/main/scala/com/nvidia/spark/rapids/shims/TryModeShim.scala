/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

import java.lang.reflect.InvocationTargetException

import org.apache.spark.sql.catalyst.expressions.Expression

object TryModeShim {
  private val TryModeName = "TRY"

  def isTryMode(expr: Expression): Boolean = {
    evalMode(expr).exists(mode => String.valueOf(mode) == TryModeName)
  }

  private def evalMode(expr: Expression): Option[AnyRef] = {
    invokeNoArg(expr, "evalMode").orElse {
      invokeNoArg(expr, "evalContext").flatMap(invokeNoArg(_, "evalMode"))
    }
  }

  private def invokeNoArg(target: AnyRef, methodName: String): Option[AnyRef] = {
    try {
      val method = target.getClass.getMethod(methodName)
      Option(method.invoke(target).asInstanceOf[AnyRef])
    } catch {
      case _: NoSuchMethodException =>
        None
      case e: InvocationTargetException if e.getCause != null =>
        throw e.getCause
      case _: IllegalAccessException =>
        None
    }
  }
}
