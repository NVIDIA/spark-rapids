/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.lang.reflect.{InvocationTargetException, Method}

import com.nvidia.spark.rapids.{GpuExec, GpuWrite}

import org.apache.spark.sql.execution.SparkPlan

object ReplaceDataExecShim {
  private val gpuReplaceDataExecCompanion =
    "org.apache.spark.sql.execution.datasources.v2.GpuReplaceDataExec$"

  def convertToGpu(
      cpuExec: AnyRef,
      childPlan: SparkPlan,
      gpuWrite: GpuWrite): GpuExec = {
    val refreshCache = invokeNoArg(cpuExec, "refreshCache")
    val companion = Class.forName(gpuReplaceDataExecCompanion).getField("MODULE$").get(null)
    maybeInvokeNoArg(cpuExec, "projections") match {
      case Some(projections) =>
        invokeApply(companion, 4, childPlan, refreshCache, projections, gpuWrite)
      case None =>
        invokeApply(companion, 3, childPlan, refreshCache, gpuWrite)
    }
  }

  private def maybeInvokeNoArg(target: AnyRef, methodName: String): Option[AnyRef] = {
    try {
      Some(invokeNoArg(target, methodName))
    } catch {
      case _: NoSuchMethodException => None
    }
  }

  private def invokeNoArg(target: AnyRef, methodName: String): AnyRef = {
    invoke(target.getClass.getMethod(methodName), target)
  }

  private def invokeApply(
      companion: AnyRef,
      parameterCount: Int,
      args: AnyRef*): GpuExec = {
    val method = companion.getClass.getMethods.find { method =>
      method.getName == "apply" && method.getParameterCount == parameterCount
    }.getOrElse {
      throw new NoSuchMethodException(
        s"GpuReplaceDataExec.apply with $parameterCount parameters")
    }
    invoke(method, companion, args: _*).asInstanceOf[GpuExec]
  }

  private def invoke(method: Method, target: AnyRef, args: AnyRef*): AnyRef = {
    try {
      method.invoke(target, args: _*)
    } catch {
      case e: InvocationTargetException =>
        throw e.getCause
    }
  }
}
