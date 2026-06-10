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

package org.apache.spark.sql.rapids.shims

import java.lang.reflect.InvocationTargetException

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan

object SparkSessionUtils {

  def sessionFromPlan(plan: SparkPlan): SparkSession = {
    invokeNoArg(plan, "session").asInstanceOf[SparkSession]
  }

  def leafNodeDefaultParallelism(ss: SparkSession): Int = {
    invokeNoArg(ss, "leafNodeDefaultParallelism").asInstanceOf[Int]
  }

  private def invokeNoArg(target: AnyRef, methodName: String): AnyRef = {
    try {
      target.getClass.getMethod(methodName).invoke(target)
    } catch {
      case e: InvocationTargetException =>
        throw e.getCause
    }
  }
}
