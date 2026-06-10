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

package com.nvidia.spark.rapids

import scala.reflect.ClassTag

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{DataWritingCommand, RunnableCommand}

final class ShimExecRule[INPUT <: SparkPlan] private (
    val desc: String,
    val tag: ClassTag[INPUT])

object ShimExecRule {
  def apply[INPUT <: SparkPlan](desc: String)(
      implicit tag: ClassTag[INPUT]): ShimExecRule[INPUT] = {
    require(desc != null)
    new ShimExecRule[INPUT](desc, tag)
  }
}

final class ShimDataWritingCommandRule[INPUT <: DataWritingCommand] private (
    val desc: String,
    val tag: ClassTag[INPUT])

object ShimDataWritingCommandRule {
  def apply[INPUT <: DataWritingCommand](desc: String)(
      implicit tag: ClassTag[INPUT]): ShimDataWritingCommandRule[INPUT] = {
    require(desc != null)
    new ShimDataWritingCommandRule[INPUT](desc, tag)
  }
}

final class ShimRunnableCommandRule[INPUT <: RunnableCommand] private (
    val desc: String,
    val tag: ClassTag[INPUT])

object ShimRunnableCommandRule {
  def apply[INPUT <: RunnableCommand](desc: String)(
      implicit tag: ClassTag[INPUT]): ShimRunnableCommandRule[INPUT] = {
    require(desc != null)
    new ShimRunnableCommandRule[INPUT](desc, tag)
  }
}
