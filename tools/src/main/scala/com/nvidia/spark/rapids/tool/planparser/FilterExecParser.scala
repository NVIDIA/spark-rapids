/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.qualification.ExecInfo

case class FilterExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {

  val fullExecName = node.name + "Exec"

  override def parse: Seq[ExecInfo] = {
    // filter doesn't have duration
    val duration = None
    val (filterSpeedupFactor, isSupported) = if (checker.isExecSupported(fullExecName)) {
      (checker.getExecSpeedupFactor(fullExecName), true)
    } else {
      (1, false)
    }
    // TODO - add in parsing expressions - average speedup across?
    Seq(ExecInfo(sqlID, node.name, "", filterSpeedupFactor,
      duration, node.id, wholeStageId = None, isSupported))
  }
}
