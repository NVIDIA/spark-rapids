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
import org.apache.spark.sql.execution.ui.SparkPlanGraphCluster
import org.apache.spark.sql.rapids.tool.AppBase

case class WholeStageExecParser(
    node: SparkPlanGraphCluster,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {

  val fullExecName = "WholeStageCodegenExec"

  override def parse: Seq[ExecInfo] = {
    // TODO - does metrics for time have previous ops?  per op thing, likely does
    //  but verify
    val accumId = node.metrics.find(_.name == "duration").map(_.accumulatorId)
    val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)

    // TODO - most of the time children those don't have timings but check all
    // TODO - add in expression checking
    val childrenSpeedupFactors = node.nodes.map { c =>
      val fullExecName = c.name + "Exec"
      if (checker.isExecSupported(fullExecName)) {
        val nodeRes = SQLPlanParser.parsePlanNode(c, sqlID, checker, app)
        val speedupFactor = checker.getExecSpeedupFactor(fullExecName)
        ExecInfo(sqlID, node.name, c.name, speedupFactor, duration=None,
          c.id, Some(node.id), isSupported=true)
      } else {
        // if not supported speedupFactor = 1 which means no speedup
        ExecInfo(sqlID, node.name, c.name, 1, duration=None, c.id, Some(node.id),
          isSupported=false)
      }
    }
    // TODO - average speedup across the execs in the WholeStageCodegen for now
    val avSpeedupFactor = SQLPlanParser.average(childrenSpeedupFactors.map(_.speedupFactor))
    // if any of the execs in WholeStageCodegen supported mark this entire thing
    // as supported
    val anySupported = childrenSpeedupFactors.exists(_.isSupported == true)
    val wholeStageSpeedup = ExecInfo(sqlID, node.name, node.name, avSpeedupFactor,
      maxDuration, node.id, wholeStageId=None, anySupported)
    childrenSpeedupFactors += wholeStageSpeedup
  }
}
