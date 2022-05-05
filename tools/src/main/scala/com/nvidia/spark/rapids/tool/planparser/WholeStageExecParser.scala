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

import org.apache.spark.sql.execution.ui.SparkPlanGraphCluster
import org.apache.spark.sql.rapids.tool.AppBase

case class WholeStageExecParser(
    node: SparkPlanGraphCluster,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) {

  val fullExecName = "WholeStageCodegenExec"

  def parse: Seq[ExecInfo] = {
    // TODO - does metrics for time have previous ops?  per op thing, only some do
    // the durations in wholestage code gen can include durations of other wholestage code
    // gen in the same stage, so we can't just add them all up.
    // Perhaps take the max of those in Stage?
    val accumId = node.metrics.find(_.name == "duration").map(_.accumulatorId)
    val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)
    val stagesInNode = SQLPlanParser.getStagesInSQLNode(node, app)

    val childNodeRes = node.nodes.flatMap { c =>
      SQLPlanParser.parsePlanNode(c, sqlID, checker, app)
    }
    // if any of the execs in WholeStageCodegen supported mark this entire thing
    // as supported
    val anySupported = childNodeRes.exists(_.isSupported == true)
    // average speedup across the execs in the WholeStageCodegen for now
    val supportedChildren = childNodeRes.filterNot(_.isSupported)
    val avSpeedupFactor = SQLPlanParser.averageSpeedup(supportedChildren.map(_.speedupFactor))
    val execInfo = ExecInfo(sqlID, node.name, node.name, avSpeedupFactor, maxDuration,
      node.id, anySupported, Some(childNodeRes), stagesInNode)
    Seq(execInfo)
  }
}
