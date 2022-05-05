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

import java.util.concurrent.TimeUnit.NANOSECONDS

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase

case class ShuffleExchangeExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {

  val fullExecName = "ShuffleExchangeExec"

  override def parse: ExecInfo = {
    // TODO - check the partitioning (hash partitioining)
    // these timings are in nanoseconds
    val writeId = node.metrics.find(_.name == "shuffle write time").map(_.accumulatorId)
    val maxWriteTime = SQLPlanParser.getTotalDuration(writeId, app).map(NANOSECONDS.toMillis(_))
    logWarning(s"shuffle write id: $writeId time $maxWriteTime")
    val fetchId = node.metrics.find(_.name == "fetch wait time").map(_.accumulatorId)
    val maxFetchTime = SQLPlanParser.getTotalDuration(fetchId, app).map(NANOSECONDS.toMillis(_))
    logWarning(s"shuffle fetch id: $fetchId time $maxFetchTime")

    val duration = (maxWriteTime ++ maxFetchTime).reduceOption(_ + _)
    val (filterSpeedupFactor, isSupported) = if (checker.isExecSupported(fullExecName)) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1, false)
    }
    // TODO - add in parsing expressions - average speedup across?
    ExecInfo(sqlID, node.name, "", filterSpeedupFactor, duration, node.id, isSupported, None)
  }
}
