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

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode

case class DataWritingCommandExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long) extends ExecParser {

  // hardcode because InsertIntoHadoopFsRelationCommand uses this same exec
  // and InsertIntoHadoopFsRelationCommand doesn't have an entry in the
  // supported execs file
  val fullExecName = "DataWritingCommandExec"

  override def parse: ExecInfo = {
    val writeFormat = checker.getWriteFormatString(node.desc)
    val writeSupported = checker.isWriteFormatsupported(writeFormat)
    val duration = None
    val speedupFactor = checker.getSpeedupFactor(fullExecName)
    val finalSpeedup = if (writeSupported) speedupFactor else 1
    // TODO - add in parsing expressions - average speedup across?
    new ExecInfo(sqlID, s"${node.name.trim} ${writeFormat.toLowerCase.trim}", "", finalSpeedup,
      duration, node.id, writeSupported, None)
  }
}
