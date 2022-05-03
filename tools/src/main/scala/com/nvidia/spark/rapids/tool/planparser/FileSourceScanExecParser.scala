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

case class FileSourceScanExecParser(
    node: SparkPlanGraphNode,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase) extends ExecParser with Logging {

  val fullExecName = "FileSourceScanExec"

  // strip off the struct<> part that Spark adds to the ReadSchema
  private def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }
  // This tries to get just the field specified by tag in a string that
  // may contain multiple fields.  It looks for a comma to delimit fields.
  private def getFieldWithoutTag(str: String, tag: String): String = {
    val index = str.indexOf(tag)
    // remove the tag from the final string retruned
    val subStr = str.substring(index + tag.size)
    val endIndex = subStr.indexOf(", ")
    if (endIndex != -1) {
      subStr.substring(0, endIndex)
    } else {
      subStr
    }
  }

  private def parseRead: Unit = {
    val schemaTag = "ReadSchema: "
    val schema = if (node.desc.contains(schemaTag)) {
      formatSchemaStr(getFieldWithoutTag(node.desc, schemaTag))
    } else {
      ""
    }
    logWarning(s"schema is: $schema")
    val locationTag = "Location: "
    val location = if (node.desc.contains(locationTag)) {
      getFieldWithoutTag(node.desc, locationTag)
    } else if (node.name.contains("JDBCRelation")) {
      // see if we can report table or query
      val JDBCPattern = raw".*JDBCRelation\((.*)\).*".r
      node.name match {
        case JDBCPattern(tableName) => tableName
        case _ => "unknown"
      }
    } else {
      "unknown"
    }
    logWarning(s"location is: $location")

    val pushedFilterTag = "PushedFilters: "
    val pushedFilters = if (node.desc.contains(pushedFilterTag)) {
      getFieldWithoutTag(node.desc, pushedFilterTag)
    } else {
      "unknown"
    }
    logWarning(s"pushedFilters is: $pushedFilters")

    val formatTag = "Format: "
    val fileFormat = if (node.desc.contains(formatTag)) {
      val format = getFieldWithoutTag(node.desc, formatTag)
      if (node.name.startsWith("Gpu")) {
        s"${format}(GPU)"
      } else {
        format
      }
    } else if (node.name.contains("JDBCRelation")) {
      "JDBC"
    } else {
      "unknown"
    }
    logWarning(s"fileFormat is: $fileFormat")

  }

  override def parse: Seq[ExecInfo] = {
    // can we use scan time?
    val accumId = node.metrics.find(_.name == "scan time").map(_.accumulatorId)
    val maxDuration = SQLPlanParser.getTotalDuration(accumId, app)
    logWarning(s"file source scan ${node.desc} scan time " +
      s"accum: $accumId max duration: $maxDuration")

    //     if (planInfo.metadata != null && planInfo.metadata.contains("ReadSchema")) {

    val (filterSpeedupFactor, isSupported) = if (checker.isExecSupported(fullExecName)) {
      (checker.getSpeedupFactor(fullExecName), true)
    } else {
      (1, false)
    }
    val stagesInNode = SQLPlanParser.getStagesInSQLNode(node, app)
    // TODO - add in parsing expressions - average speedup across?
    Seq(ExecInfo(sqlID, node.name, "", filterSpeedupFactor,
      maxDuration, node.id, isSupported, None, stagesInNode))
  }
}
