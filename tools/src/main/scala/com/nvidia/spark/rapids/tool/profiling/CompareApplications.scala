/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

/**
 * CompareApplications compares multiple ApplicationInfo objects
 */
class CompareApplications(apps: Seq[ApplicationInfo],
    fileWriter: Option[ToolTextFileWriter]) extends Logging {

  require(apps.size>1)

  // Compare the App Information.
  def compareAppInfo(): Unit = {
    val messageHeader = "\n\nCompare Application Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"appDF_${app.index}")) {
        query += app.generateAppInfo
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Application Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Job information
  def compareJobInfo(): Unit = {
    val messageHeader = "\n\nCompare Job Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"jobDF_${app.index}")) {
        query += app.jobtoStagesSQL
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Job Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Executors information
  def compareExecutorInfo(): Unit = {
    val messageHeader = "\n\nCompare Executor Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"executorsDF_${app.index}")) {
        query += app.generateExecutorInfo
        if (i < apps.size) {
          query += "\n union \n"
        } else {
          query += " order by appIndex"
        }
      } else {
        fileWriter.foreach(_.write("No Executor Information Found!\n"))
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }

  // Compare Rapids Properties which are set explicitly
  def compareRapidsProperties(): Unit = {
    val messageHeader = "\n\nCompare Rapids Properties which are set explicitly:\n"
    var withClauseAllKeys = "with allKeys as \n ("
    val selectKeyPart = "select allKeys.propertyName"
    var selectValuePart = ""
    var query = " allKeys LEFT OUTER JOIN \n"
    var i = 1
    for (app <- apps) {
      if (app.allDataFrames.contains(s"propertiesDF_${app.index}")) {
        if (i < apps.size) {
          withClauseAllKeys += "select distinct propertyName from (" +
              app.generateRapidsProperties + ") union "
          query += "(" + app.generateRapidsProperties + s") tmp_$i"
          query += s" on allKeys.propertyName=tmp_$i.propertyName"
          query += "\n LEFT OUTER JOIN \n"
        } else { // For the last app
          withClauseAllKeys += "select distinct propertyName from (" +
              app.generateRapidsProperties + "))\n"
          query += "(" + app.generateRapidsProperties + s") tmp_$i"
          query += s" on allKeys.propertyName=tmp_$i.propertyName"
        }
        selectValuePart += s",appIndex_${app.index}"
      } else {
        fileWriter.foreach(_.write("No Spark Rapids parameters Found!\n"))
      }
      i += 1
    }

    query = withClauseAllKeys + selectKeyPart + selectValuePart +
        " from (\n" + query + "\n) order by propertyName"
    logDebug("Running query " + query)
    apps.head.runQuery(query = query, fileWriter = fileWriter, messageHeader = messageHeader)
  }
}
