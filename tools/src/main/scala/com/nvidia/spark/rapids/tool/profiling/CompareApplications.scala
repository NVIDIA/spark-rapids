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

import java.io.FileWriter

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

/**
 * CompareApplications compares multiple ApplicationInfo objects
 */
class CompareApplications(apps: ArrayBuffer[ApplicationInfo],
    fileWriter: FileWriter) extends Logging {

  require(apps.size>1)

  // Compare the App Information.
  def compareAppInfo(): Unit = {
    val messageHeader = "Compare Application Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      query += app.generateAppInfo
      if (i < apps.size) {
        query += "\n union \n"
      } else {
        query += " order by appIndex"
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = Some(fileWriter), messageHeader = messageHeader)
  }

  // Compare Executors information
  def compareExecutorInfo(): Unit = {
    val messageHeader = "\n\nCompare Executor Information:\n"
    var query = ""
    var i = 1
    for (app <- apps) {
      query += app.generateExecutorInfo
      if (i < apps.size) {
        query += "\n union \n"
      } else {
        query += " order by appIndex, cast(executorID as long)"
      }
      i += 1
    }
    apps.head.runQuery(query = query, fileWriter = Some(fileWriter), messageHeader = messageHeader)
  }

  // Compare Rapids Properties which are set explicitly
  def compareRapidsProperties(): Unit ={
    val messageHeader = "\n\nCompare Rapids Properties which are set explicitly:\n"
    var withClauseAllKeys = "with allKeys as \n ("
    val selectKeyPart = "select allKeys.key"
    var selectValuePart = ""
    var query = " allKeys LEFT OUTER JOIN \n"
    var i = 1
    for (app <- apps) {
      // For the 1st app
      if (i == 1) {
        withClauseAllKeys += "select distinct key from (" +
            app.generateRapidsProperties + ") union "
        query += "(" + app.generateRapidsProperties + s") tmp_$i"
        query += s" on allKeys.key=tmp_$i.key"
        query += "\n LEFT OUTER JOIN \n"
      } else if (i < apps.size) { // For the 2nd to non-last app(s)
        withClauseAllKeys += "select distinct key from (" +
            app.generateRapidsProperties + ") union "
        query += "(" + app.generateRapidsProperties + s") tmp_$i"
        query += s" on allKeys.key=tmp_$i.key"
        query += "\n LEFT OUTER JOIN \n"
      } else { // For the last app
        withClauseAllKeys += "select distinct key from (" +
            app.generateRapidsProperties + "))\n"
        query += "(" + app.generateRapidsProperties + s") tmp_$i"
        query += s" on allKeys.key=tmp_$i.key"
      }
      selectValuePart += s",value_app$i"
      i += 1
    }

    query = withClauseAllKeys + selectKeyPart + selectValuePart +
        " from (\n" + query + "\n) order by key"
    logDebug("Running query " + query)
    apps.head.runQuery(query = query, fileWriter = Some(fileWriter), messageHeader = messageHeader)
  }
}