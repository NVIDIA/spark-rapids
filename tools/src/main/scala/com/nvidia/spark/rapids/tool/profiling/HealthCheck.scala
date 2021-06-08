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

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

/**
 * HealthCheck defined health check rules
 */
class HealthCheck(apps: ArrayBuffer[ApplicationInfo], fileWriter: FileWriter){

  require(apps.nonEmpty)

  // Function to list all failed tasks , stages and jobs.
  def listFailedJobsStagesTasks(): Unit = {
    for (app <- apps) {
      // Look for failed tasks.
      val tasksMessageHeader = s"Application ${app.appId} (index=${app.index}) failed tasks:\n"
      app.runQuery(query = app.getFailedTasks, fileWriter = Some(fileWriter),
        messageHeader = tasksMessageHeader)

      // Look for failed stages.
      val stagesMessageHeader = s"Application ${app.appId} (index=${app.index}) failed stages:\n"
      app.runQuery(query = app.getFailedStages, fileWriter = Some(fileWriter),
        messageHeader = stagesMessageHeader)

      // Look for failed jobs.
      val jobsMessageHeader = s"Application ${app.appId} (index=${app.index}) failed jobs:\n"
      app.runQuery(query = app.getFailedJobs, fileWriter = Some(fileWriter),
        messageHeader = jobsMessageHeader)
    }
  }

  //Function to list all SparkListenerBlockManagerRemoved
  def listRemovedBlockManager(): Unit = {
    for (app <- apps) {
      if (app.allDataFrames.contains(s"blockManagersRemovedDF_${app.index}")) {
        val blockManagersMessageHeader =
          s"Application ${app.appId} (index=${app.index}) removed BlockManager(s):\n"
        app.runQuery(query = app.getblockManagersRemoved, fileWriter = Some(fileWriter),
          messageHeader = blockManagersMessageHeader)
      }
    }
  }

  //Function to list all SparkListenerExecutorRemoved
  def listRemovedExecutors(): Unit = {
    for (app <- apps) {
      if (app.allDataFrames.contains(s"executorsRemovedDF_${app.index}")) {
        val executorsRemovedMessageHeader =
          s"Application ${app.appId} (index=${app.index}) removed Executors(s):\n"
        app.runQuery(query = app.getExecutorsRemoved, fileWriter = Some(fileWriter),
          messageHeader = executorsRemovedMessageHeader)
      }
    }
  }

  //Function to list all *possible* not-supported plan nodes if GPU Mode=on
  def listPossibleUnsupportedSQLPlan(): Unit = {
    fileWriter.write("\nSQL Plan HealthCheck: Not supported SQL Plan\n")
    for (app <- apps) {
      if (app.allDataFrames.contains(s"sqlDF_${app.index}") && app.sqlPlan.nonEmpty) {
        app.runQuery(query = app.unsupportedSQLPlan, fileWriter = Some(fileWriter),
          messageHeader = s"Application ${app.appId} (index=${app.index})\n")
      }
    }
  }
}
