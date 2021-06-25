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

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

/**
 * HealthCheck defined health check rules
 */
class HealthCheck(apps: Seq[ApplicationInfo], textFileWriter: ToolTextFileWriter) {

  require(apps.nonEmpty)

  // Function to list all failed tasks , stages and jobs.
  def listFailedJobsStagesTasks(): Unit = {
    val tasksMessageHeader = s"\nFailed tasks:\n"
    val tasksQuery = apps
        .filter { p =>
          p.allDataFrames.contains(s"taskDF_${p.index}")
        }.map(app => "(" + app.getFailedTasks + ")")
        .mkString(" union ")
    if (tasksQuery.nonEmpty) {
      apps.head.runQuery(tasksQuery + "order by appIndex", false,
        fileWriter = Some(textFileWriter), messageHeader = tasksMessageHeader)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }

    val stagesMessageHeader = s"\nFailed stages:\n"
    val stagesQuery = apps
        .filter { p =>
          p.allDataFrames.contains(s"stageDF_${p.index}")
        }.map(app => "(" + app.getFailedStages + ")")
        .mkString(" union ")
    if (stagesQuery.nonEmpty) {
      apps.head.runQuery(stagesQuery + "order by appIndex", false,
        fileWriter = Some(textFileWriter), messageHeader = stagesMessageHeader)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }

    val jobsMessageHeader = s"\nFailed jobs:\n"
    val jobsQuery = apps
        .filter { p =>
          p.allDataFrames.contains(s"jobDF_${p.index}")
        }.map(app => "(" + app.getFailedJobs + ")")
        .mkString(" union ")
    if (jobsQuery.nonEmpty) {
      apps.head.runQuery(jobsQuery + "order by appIndex", false,
        fileWriter = Some(textFileWriter), messageHeader = jobsMessageHeader)
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }

  //Function to list all SparkListenerBlockManagerRemoved
  def listRemovedBlockManager(): Unit = {
    for (app <- apps) {
      if (app.allDataFrames.contains(s"blockManagersRemovedDF_${app.index}")) {
        val blockManagersMessageHeader =
          s"Removed BlockManager(s):\n"
        app.runQuery(query = app.getblockManagersRemoved, fileWriter = Some(textFileWriter),
          messageHeader = blockManagersMessageHeader)
      }
    }
  }

  //Function to list all SparkListenerExecutorRemoved
  def listRemovedExecutors(): Unit = {
    for (app <- apps) {
      if (app.allDataFrames.contains(s"executorsRemovedDF_${app.index}")) {
        val executorsRemovedMessageHeader =
          s"Removed Executors(s):\n"
        app.runQuery(query = app.getExecutorsRemoved, fileWriter = Some(textFileWriter),
          messageHeader = executorsRemovedMessageHeader)
      }
    }
  }

  //Function to list all *possible* not-supported plan nodes if GPU Mode=on
  def listPossibleUnsupportedSQLPlan(): Unit = {
    textFileWriter.write("\nSQL Plan HealthCheck:\n")
    val query = apps
        .filter { p =>
          (p.allDataFrames.contains(s"sqlDF_${p.index}") && p.sqlPlan.nonEmpty)
        }.map(app => "(" + app.unsupportedSQLPlan + ")")
        .mkString(" union ")

    if (query.nonEmpty) {
      apps.head.runQuery(query + "order by appIndex", false,
        fileWriter = Some(textFileWriter), messageHeader = s"\nUnsupported SQL Plan\n")
    } else {
      apps.head.sparkSession.emptyDataFrame
    }
  }
}
