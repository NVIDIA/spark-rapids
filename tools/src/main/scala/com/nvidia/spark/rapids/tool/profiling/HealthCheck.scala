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
class HealthCheck(apps: Seq[ApplicationInfo], fileWriter: Option[ToolTextFileWriter],
    numOutputRows: Int) {

  // Function to list all failed tasks , stages and jobs.
  def listFailedJobsStagesTasks(): Unit = {
    val tasksMessageHeader = s"\nFailed tasks:\n"
    fileWriter.foreach(_.write(tasksMessageHeader))
    val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
      "attempt", "failureReason")
    val failed = apps.flatMap { app =>
      val tasksFailed = app.taskEnd.filter(_.successful == false)
      tasksFailed.map { t =>
        Seq(app.index.toString, t.stageId.toString, t.stageAttemptId.toString,
          t.taskId.toString, t.attempt.toString,
          t.endReason.substring(0, Math.min(t.endReason.size - 1, 100)))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols => (cols(0).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Job Information Found!\n"))
    }


/*
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
    */
  }

  /*
  //Function to list all SparkListenerBlockManagerRemoved
  def listRemovedBlockManager(): Unit = {
    val header = "\nRemoved BlockManager(s):\n"
    val query = apps
      .filter { p =>
        (p.allDataFrames.contains(s"blockManagersRemovedDF_${p.index}"))
      }.map(app => "(" + app.getblockManagersRemoved + ")")
      .mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query + "order by appIndex, executorID", false,
        fileWriter = Some(textFileWriter), messageHeader = header)
    }
  }

  //Function to list all SparkListenerExecutorRemoved
  def listRemovedExecutors(): Unit = {
    val header = "\nRemoved Executors(s):\n"
    val query = apps
      .filter { p =>
        (p.allDataFrames.contains(s"executorsRemovedDF_${p.index}"))
      }.map(app => "(" + app.getExecutorsRemoved + ")")
      .mkString(" union ")
    if (query.nonEmpty) {
      apps.head.runQuery(query + "order by appIndex, executorID", false,
        fileWriter = Some(textFileWriter), messageHeader = header)
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
  */
}
