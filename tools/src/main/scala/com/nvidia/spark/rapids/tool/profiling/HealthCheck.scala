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

  def truncateFailureStr(failureStr: String): String = {
    failureStr.substring(0, Math.min(failureStr.size, 100))
  }

  // Function to list all failed tasks , stages and jobs.
  def listFailedTasks(): Unit = {
    val tasksMessageHeader = s"\nFailed tasks:\n"
    fileWriter.foreach(_.write(tasksMessageHeader))
    val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
      "attempt", "failureReason")
    val failed = apps.flatMap { app =>
      val tasksFailed = app.taskEnd.filter(_.successful == false)
      tasksFailed.map { t =>
        Seq(app.index.toString, t.stageId.toString, t.stageAttemptId.toString,
          t.taskId.toString, t.attempt.toString,
          truncateFailureStr(t.endReason))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols =>
        (cols(0).toLong, cols(1).toLong, cols(2).toLong, cols(3).toLong, cols(4).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Failed Tasks Found!\n"))
    }
  }

  def listFailedStages(): Unit = {
    val stagesMessageHeader = s"\nFailed stages:\n"
    fileWriter.foreach(_.write(stagesMessageHeader))
    val outputHeaders = Seq("appIndex", "stageId", "attemptId", "name",
      "numTasks", "failureReason")
    val failed = apps.flatMap { app =>
      val stagesFailed = app.liveStages.filter { case (_, sc) =>
        sc.failureReason.nonEmpty
      }
      stagesFailed.map { case ((id, attId), sc) =>
        val failureStr = sc.failureReason.getOrElse("")
        Seq(app.index.toString, id.toString, attId.toString,
          sc.info.name, sc.info.numTasks.toString,
          truncateFailureStr(failureStr))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols => (cols(0).toLong, cols(1).toLong, cols(2).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Failed Stages Found!\n"))
    }
  }

  def listFailedJobs(): Unit = {
    val jobsMessageHeader = s"\nFailed jobs:\n"
    fileWriter.foreach(_.write(jobsMessageHeader))
    val outputHeaders = Seq("appIndex", "jobId", "jobResult", "failureReason")
    val failed = apps.flatMap { app =>
      val jobsFailed = app.liveJobs.filter { case (_, jc) =>
        jc.jobResult.nonEmpty && !jc.jobResult.get.equals("JobSucceeded")
      }
      jobsFailed.map { case (id, jc) =>
        val failureStr = jc.failedReason.getOrElse("")
        Seq(app.index.toString, id.toString, jc.jobResult.getOrElse("Unknown"),
          truncateFailureStr(failureStr))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols => (cols(0).toLong, cols(1).toLong, cols(2).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Failed Jobs Found!\n"))
    }
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
