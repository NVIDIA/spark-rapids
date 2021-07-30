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
  def listFailedTasks(): Unit = {
    val tasksMessageHeader = s"\nFailed tasks:\n"
    fileWriter.foreach(_.write(tasksMessageHeader))
    val outputHeaders = Seq("appIndex", "stageId", "stageAttemptId", "taskId",
      "attempt", "failureReason")
    val failed = apps.flatMap { app =>
      val tasksFailed = app.taskEnd.filter(_.successful == false)
      tasksFailed.map { t =>
        FailedTaskProfileResults(app.index, t.stageId, t.stageAttemptId,
          t.taskId, t.attempt, ProfileUtils.truncateFailureStr(t.endReason))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols =>
        (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
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
      val stagesFailed = app.stageIdToInfo.filter { case (_, sc) =>
        sc.failureReason.nonEmpty
      }
      stagesFailed.map { case ((id, attId), sc) =>
        val failureStr = sc.failureReason.getOrElse("")
        Seq(app.index.toString, id.toString, attId.toString,
          sc.info.name, sc.info.numTasks.toString,
          ProfileUtils.truncateFailureStr(failureStr))
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
      val jobsFailed = app.jobIdToInfo.filter { case (_, jc) =>
        jc.jobResult.nonEmpty && !jc.jobResult.get.equals("JobSucceeded")
      }
      jobsFailed.map { case (id, jc) =>
        val failureStr = jc.failedReason.getOrElse("")
        Seq(app.index.toString, id.toString, jc.jobResult.getOrElse("Unknown"),
          ProfileUtils.truncateFailureStr(failureStr))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy { cols =>
        (cols(0).toLong, cols(1).toLong, cols(2))
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Failed Jobs Found!\n"))
    }
  }


  def listRemovedBlockManager(): Unit = {
    val header = "\nRemoved BlockManager(s):\n"
    fileWriter.foreach(_.write(header))
    val outputHeaders = Seq("appIndex", "executorId", "time")
    val res = apps.flatMap { app =>
      app.blockManagersRemoved.map { bm =>
        Seq(app.index.toString, bm.executorID.toString, bm.time.toString)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols(0).toLong, cols(1).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Removed BlockManagers Found!\n"))
    }
  }


  def listRemovedExecutors(): Unit = {
    val header = "\nRemoved Executors(s):\n"
    fileWriter.foreach(_.write(header))
    val outputHeaders = Seq("appIndex", "executorId", "time", "reason")
    val res = apps.flatMap { app =>
      val execsRemoved = app.executorIdToInfo.filter { case (_, exec) =>
          exec.isActive == false
      }
      execsRemoved.map { case (id, exec) =>
        Seq(app.index.toString, id, exec.removeTime.toString, exec.removeReason)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols(0).toLong, cols(1).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Removed Executors Found!\n"))
    }
  }

  //Function to list all *possible* not-supported plan nodes if GPU Mode=on
  def listPossibleUnsupportedSQLPlan(): Unit = {
    val header = "\nSQL Plan HealthCheck:\n"
    fileWriter.foreach(_.write(header))
    val outputHeaders = Seq("appIndex", "sqlID", "nodeID", "nodeName", "nodeDescription", "reason")
    val res = apps.flatMap { app =>
      app.unsupportedSQLplan.map { unsup =>
        Seq(app.index.toString, unsup.sqlID.toString, unsup.nodeID.toString, unsup.nodeName,
          unsup.nodeDesc, unsup.reason)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols(0).toLong, cols(1).toLong, cols(2).toLong))
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        outputHeaders, sortedRows)
      fileWriter.foreach(_.write(outStr))
    } else {
      fileWriter.foreach(_.write("No Unsupported SQL Ops Found!\n"))
    }
  }
}
