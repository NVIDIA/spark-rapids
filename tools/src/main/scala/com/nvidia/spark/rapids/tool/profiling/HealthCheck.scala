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
  def listFailedTasks(): Seq[FailedTaskProfileResults] = {
    val tasksMessageHeader = s"\nFailed tasks:\n"
    fileWriter.foreach(_.write(tasksMessageHeader))
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
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Failed Tasks Found!\n"))
      Seq.empty
    }
  }

  def listFailedStages(): Seq[FailedStagesProfileResults] = {
    val stagesMessageHeader = s"\nFailed stages:\n"
    fileWriter.foreach(_.write(stagesMessageHeader))
    val failed = apps.flatMap { app =>
      val stagesFailed = app.stageIdToInfo.filter { case (_, sc) =>
        sc.failureReason.nonEmpty
      }
      stagesFailed.map { case ((id, attId), sc) =>
        val failureStr = sc.failureReason.getOrElse("")
        FailedStagesProfileResults(app.index, id, attId,
          sc.info.name, sc.info.numTasks,
          ProfileUtils.truncateFailureStr(failureStr))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy(cols => (cols.appIndex, cols.stageId,
        cols.stageAttemptId))
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Failed Stages Found!\n"))
      Seq.empty
    }
  }

  def listFailedJobs(): Seq[FailedJobsProfileResults] = {
    val jobsMessageHeader = s"\nFailed jobs:\n"
    fileWriter.foreach(_.write(jobsMessageHeader))
    val failed = apps.flatMap { app =>
      val jobsFailed = app.jobIdToInfo.filter { case (_, jc) =>
        jc.jobResult.nonEmpty && !jc.jobResult.get.equals("JobSucceeded")
      }
      jobsFailed.map { case (id, jc) =>
        val failureStr = jc.failedReason.getOrElse("")
        FailedJobsProfileResults(app.index, id, jc.jobResult.getOrElse("Unknown"),
          ProfileUtils.truncateFailureStr(failureStr))
      }
    }
    if (failed.size > 0) {
      val sortedRows = failed.sortBy { cols =>
        (cols.appIndex, cols.jobId, cols.jobResult)
      }
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Failed Jobs Found!\n"))
      Seq.empty
    }
  }

  def listRemovedBlockManager(): Seq[BlockManagerRemovedProfileResult] = {
    val header = "\nRemoved BlockManager(s):\n"
    fileWriter.foreach(_.write(header))
    val res = apps.flatMap { app =>
      app.blockManagersRemoved.map { bm =>
        BlockManagerRemovedProfileResult(app.index, bm.executorID, bm.time)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols.appIndex, cols.executorID))
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Removed BlockManagers Found!\n"))
      Seq.empty
    }
  }

  def listRemovedExecutors(): Seq[ExecutorsRemovedProfileResult] = {
    val header = "\nRemoved Executors(s):\n"
    fileWriter.foreach(_.write(header))
    val res = apps.flatMap { app =>
      val execsRemoved = app.executorIdToInfo.filter { case (_, exec) =>
          exec.isActive == false
      }
      execsRemoved.map { case (id, exec) =>
        ExecutorsRemovedProfileResult(app.index, id, exec.removeTime, exec.removeReason)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols.appIndex, cols.executorID))
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Removed Executors Found!\n"))
      Seq.empty
    }
  }

  //Function to list all *possible* not-supported plan nodes if GPU Mode=on
  def listPossibleUnsupportedSQLPlan(): Seq[UnsupportedOpsProfileResult] = {
    val header = "\nUnsupported SQL Plan:\n"
    fileWriter.foreach(_.write(header))
    val res = apps.flatMap { app =>
      app.unsupportedSQLplan.map { unsup =>
        UnsupportedOpsProfileResult(app.index, unsup.sqlID, unsup.nodeID, unsup.nodeName,
          ProfileUtils.truncateFailureStr(unsup.nodeDesc), unsup.reason)
      }
    }
    if (res.size > 0) {
      val sortedRows = res.sortBy(cols => (cols.appIndex, cols.sqlID, cols.nodeID))
      val outStr = ProfileOutputWriter.makeFormattedString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Unsupported SQL Ops Found!\n"))
      Seq.empty
    }
  }
}
