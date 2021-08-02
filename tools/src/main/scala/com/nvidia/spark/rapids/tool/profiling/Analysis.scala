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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: Seq[ApplicationInfo], fileWriter: Option[ToolTextFileWriter],
    numOutputRows: Int) extends Logging {

  def getDurations(tcs: ArrayBuffer[TaskCase]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.size > 0 ) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Seq[JobStageAggTaskMetrics] = {
    val messageHeader = "\nJob + Stage level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))
    val allJobRows = apps.flatMap { app =>
      app.jobIdToInfo.map { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        val stagesInJob = app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }.keys.map(_._1).toSeq
        if (stagesInJob.isEmpty) {
          None
        } else {
          val tasksInJob = app.taskEnd.filter { tc =>
            stagesInJob.contains(tc.stageId)
          }
          // count duplicate task attempts
          val numTaskAttempt = tasksInJob.size
          // Above is a bit out of ordinary fro spark perhaps print both
          // val uniqueTasks = tasksInJob.groupBy(tc => tc.taskId)
          logWarning("count duplicates " + tasksInJob.size)

          // TODO - how to deal with attempts?
          val (durSum, durMax, durMin, durAvg) = getDurations(tasksInJob)
          Some(JobStageAggTaskMetrics(app.index,
            s"job_$id",
            numTaskAttempt,
            jc.duration,
            Option(tasksInJob.map(_.diskBytesSpilled)).getOrElse(Seq(0L)).sum,
            durSum,
            durMax,
            durMin,
            durAvg,
            Option(tasksInJob.map(_.executorCPUTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.executorDeserializeCPUTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.executorDeserializeTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.executorRunTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.gettingResultTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.input_bytesRead)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.input_recordsRead)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.jvmGCTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.memoryBytesSpilled)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.output_bytesWritten)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.output_recordsWritten)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.peakExecutionMemory)).getOrElse(Seq(0L)).max,
            Option(tasksInJob.map(_.resultSerializationTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.resultSize)).getOrElse(Seq(0L)).max,
            Option(tasksInJob.map(_.sr_fetchWaitTime)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_localBlocksFetched)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_localBytesRead)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_remoteBlocksFetched)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_remoteBytesRead)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_remoteBytesReadToDisk)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sr_totalBytesRead)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sw_bytesWritten)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sw_recordsWritten)).getOrElse(Seq(0L)).sum,
            Option(tasksInJob.map(_.sw_writeTime)).getOrElse(Seq(0L)).sum
          ))
        }
      }
    }
    val allJobStageRows = apps.flatMap { app =>
      // TODO need to get stages not in a job
      app.jobIdToInfo.flatMap { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        val stagesInJob = app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }
        if (stagesInJob.isEmpty) {
          None
        } else {
          stagesInJob.map { case ((id, said), sc) =>
            val tasksInStage = app.taskEnd.filter { tc =>
              tc.stageId == id
            }
            // count duplicate task attempts
            val numAttempts = tasksInStage.size
            // Above is a bit out of ordinary fro spark perhaps print both
            // val uniqueTasks = tasksInStage.groupBy(tc => tc.taskId)
            // TODO - how to deal with attempts?

            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
            Some(JobStageAggTaskMetrics(app.index,
              s"stage_$id",
              numAttempts,
              sc.duration,
              Option(tasksInStage.map(_.diskBytesSpilled)).getOrElse(Seq(0L)).sum,
              durSum,
              durMax,
              durMin,
              durAvg,
              Option(tasksInStage.map(_.executorCPUTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.executorDeserializeCPUTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.executorDeserializeTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.executorRunTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.gettingResultTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.input_bytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.input_recordsRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.jvmGCTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.memoryBytesSpilled)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.output_bytesWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.output_recordsWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.peakExecutionMemory)).getOrElse(Seq(0L)).max,
              Option(tasksInStage.map(_.resultSerializationTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.resultSize)).getOrElse(Seq(0L)).max,
              Option(tasksInStage.map(_.sr_fetchWaitTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_localBlocksFetched)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_localBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_remoteBlocksFetched)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_remoteBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_remoteBytesReadToDisk)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sr_totalBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sw_bytesWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sw_recordsWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInStage.map(_.sw_writeTime)).getOrElse(Seq(0L)).sum
            ))
          }
        }
      }
    }
    // stages that are missing from a job, perhaps dropped events
    val stagesWithoutJobs = apps.flatMap { app =>
      val allStageinJobs = app.jobIdToInfo.flatMap { case (id, jc) =>
        val stageIdsInJob = jc.stageIds
        app.stageIdToInfo.filterKeys { case (sid, _) =>
          stageIdsInJob.contains(sid)
        }
      }
      val missing = app.stageIdToInfo.keys.toSeq.diff(allStageinJobs.keys.toSeq)
      if (missing.isEmpty) {
        Seq.empty
      } else {
        missing.map { case ((id, saId)) =>
          val scOpt = app.stageIdToInfo.get((id, saId))
          scOpt match {
            case None =>
              None
            case Some(sc) =>
              val tasksInStage = app.taskEnd.filter { tc =>
                tc.stageId == id
              }
              // count duplicate task attempts
              val numAttempts = tasksInStage.size
              // Above is a bit out of ordinary fro spark perhaps print both
              // val uniqueTasks = tasksInStage.groupBy(tc => tc.taskId)
              // TODO - how to deal with attempts?

              val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
              Some(JobStageAggTaskMetrics(app.index,
                s"stage_$id",
                numAttempts,
                sc.duration,
                Option(tasksInStage.map(_.diskBytesSpilled)).getOrElse(Seq(0L)).sum,
                durSum,
                durMax,
                durMin,
                durAvg,
                Option(tasksInStage.map(_.executorCPUTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.executorDeserializeCPUTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.executorDeserializeTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.executorRunTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.gettingResultTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.input_bytesRead)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.input_recordsRead)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.jvmGCTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.memoryBytesSpilled)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.output_bytesWritten)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.output_recordsWritten)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.peakExecutionMemory)).getOrElse(Seq(0L)).max,
                Option(tasksInStage.map(_.resultSerializationTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.resultSize)).getOrElse(Seq(0L)).max,
                Option(tasksInStage.map(_.sr_fetchWaitTime)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_localBlocksFetched)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_localBytesRead)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_remoteBlocksFetched)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_remoteBytesRead)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_remoteBytesReadToDisk)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sr_totalBytesRead)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sw_bytesWritten)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sw_recordsWritten)).getOrElse(Seq(0L)).sum,
                Option(tasksInStage.map(_.sw_writeTime)).getOrElse(Seq(0L)).sum
              ))
          }
        }
      }
    }

    val allRows = allJobRows ++ allJobStageRows ++ stagesWithoutJobs
    val filteredRows = allRows.filter(_.isDefined).map(_.get)
    if (filteredRows.size > 0) {
      val sortedRows = filteredRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -(sortDur), cols.id)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No Job/Stage Metrics Found!\n"))
      Seq.empty
    }

  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): Seq[SQLTaskAggMetrics] = {
    val messageHeader = "\nSQL level aggregated task metrics:\n"
    fileWriter.foreach(_.write(messageHeader))

    val allRows = apps.flatMap { app =>
      // TODO - how to deal with attempts?
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        val jcs = app.jobIdToInfo.filter { case (_, jc) =>
          val jcid = jc.sqlID.getOrElse(-1)
          jc.sqlID.getOrElse(-1) == sqlId
        }
        if (jcs.isEmpty) {
          None
        } else {
          val stageIdsForSQL = jcs.flatMap(_._2.stageIds).toSeq
          val tasksInSQL = app.taskEnd.filter { tc =>
            stageIdsForSQL.contains(tc.stageId)
          }
          if (tasksInSQL.isEmpty) {
            None
          } else {
            // don't count duplicate task attempts ???
            // val uniqueTasks = tasksInSQL.groupBy(tc => tc.taskId)
            // count all attempts
            val numAttempts = tasksInSQL.size

            val diskBytes = Option(tasksInSQL.map(_.diskBytesSpilled)).getOrElse(Seq(0L)).sum
            val execCpuTime = Option(tasksInSQL.map(_.executorCPUTime)).getOrElse(Seq(0L)).sum
            val execRunTime = Option(tasksInSQL.map(_.executorRunTime)).getOrElse(Seq(0L)).sum
            val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)
            // TODO - set this here make sure we don't get it again until later
            sqlCase.sqlCpuTimePercent = execCPURatio
           
            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInSQL)
            Some(SQLTaskAggMetrics(app.index,
              app.appId,
              sqlId,
              sqlCase.description,
              numAttempts,
              sqlCase.duration,
              execCpuTime,
              execRunTime,
              execCPURatio,
              diskBytes,
              durSum,
              durMax,
              durMin,
              durAvg,
              execCpuTime,
              Option(tasksInSQL.map(_.executorDeserializeCPUTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.executorDeserializeTime)).getOrElse(Seq(0L)).sum,
              execRunTime,
              Option(tasksInSQL.map(_.gettingResultTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.input_bytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.input_recordsRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.jvmGCTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.memoryBytesSpilled)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.output_bytesWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.output_recordsWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.peakExecutionMemory)).getOrElse(Seq(0L)).max,
              Option(tasksInSQL.map(_.resultSerializationTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.resultSize)).getOrElse(Seq(0L)).max,
              Option(tasksInSQL.map(_.sr_fetchWaitTime)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_localBlocksFetched)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_localBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_remoteBlocksFetched)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_remoteBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_remoteBytesReadToDisk)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sr_totalBytesRead)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sw_bytesWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sw_recordsWritten)).getOrElse(Seq(0L)).sum,
              Option(tasksInSQL.map(_.sw_writeTime)).getOrElse(Seq(0L)).sum
            ))
          }
        }
      }
    }
    val allFiltered = allRows.filter(_.isDefined).map(_.get)
    if (allFiltered.size > 0) {
      val sortedRows = allFiltered.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, -(sortDur), cols.sqlId, cols.executorCpuTime)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No SQL Metrics Found!\n"))
      Seq.empty
    }
  }

  def sqlMetricsAggregationDurationAndCpuTime(): Seq[SQLDurationExecutorTime] = {
    val messageHeader = "\nSQL Duration and Executor CPU Time Percent\n"
    fileWriter.foreach(_.write(messageHeader))

    val allRows = apps.flatMap { app =>
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        // Potential problems not properly track, add it later
        SQLDurationExecutorTime(app.index, app.appId, sqlId, sqlCase.duration,
          sqlCase.hasDataset, app.appInfo.duration, sqlCase.problematic,
          sqlCase.sqlCpuTimePercent)
      }
    }

    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, cols.sqlID, sortDur)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No SQL Duration and Executor CPU Time Percent Found!\n"))
      Seq.empty
    }
  }

  private case class AverageStageInfo(avgDuration: Double, avgShuffleReadBytes: Double)

  def shuffleSkewCheck(): Seq[ShuffleSkewInfo] = {
    val messageHeader = s"\nShuffle Skew Check:" +
      " (When task's Shuffle Read Size > 3 * Avg Stage-level size)\n"
    fileWriter.foreach(_.write(messageHeader))

    // TODO - how expensive on large number tasks?
    val allRows = apps.flatMap { app =>
      val tasksPerStageAttempt = app.taskEnd.groupBy { tc =>
        (tc.stageId, tc.stageAttemptId)
      }
      val avgsStageInfos = tasksPerStageAttempt.map { case ((sId, saId), tcArr) =>
        val sumDuration = tcArr.map(_.duration).sum
        val avgDuration = ToolUtils.calculateAverage(sumDuration, tcArr.size, 2)
        val sumShuffleReadBytes = tcArr.map(_.sr_totalBytesRead).sum
        val avgShuffleReadBytes = ToolUtils.calculateAverage(sumShuffleReadBytes, tcArr.size, 2)
        ((sId, saId), AverageStageInfo(avgDuration, avgShuffleReadBytes))
      }

      val tasksWithSkew = app.taskEnd.filter { tc =>
        val avgShuffleDur = avgsStageInfos.get((tc.stageId, tc.stageAttemptId))
        avgShuffleDur match {
          case Some(avg) =>
            (tc.sr_totalBytesRead > 3 * avg.avgShuffleReadBytes) &&
              (tc.sr_totalBytesRead > 100 * 1024 * 1024)
          case None => false
        }
      }

      val groupedTasks = tasksWithSkew.groupBy { tc =>
        (tc.stageId, tc.stageAttemptId)
      }

      tasksWithSkew.map { tc =>
        val avgShuffleDur = avgsStageInfos.get((tc.stageId, tc.stageAttemptId))
        avgShuffleDur match {
          case Some(avg) =>
            Some(ShuffleSkewInfo(app.index, tc.stageId, tc.stageAttemptId,
              tc.taskId, tc.attempt, tc.duration, avg.avgDuration, tc.sr_totalBytesRead,
              avg.avgShuffleReadBytes, tc.peakExecutionMemory, tc.successful, tc.endReason))
          case None =>
            None
        }
      }
    }

    val allNonEmptyRows = allRows.filter(_.isDefined).map(_.get)
    if (allNonEmptyRows.size > 0) {
      val sortedRows = allNonEmptyRows.sortBy { cols =>
        (cols.appIndex, cols.stageId, cols.stageAttemptId, cols.taskId, cols.taskAttemptId)
      }
      val outStr = ProfileOutputWriter.showString(numOutputRows, 0,
        sortedRows.head.outputHeaders, sortedRows.map(_.convertToSeq))
      fileWriter.foreach(_.write(outStr))
      sortedRows
    } else {
      fileWriter.foreach(_.write("No SQL Duration and Executor CPU Time Percent Found!\n"))
      Seq.empty
    }
  }
}
