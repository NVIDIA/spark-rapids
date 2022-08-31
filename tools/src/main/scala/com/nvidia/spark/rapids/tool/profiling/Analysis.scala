/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.profiling._

/**
 * Does analysis on the DataFrames
 * from object of ApplicationInfo
 */
class Analysis(apps: Seq[ApplicationInfo]) {

  def getDurations(tcs: ArrayBuffer[TaskCase]): (Long, Long, Long, Double) = {
    val durations = tcs.map(_.duration)
    if (durations.size > 0 ) {
      (durations.sum, durations.max, durations.min,
        ToolUtils.calculateAverage(durations.sum, durations.size, 1))
    } else {
      (0L, 0L, 0L, 0.toDouble)
    }
  }

  private def maxWithEmptyHandling(arr: ArrayBuffer[Long]): Long = {
    if (arr.isEmpty) {
      0L
    } else {
      arr.max
    }
  }

  // Job + Stage Level TaskMetrics Aggregation
  def jobAndStageMetricsAggregation(): Seq[JobStageAggTaskMetricsProfileResult] = {
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
          val (durSum, durMax, durMin, durAvg) = getDurations(tasksInJob)
          Some(JobStageAggTaskMetricsProfileResult(app.index,
            s"job_$id",
            numTaskAttempt,
            jc.duration,
            tasksInJob.map(_.diskBytesSpilled).sum,
            durSum,
            durMax,
            durMin,
            durAvg,
            tasksInJob.map(_.executorCPUTime).sum,
            tasksInJob.map(_.executorDeserializeCPUTime).sum,
            tasksInJob.map(_.executorDeserializeTime).sum,
            tasksInJob.map(_.executorRunTime).sum,
            tasksInJob.map(_.input_bytesRead).sum,
            tasksInJob.map(_.input_recordsRead).sum,
            tasksInJob.map(_.jvmGCTime).sum,
            tasksInJob.map(_.memoryBytesSpilled).sum,
            tasksInJob.map(_.output_bytesWritten).sum,
            tasksInJob.map(_.output_recordsWritten).sum,
            maxWithEmptyHandling(tasksInJob.map(_.peakExecutionMemory)),
            tasksInJob.map(_.resultSerializationTime).sum,
            maxWithEmptyHandling(tasksInJob.map(_.resultSize)),
            tasksInJob.map(_.sr_fetchWaitTime).sum,
            tasksInJob.map(_.sr_localBlocksFetched).sum,
            tasksInJob.map(_.sr_localBytesRead).sum,
            tasksInJob.map(_.sr_remoteBlocksFetched).sum,
            tasksInJob.map(_.sr_remoteBytesRead).sum,
            tasksInJob.map(_.sr_remoteBytesReadToDisk).sum,
            tasksInJob.map(_.sr_totalBytesRead).sum,
            tasksInJob.map(_.sw_bytesWritten).sum,
            tasksInJob.map(_.sw_recordsWritten).sum,
            tasksInJob.map(_.sw_writeTime).sum
          ))
        }
      }
    }
    val allJobStageRows = apps.flatMap { app =>
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
            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
            Some(JobStageAggTaskMetricsProfileResult(app.index,
              s"stage_$id",
              numAttempts,
              sc.duration,
              tasksInStage.map(_.diskBytesSpilled).sum,
              durSum,
              durMax,
              durMin,
              durAvg,
              tasksInStage.map(_.executorCPUTime).sum,
              tasksInStage.map(_.executorDeserializeCPUTime).sum,
              tasksInStage.map(_.executorDeserializeTime).sum,
              tasksInStage.map(_.executorRunTime).sum,
              tasksInStage.map(_.input_bytesRead).sum,
              tasksInStage.map(_.input_recordsRead).sum,
              tasksInStage.map(_.jvmGCTime).sum,
              tasksInStage.map(_.memoryBytesSpilled).sum,
              tasksInStage.map(_.output_bytesWritten).sum,
              tasksInStage.map(_.output_recordsWritten).sum,
              maxWithEmptyHandling(tasksInStage.map(_.peakExecutionMemory)),
              tasksInStage.map(_.resultSerializationTime).sum,
              maxWithEmptyHandling(tasksInStage.map(_.resultSize)),
              tasksInStage.map(_.sr_fetchWaitTime).sum,
              tasksInStage.map(_.sr_localBlocksFetched).sum,
              tasksInStage.map(_.sr_localBytesRead).sum,
              tasksInStage.map(_.sr_remoteBlocksFetched).sum,
              tasksInStage.map(_.sr_remoteBytesRead).sum,
              tasksInStage.map(_.sr_remoteBytesReadToDisk).sum,
              tasksInStage.map(_.sr_totalBytesRead).sum,
              tasksInStage.map(_.sw_bytesWritten).sum,
              tasksInStage.map(_.sw_recordsWritten).sum,
              tasksInStage.map(_.sw_writeTime).sum
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
              val (durSum, durMax, durMin, durAvg) = getDurations(tasksInStage)
              Some(JobStageAggTaskMetricsProfileResult(app.index,
                s"stage_$id",
                numAttempts,
                sc.duration,
                tasksInStage.map(_.diskBytesSpilled).sum,
                durSum,
                durMax,
                durMin,
                durAvg,
                tasksInStage.map(_.executorCPUTime).sum,
                tasksInStage.map(_.executorDeserializeCPUTime).sum,
                tasksInStage.map(_.executorDeserializeTime).sum,
                tasksInStage.map(_.executorRunTime).sum,
                tasksInStage.map(_.input_bytesRead).sum,
                tasksInStage.map(_.input_recordsRead).sum,
                tasksInStage.map(_.jvmGCTime).sum,
                tasksInStage.map(_.memoryBytesSpilled).sum,
                tasksInStage.map(_.output_bytesWritten).sum,
                tasksInStage.map(_.output_recordsWritten).sum,
                maxWithEmptyHandling(tasksInStage.map(_.peakExecutionMemory)),
                tasksInStage.map(_.resultSerializationTime).sum,
                maxWithEmptyHandling(tasksInStage.map(_.resultSize)),
                tasksInStage.map(_.sr_fetchWaitTime).sum,
                tasksInStage.map(_.sr_localBlocksFetched).sum,
                tasksInStage.map(_.sr_localBytesRead).sum,
                tasksInStage.map(_.sr_remoteBlocksFetched).sum,
                tasksInStage.map(_.sr_remoteBytesRead).sum,
                tasksInStage.map(_.sr_remoteBytesReadToDisk).sum,
                tasksInStage.map(_.sr_totalBytesRead).sum,
                tasksInStage.map(_.sw_bytesWritten).sum,
                tasksInStage.map(_.sw_recordsWritten).sum,
                tasksInStage.map(_.sw_writeTime).sum
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
      sortedRows
    } else {
      Seq.empty
    }

  }

  // SQL Level TaskMetrics Aggregation(Only when SQL exists)
  def sqlMetricsAggregation(): Seq[SQLTaskAggMetricsProfileResult] = {
    val allRows = apps.flatMap { app =>
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
            // count all attempts
            val numAttempts = tasksInSQL.size

            val diskBytes = tasksInSQL.map(_.diskBytesSpilled).sum
            val execCpuTime = tasksInSQL.map(_.executorCPUTime).sum
            val execRunTime = tasksInSQL.map(_.executorRunTime).sum
            val execCPURatio = ToolUtils.calculateDurationPercent(execCpuTime, execRunTime)

            // set this here, so make sure we don't get it again until later
            sqlCase.sqlCpuTimePercent = execCPURatio
           
            val (durSum, durMax, durMin, durAvg) = getDurations(tasksInSQL)
            Some(SQLTaskAggMetricsProfileResult(app.index,
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
              tasksInSQL.map(_.executorDeserializeCPUTime).sum,
              tasksInSQL.map(_.executorDeserializeTime).sum,
              execRunTime,
              tasksInSQL.map(_.input_bytesRead).sum,
              tasksInSQL.map(_.input_bytesRead).sum * 1.0 / tasksInSQL.size,
              tasksInSQL.map(_.input_recordsRead).sum,
              tasksInSQL.map(_.jvmGCTime).sum,
              tasksInSQL.map(_.memoryBytesSpilled).sum,
              tasksInSQL.map(_.output_bytesWritten).sum,
              tasksInSQL.map(_.output_recordsWritten).sum,
              maxWithEmptyHandling(tasksInSQL.map(_.peakExecutionMemory)),
              tasksInSQL.map(_.resultSerializationTime).sum,
              maxWithEmptyHandling(tasksInSQL.map(_.resultSize)),
              tasksInSQL.map(_.sr_fetchWaitTime).sum,
              tasksInSQL.map(_.sr_localBlocksFetched).sum,
              tasksInSQL.map(_.sr_localBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBlocksFetched).sum,
              tasksInSQL.map(_.sr_remoteBytesRead).sum,
              tasksInSQL.map(_.sr_remoteBytesReadToDisk).sum,
              tasksInSQL.map(_.sr_totalBytesRead).sum,
              tasksInSQL.map(_.sw_bytesWritten).sum,
              tasksInSQL.map(_.sw_recordsWritten).sum,
              tasksInSQL.map(_.sw_writeTime).sum
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
      sortedRows
    } else {
      Seq.empty
    }
  }

  def sqlMetricsAggregationDurationAndCpuTime(): Seq[SQLDurationExecutorTimeProfileResult] = {
    val allRows = apps.flatMap { app =>
      app.sqlIdToInfo.map { case (sqlId, sqlCase) =>
        SQLDurationExecutorTimeProfileResult(app.index, app.appId, sqlId, sqlCase.duration,
          sqlCase.hasDatasetOrRDD, app.appInfo.duration, sqlCase.problematic,
          sqlCase.sqlCpuTimePercent)
      }
    }

    if (allRows.size > 0) {
      val sortedRows = allRows.sortBy { cols =>
        val sortDur = cols.duration.getOrElse(0L)
        (cols.appIndex, cols.sqlID, sortDur)
      }
      sortedRows
    } else {
      Seq.empty
    }
  }

  private case class AverageStageInfo(avgDuration: Double, avgShuffleReadBytes: Double)

  def shuffleSkewCheck(): Seq[ShuffleSkewProfileResult] = {
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
            Some(ShuffleSkewProfileResult(app.index, tc.stageId, tc.stageAttemptId,
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
      sortedRows
    } else {
      Seq.empty
    }
  }
}
