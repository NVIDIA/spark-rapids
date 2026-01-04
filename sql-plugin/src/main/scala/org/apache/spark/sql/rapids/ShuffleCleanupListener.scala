/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.ShuffleCleanupManager

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

/**
 * A SparkListener that tracks shuffle IDs associated with each job and triggers
 * cleanup when jobs end.
 *
 * This listener solves the problem of GC-based shuffle cleanup being unreliable
 * for short-running jobs. Instead of waiting for ShuffleDependency objects to be
 * garbage collected, this listener proactively triggers cleanup when a job ends.
 *
 * The flow is:
 * 1. On JobStart: Extract shuffle IDs from stageInfos and record the mapping
 * 2. On JobEnd: Look up the shuffle IDs for this job and register them for cleanup
 * 3. ShuffleCleanupManager handles the RPC to executors for actual cleanup
 *
 * Note: This file is placed in org.apache.spark.sql.rapids package to access
 * the private[spark] shuffleDepId field in StageInfo.
 */
class ShuffleCleanupListener extends SparkListener with Logging {

  /**
   * Maps jobId to the set of shuffle IDs associated with that job.
   * This is populated on JobStart and consumed on JobEnd.
   */
  private val jobShuffles = new ConcurrentHashMap[Int, Set[Int]]()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // Extract all shuffle IDs from the stages in this job
    // StageInfo.shuffleDepId contains the shuffle ID for ShuffleMapStages
    // Note: shuffleDepId is private[spark], hence this class is in spark package
    val shuffleIds = jobStart.stageInfos
      .flatMap(_.shuffleDepId)
      .toSet

    if (shuffleIds.nonEmpty) {
      logDebug(s"Job ${jobStart.jobId} started with ${shuffleIds.size} shuffles: " +
        s"${shuffleIds.mkString(", ")}")
      jobShuffles.put(jobStart.jobId, shuffleIds)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val shuffleIds = Option(jobShuffles.remove(jobEnd.jobId))
    shuffleIds.foreach { ids =>
      logInfo(s"Job ${jobEnd.jobId} ended, triggering cleanup for ${ids.size} shuffles: " +
        s"${ids.mkString(", ")}")

      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        ids.foreach { shuffleId =>
          cleanupManager.registerForCleanup(shuffleId)
        }
      } else {
        logWarning(s"ShuffleCleanupManager not available, " +
          s"cannot trigger cleanup for shuffles: ${ids.mkString(", ")}")
      }
    }
  }

  /**
   * Called during shutdown to cleanup any remaining job -> shuffle mappings.
   * This handles the case where jobs are still running when the application ends.
   */
  def shutdown(): Unit = {
    val remainingJobs = jobShuffles.keySet().asScala.toSeq
    if (remainingJobs.nonEmpty) {
      logInfo(s"Shutdown: ${remainingJobs.size} jobs still have pending shuffle cleanup")
      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        jobShuffles.asScala.foreach { case (jobId, shuffleIds) =>
          logDebug(s"Shutdown: triggering cleanup for job $jobId shuffles: " +
            s"${shuffleIds.mkString(", ")}")
          shuffleIds.foreach(cleanupManager.registerForCleanup)
        }
      }
    }
    jobShuffles.clear()
  }
}

