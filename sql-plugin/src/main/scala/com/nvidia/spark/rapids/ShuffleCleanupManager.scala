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

package com.nvidia.spark.rapids

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil

/**
 * Manager for coordinating shuffle cleanup between driver and executors.
 *
 * When a shuffle is unregistered on the driver, this manager:
 * 1. Records the shuffle ID for cleanup
 * 2. Responds to executor polls with the list of shuffles to clean up
 * 3. Collects cleanup statistics from executors
 * 4. Aggregates statistics and posts SparkRapidsShuffleDiskSavingsEvent
 *
 * This solves the problem where shuffle data is stored in executor-side catalogs,
 * but unregisterShuffle is called on the driver. By using a polling mechanism,
 * executors can learn about shuffles that need cleanup and report their statistics.
 *
 * @param sc SparkContext for posting events
 * @param staleEntryMaxAgeMs maximum age for pending entries before they are considered stale
 * @param cleanupIntervalMs interval for running stale entry cleanup
 */
class ShuffleCleanupManager(
    sc: SparkContext,
    staleEntryMaxAgeMs: Long = 300000,  // 5 minutes
    cleanupIntervalMs: Long = 60000      // 1 minute
) extends Logging {

  /**
   * Shuffles pending cleanup. Maps shuffleId -> timestamp when unregister was called.
   */
  private val pendingCleanup = new ConcurrentHashMap[Int, Long]()

  /**
   * Aggregated statistics for each shuffle.
   * Maps shuffleId -> (totalBytesFromMemory, totalBytesFromDisk, executorCount)
   */
  private val aggregatedStats =
    new ConcurrentHashMap[Int, (Long, Long, Int)]()

  /**
   * Track which executors have reported stats for each shuffle.
   * Maps shuffleId -> Set of executorIds that have reported
   */
  private val reportedExecutors =
    new ConcurrentHashMap[Int, java.util.Set[String]]()

  /**
   * Scheduled executor for periodic stale entry cleanup.
   */
  private val cleanupExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setNameFormat("shuffle-cleanup-manager")
        .setDaemon(true)
        .build())

  // Start periodic cleanup
  cleanupExecutor.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        try {
          cleanupStaleEntries(staleEntryMaxAgeMs)
        } catch {
          case e: Exception =>
            logWarning("Error during stale entry cleanup", e)
        }
      }
    },
    cleanupIntervalMs,
    cleanupIntervalMs,
    TimeUnit.MILLISECONDS)

  /**
   * Register a shuffle for cleanup. Called from driver when unregisterShuffle is invoked.
   *
   * @param shuffleId the shuffle ID to clean up
   */
  def registerForCleanup(shuffleId: Int): Unit = {
    logInfo(s"Registering shuffle $shuffleId for cleanup")
    pendingCleanup.put(shuffleId, System.currentTimeMillis())
    reportedExecutors.put(shuffleId, ConcurrentHashMap.newKeySet[String]())
  }

  /**
   * Handle poll from executor asking for shuffles to clean up.
   *
   * @param executorId the executor polling for work
   * @return list of shuffle IDs that need cleanup
   */
  def handlePoll(executorId: String): RapidsShuffleCleanupResponseMsg = {
    val shuffleIds = new ArrayBuffer[Int]()
    val iter = pendingCleanup.keySet().iterator()
    while (iter.hasNext) {
      val shuffleId = iter.next()
      val reported = reportedExecutors.get(shuffleId)
      // Only send to executors that haven't reported yet
      if (reported != null && !reported.contains(executorId)) {
        shuffleIds += shuffleId
      }
    }
    if (shuffleIds.nonEmpty) {
      logDebug(s"Sending ${shuffleIds.size} shuffles to executor $executorId for cleanup: " +
        s"${shuffleIds.mkString(", ")}")
    }
    RapidsShuffleCleanupResponseMsg(shuffleIds.toArray)
  }

  /**
   * Handle cleanup statistics from executor.
   *
   * @param executorId the executor reporting stats
   * @param stats cleanup statistics
   */
  def handleStats(executorId: String, stats: Array[ShuffleCleanupStats]): Unit = {
    stats.foreach { stat =>
      val shuffleId = stat.shuffleId
      logDebug(s"Received cleanup stats from executor $executorId for shuffle $shuffleId: " +
        s"bytesFromMemory=${stat.bytesFromMemory}, bytesFromDisk=${stat.bytesFromDisk}")

      // Mark this executor as having reported for this shuffle
      val reported = reportedExecutors.get(shuffleId)
      if (reported != null) {
        reported.add(executorId)
      }

      // Aggregate statistics
      aggregatedStats.compute(shuffleId, (_, existing) => {
        if (existing == null) {
          (stat.bytesFromMemory, stat.bytesFromDisk, 1)
        } else {
          (existing._1 + stat.bytesFromMemory,
           existing._2 + stat.bytesFromDisk,
           existing._3 + 1)
        }
      })

      // Check if we should emit the event
      // We emit after a reasonable delay to allow all executors to report
      maybeEmitEvent(shuffleId)
    }
  }

  /**
   * Check if we should emit the event for a shuffle.
   * We use a simple heuristic: emit if at least one executor has reported.
   * The event will aggregate all stats received so far.
   */
  private def maybeEmitEvent(shuffleId: Int): Unit = {
    val stats = aggregatedStats.get(shuffleId)
    if (stats != null && (stats._1 > 0 || stats._2 > 0)) {
      // Remove from pending and aggregated to emit exactly once
      // Use containsKey + remove pattern to avoid Scala Long vs null comparison warning
      if (pendingCleanup.containsKey(shuffleId)) {
        pendingCleanup.remove(shuffleId)
        aggregatedStats.remove(shuffleId)
        reportedExecutors.remove(shuffleId)

        logInfo(s"Emitting SparkRapidsShuffleDiskSavingsEvent for shuffle $shuffleId: " +
          s"bytesFromMemory=${stats._1}, bytesFromDisk=${stats._2} " +
          s"(aggregated from ${stats._3} executor(s))")

        try {
          TrampolineUtil.postEvent(sc,
            SparkRapidsShuffleDiskSavingsEvent(shuffleId, stats._1, stats._2))
        } catch {
          case e: Exception =>
            logWarning(s"Failed to post shuffle disk savings event for shuffle $shuffleId", e)
        }
      }
    }
  }

  /**
   * Cleanup stale entries that haven't been processed.
   * Called periodically to prevent memory leaks.
   *
   * @param maxAgeMs maximum age in milliseconds before an entry is considered stale
   */
  def cleanupStaleEntries(maxAgeMs: Long = 300000): Unit = {
    val now = System.currentTimeMillis()
    val iter = pendingCleanup.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val shuffleId = entry.getKey
      val timestamp = entry.getValue
      if (now - timestamp > maxAgeMs) {
        logWarning(s"Removing stale shuffle cleanup entry for shuffle $shuffleId " +
          s"(no executor reported in ${maxAgeMs}ms)")

        // Try to emit with whatever stats we have
        val stats = aggregatedStats.remove(shuffleId)
        if (stats != null && (stats._1 > 0 || stats._2 > 0)) {
          try {
            TrampolineUtil.postEvent(sc,
              SparkRapidsShuffleDiskSavingsEvent(shuffleId, stats._1, stats._2))
          } catch {
            case e: Exception =>
              logDebug(s"Failed to post stale shuffle disk savings event", e)
          }
        }

        iter.remove()
        reportedExecutors.remove(shuffleId)
      }
    }
  }

  /**
   * Shutdown the manager and emit events for any remaining pending shuffles.
   */
  def shutdown(): Unit = {
    logInfo("Shutting down ShuffleCleanupManager")

    // Stop the cleanup executor first
    cleanupExecutor.shutdown()
    try {
      if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        cleanupExecutor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        cleanupExecutor.shutdownNow()
    }

    // Emit events for any remaining pending shuffles
    val iter = pendingCleanup.keySet().iterator()
    while (iter.hasNext) {
      val shuffleId = iter.next()
      val stats = aggregatedStats.remove(shuffleId)
      if (stats != null && (stats._1 > 0 || stats._2 > 0)) {
        logInfo(s"Shutdown: emitting event for shuffle $shuffleId with " +
          s"bytesFromMemory=${stats._1}, bytesFromDisk=${stats._2}")
        try {
          TrampolineUtil.postEvent(sc,
            SparkRapidsShuffleDiskSavingsEvent(shuffleId, stats._1, stats._2))
        } catch {
          case e: Exception =>
            logDebug(s"Failed to post shutdown shuffle disk savings event", e)
        }
      }
    }
    pendingCleanup.clear()
    aggregatedStats.clear()
    reportedExecutors.clear()
  }
}

object ShuffleCleanupManager {
  @volatile private var instance: ShuffleCleanupManager = _

  def init(sc: SparkContext): Unit = synchronized {
    if (instance == null) {
      instance = new ShuffleCleanupManager(sc)
    }
  }

  def get: ShuffleCleanupManager = instance

  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
    }
  }
}

