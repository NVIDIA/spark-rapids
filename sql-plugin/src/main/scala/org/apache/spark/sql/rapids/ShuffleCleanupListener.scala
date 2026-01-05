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
import scala.collection.mutable

import com.nvidia.spark.rapids.ShuffleCleanupManager

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart,
  SparkListenerStageCompleted}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd,
  SparkListenerSQLExecutionStart}

/**
 * A SparkListener that triggers shuffle cleanup when a SQL query completes,
 * with support for early cleanup of shuffles that are guaranteed not to be reused.
 *
 * == Early Cleanup Strategy ==
 *
 * This listener analyzes the query plan to identify shuffles that can be cleaned up
 * early (before query end). A shuffle can be cleaned up early if:
 *
 * 1. Its Exchange.canonicalized is unique in the entire plan (no ReuseExchange possible)
 * 2. All its consumer stages have completed
 *
 * For shuffles that may be reused (duplicate canonicalized or ReusedExchange), we wait
 * until query end to clean them up.
 *
 * == Plan Analysis ==
 *
 * On SQLExecutionStart, we get the real SparkPlan via SQLExecution.getQueryExecution()
 * and analyze it using Exchange.canonicalized.sameResult() - the same mechanism Spark
 * uses for Exchange reuse decisions. For AQE plans, we analyze the initialPlan which
 * contains the full plan before dynamic optimization.
 *
 * == Safety ==
 *
 * This is a conservative approach:
 * - We only early-cleanup shuffles that are GUARANTEED not to be reused
 * - If any two Exchanges have the same canonicalized form, all shuffles are deferred
 * - In doubt, we defer to query-end cleanup
 *
 * Note: This file is placed in org.apache.spark.sql.rapids package to access
 * the private[spark] shuffleDepId field in StageInfo.
 */
class ShuffleCleanupListener extends SparkListener with Logging {

  // ========== Execution-level tracking ==========

  /**
   * Maps SQL execution ID to the set of shuffle IDs that must wait for query end.
   * These are shuffles that may be reused or couldn't be analyzed for early cleanup.
   */
  private val deferredShuffles = new ConcurrentHashMap[Long, mutable.Set[Int]]()

  /**
   * Maps SQL execution ID to all shuffle IDs in that execution (for tracking).
   */
  private val executionShuffles = new ConcurrentHashMap[Long, mutable.Set[Int]]()

  /**
   * Executions that have ReusedExchange or potential reuse in the plan.
   * All shuffles in these executions are deferred to query end.
   *
   * We use Exchange.canonicalized.sameResult() to detect potential reuse,
   * which is the same mechanism Spark uses for Exchange reuse decisions.
   * If any two Exchanges have the same canonicalized form, all shuffles
   * in the execution are deferred to query end.
   */
  private val executionsWithReuse = ConcurrentHashMap.newKeySet[Long]()

  // ========== Shuffle reuse analysis ==========

  /**
   * Shuffles that are safe for early cleanup (no reuse possible).
   * Maps shuffle ID -> consumer stage ID (only set for single-consumer shuffles).
   */
  private val earlyCleanupCandidates = new ConcurrentHashMap[Int, Int]()

  /**
   * Shuffles that may be reused (based on plan analysis).
   * These are never cleaned up early.
   */
  private val mayReuseShuffles = ConcurrentHashMap.newKeySet[Int]()

  // ========== Stage-shuffle relationship tracking ==========

  /**
   * Maps shuffle ID -> set of consumer stage IDs.
   * Updated on each JobStart as we discover new consumer relationships.
   */
  private val shuffleConsumers = new ConcurrentHashMap[Int, mutable.Set[Int]]()

  /**
   * Maps stage ID -> shuffle ID it produces.
   */
  private val stageProducedShuffle = new ConcurrentHashMap[Int, Int]()

  /**
   * Maps shuffle ID -> SQL execution ID.
   */
  private val shuffleToExecution = new ConcurrentHashMap[Int, Long]()

  /**
   * Completed stages (stage ID -> completion time).
   */
  private val completedStages = new ConcurrentHashMap[Int, Long]()

  /**
   * Shuffles that have already been cleaned up.
   */
  private val cleanedUpShuffles = ConcurrentHashMap.newKeySet[Int]()

  // ========== Plan analysis on SQL execution start ==========

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart =>
      onSQLExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd =>
      onSQLExecutionEnd(e)
    case _ => // ignore other events
  }

  private def onSQLExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    val executionId = event.executionId

    // Initialize tracking sets
    deferredShuffles.put(executionId, mutable.Set[Int]())
    executionShuffles.put(executionId, mutable.Set[Int]())

    // Get the real QueryExecution for accurate plan analysis
    val queryExecution = SQLExecution.getQueryExecution(executionId)
    if (queryExecution != null) {
      // Use the real SparkPlan for accurate canonicalized analysis
      analyzeRealPlanForReuse(executionId, queryExecution.executedPlan)
    } else {
      // QueryExecution not available - this should not happen normally
      // as Spark puts it in the map before posting the event.
      // Be conservative and defer all shuffles to query end.
      logWarning(s"Execution $executionId: QueryExecution not available, " +
        s"all shuffles will be cleaned up at query end")
      executionsWithReuse.add(executionId)
    }
  }

  /**
   * Analyze the real SparkPlan for Exchange reuse using accurate canonicalized comparison.
   * This is the preferred method as it uses the same mechanism Spark uses for reuse detection.
   */
  private def analyzeRealPlanForReuse(executionId: Long, executedPlan: SparkPlan): Unit = {
    // For AQE plans, get the initialPlan which contains the full plan before optimization
    val targetPlan = executedPlan match {
      case aqe: AdaptiveSparkPlanExec =>
        aqe.initialPlan
      case other => other
    }

    val isAQE = executedPlan.isInstanceOf[AdaptiveSparkPlanExec]

    // Collect all Exchange nodes (excluding ReusedExchangeExec)
    val exchanges = collectExchangeNodes(targetPlan)

    // Check for static ReusedExchange first
    val hasReusedExchange = containsReusedExchange(targetPlan)
    if (hasReusedExchange) {
      logInfo(s"Execution $executionId: has ReusedExchange nodes in plan, " +
        s"all shuffles will be cleaned up at query end")
      executionsWithReuse.add(executionId)
      return
    }

    if (exchanges.size <= 1) {
      // Single or no exchange - safe for early cleanup
      logInfo(s"Execution $executionId: ${if (isAQE) "AQE" else "non-AQE"} query " +
        s"with ${exchanges.size} exchange(s), early cleanup enabled")
      return
    }

    // Multiple exchanges - check for duplicate canonicalized forms
    // This is how Spark determines Exchange reuse
    // Use sameResult() which compares canonicalized forms
    val canonicalizedList = exchanges.map(_.canonicalized)
    var hasDuplicateCanonicalized = false

    for (i <- canonicalizedList.indices if !hasDuplicateCanonicalized) {
      for (j <- (i + 1) until canonicalizedList.size if !hasDuplicateCanonicalized) {
        if (canonicalizedList(i).sameResult(canonicalizedList(j))) {
          hasDuplicateCanonicalized = true
        }
      }
    }

    val uniqueCount = if (hasDuplicateCanonicalized) {
      // Calculate actual unique count for logging
      canonicalizedList.distinct.size
    } else {
      exchanges.size
    }

    if (hasDuplicateCanonicalized) {
      logInfo(s"Execution $executionId: detected ${exchanges.size} Exchanges with " +
        s"$uniqueCount unique canonicalized forms, " +
        s"potential reuse detected, all shuffles will be cleaned up at query end")
      executionsWithReuse.add(executionId)
    } else {
      logInfo(s"Execution $executionId: ${if (isAQE) "AQE" else "non-AQE"} query " +
        s"with ${exchanges.size} unique exchanges, early cleanup enabled")
    }
  }

  /**
   * Collect all Exchange nodes from the plan (excluding ReusedExchangeExec).
   */
  private def collectExchangeNodes(plan: SparkPlan): Seq[Exchange] = {
    val exchanges = mutable.ArrayBuffer[Exchange]()

    def traverse(node: SparkPlan): Unit = {
      node match {
        case e: Exchange =>
          exchanges += e
        case _: ReusedExchangeExec =>
          // Skip ReusedExchangeExec - it references an existing Exchange
          ()
        case _ => ()
      }
      node.children.foreach(traverse)
      // Also traverse subqueries
      node.subqueries.foreach(traverse)
    }

    traverse(plan)
    exchanges
  }

  /**
   * Check if the plan contains any ReusedExchangeExec nodes.
   */
  private def containsReusedExchange(plan: SparkPlan): Boolean = {
    var found = false

    def traverse(node: SparkPlan): Unit = {
      if (!found) {
        node match {
          case _: ReusedExchangeExec =>
            found = true
          case _ =>
            node.children.foreach(traverse)
            if (!found) {
              node.subqueries.foreach(traverse)
            }
        }
      }
    }

    traverse(plan)
    found
  }

  // ========== Job start: build shuffle-stage relationships ==========

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdOpt = Option(jobStart.properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(s => scala.util.Try(s.toLong).toOption)

    executionIdOpt.foreach { executionId =>
      processJobStart(executionId, jobStart)
    }
  }

  private def processJobStart(executionId: Long, jobStart: SparkListenerJobStart): Unit = {
    val stageInfos = jobStart.stageInfos
    val hasReuse = executionsWithReuse.contains(executionId)

    // Build stage -> produced shuffle mapping
    val localStageProducedShuffle = mutable.Map[Int, Int]()
    stageInfos.foreach { stageInfo =>
      stageInfo.shuffleDepId.foreach { shuffleId =>
        localStageProducedShuffle(stageInfo.stageId) = shuffleId
        stageProducedShuffle.put(stageInfo.stageId, shuffleId)
        shuffleToExecution.put(shuffleId, executionId)

        // Track all shuffles for this execution
        executionShuffles.compute(executionId, (_, existing) => {
          val set = if (existing == null) mutable.Set[Int]() else existing
          set += shuffleId
          set
        })

        // If this execution has ReusedExchange, all shuffles are deferred
        if (hasReuse) {
          deferredShuffles.compute(executionId, (_, set) => {
            if (set != null) set += shuffleId
            set
          })
        }
      }
    }

    // Skip early cleanup analysis if execution has ReusedExchange
    if (hasReuse) {
      logDebug(s"Job ${jobStart.jobId} (execution $executionId): " +
        s"${localStageProducedShuffle.size} shuffles, all deferred due to ReusedExchange")
      return
    }

    // Build shuffle -> consumer stages mapping
    stageInfos.foreach { stageInfo =>
      stageInfo.parentIds.foreach { parentStageId =>
        localStageProducedShuffle.get(parentStageId).foreach { shuffleId =>
          val isNewConsumer = addConsumer(shuffleId, stageInfo.stageId)

          if (isNewConsumer) {
            // Check if this shuffle now has multiple consumers
            val consumers = shuffleConsumers.get(shuffleId)
            if (consumers != null && consumers.size > 1) {
              // Multiple consumers - cannot early cleanup
              markAsCannotEarlyCleanup(shuffleId, executionId,
                s"has ${consumers.size} consumer stages")
            }
          }
        }
      }
    }

    // Categorize new shuffles
    localStageProducedShuffle.values.foreach { shuffleId =>
      if (!mayReuseShuffles.contains(shuffleId) && !cleanedUpShuffles.contains(shuffleId)) {
        val consumers = shuffleConsumers.get(shuffleId)
        if (consumers == null || consumers.isEmpty) {
          // No consumer found - might be consumed by a later job, defer to query end
          logDebug(s"Shuffle $shuffleId has no visible consumers, deferring to query end")
          deferredShuffles.compute(executionId, (_, set) => {
            if (set != null) set += shuffleId
            set
          })
        } else if (consumers.size == 1) {
          // Single consumer - candidate for early cleanup
          val consumerStageId = consumers.head
          earlyCleanupCandidates.put(shuffleId, consumerStageId)
          logDebug(s"Shuffle $shuffleId is candidate for early cleanup " +
            s"after stage $consumerStageId completes")
        }
        // Multiple consumers already handled above
      }
    }

    logInfo(s"Job ${jobStart.jobId} (execution $executionId): " +
      s"${localStageProducedShuffle.size} shuffles, " +
      s"${earlyCleanupCandidates.size()} early cleanup candidates")
  }

  /**
   * Add a consumer stage for a shuffle.
   * @return true if this is a new consumer (not seen before)
   */
  private def addConsumer(shuffleId: Int, consumerStageId: Int): Boolean = {
    var isNew = false
    shuffleConsumers.compute(shuffleId, (_, existing) => {
      val set = if (existing == null) mutable.Set[Int]() else existing
      if (!set.contains(consumerStageId)) {
        set += consumerStageId
        isNew = true
      }
      set
    })
    isNew
  }

  /**
   * Mark a shuffle as cannot be cleaned up early.
   */
  private def markAsCannotEarlyCleanup(shuffleId: Int, executionId: Long, reason: String): Unit = {
    logDebug(s"Shuffle $shuffleId cannot early cleanup: $reason")
    mayReuseShuffles.add(shuffleId)
    earlyCleanupCandidates.remove(shuffleId)
    deferredShuffles.compute(executionId, (_, set) => {
      if (set != null) set += shuffleId
      set
    })
  }

  // ========== Stage completed: trigger early cleanup ==========

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stageInfo = event.stageInfo
    val stageId = stageInfo.stageId

    // Only process successful stage completions
    if (stageInfo.failureReason.isDefined) {
      logDebug(s"Stage $stageId failed, skipping early cleanup check")
      return
    }

    completedStages.put(stageId, System.currentTimeMillis())

    // Check if any shuffle can be cleaned up now
    checkAndTriggerEarlyCleanup(stageId)
  }

  private def checkAndTriggerEarlyCleanup(completedStageId: Int): Unit = {
    val toCleanup = mutable.ArrayBuffer[Int]()

    // Find shuffles whose single consumer is this stage
    val iter = earlyCleanupCandidates.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val shuffleId = entry.getKey
      val consumerStageId = entry.getValue

      if (consumerStageId == completedStageId) {
        // Verify shuffle is still valid for early cleanup
        if (!mayReuseShuffles.contains(shuffleId) && !cleanedUpShuffles.contains(shuffleId)) {
          logInfo(s"Stage $completedStageId completed, " +
            s"triggering early cleanup for shuffle $shuffleId")
          toCleanup += shuffleId
          cleanedUpShuffles.add(shuffleId)
          iter.remove()

          Option(shuffleToExecution.get(shuffleId)).foreach { executionId =>
            deferredShuffles.compute(executionId, (_, set) => {
              if (set != null) set -= shuffleId
              set
            })
          }
        }
      }
    }

    // Trigger cleanup for identified shuffles
    if (toCleanup.nonEmpty) {
      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        toCleanup.foreach { shuffleId =>
          cleanupManager.registerForCleanup(shuffleId)
        }
      }
    }
  }

  // ========== SQL execution end: cleanup remaining shuffles ==========

  private def onSQLExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    val executionId = event.executionId

    // Collect all shuffles that need cleanup at query end
    val deferred = Option(deferredShuffles.remove(executionId)).getOrElse(mutable.Set[Int]())
    val allShuffles = Option(executionShuffles.remove(executionId)).getOrElse(mutable.Set[Int]())

    // Also check for any remaining early cleanup candidates
    val remaining = mutable.ArrayBuffer[Int]()
    val iter = shuffleToExecution.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (entry.getValue == executionId && !cleanedUpShuffles.contains(entry.getKey)) {
        remaining += entry.getKey
        iter.remove()
      }
    }

    // Calculate what actually needs cleanup (not already cleaned)
    val needsCleanup = (deferred ++ remaining).filterNot(cleanedUpShuffles.contains)

    if (needsCleanup.nonEmpty) {
      // Count early vs deferred for logging
      val earlyCleanedCount = allShuffles.count(cleanedUpShuffles.contains)
      logInfo(s"SQL execution $executionId ended: " +
        s"$earlyCleanedCount shuffles cleaned early, " +
        s"${needsCleanup.size} shuffles cleaned at query end")

      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        needsCleanup.foreach { shuffleId =>
          cleanupManager.registerForCleanup(shuffleId)
          cleanedUpShuffles.add(shuffleId)
        }
      }
    }

    // Cleanup tracking data for this execution
    cleanupTrackingData(executionId, allShuffles)
  }

  private def cleanupTrackingData(executionId: Long, shuffleIds: mutable.Set[Int]): Unit = {
    executionsWithReuse.remove(executionId)
    shuffleIds.foreach { shuffleId =>
      shuffleConsumers.remove(shuffleId)
      earlyCleanupCandidates.remove(shuffleId)
      mayReuseShuffles.remove(shuffleId)
      shuffleToExecution.remove(shuffleId)
      // Keep cleanedUpShuffles for a while to handle late arrivals
      // It will be cleaned up eventually on shutdown
    }
  }

  /**
   * Called during shutdown to cleanup any remaining execution -> shuffle mappings.
   */
  def shutdown(): Unit = {
    val remainingExecutions = executionShuffles.keySet().asScala.toSeq
    if (remainingExecutions.nonEmpty) {
      logInfo(s"Shutdown: ${remainingExecutions.size} executions still have " +
        s"pending shuffle cleanup")
      val cleanupManager = ShuffleCleanupManager.get
      if (cleanupManager != null) {
        executionShuffles.asScala.foreach { case (executionId, shuffleIds) =>
          val needsCleanup = shuffleIds.filterNot(cleanedUpShuffles.contains)
          if (needsCleanup.nonEmpty) {
            logDebug(s"Shutdown: triggering cleanup for execution $executionId shuffles: " +
              s"${needsCleanup.mkString(", ")}")
            needsCleanup.foreach(cleanupManager.registerForCleanup)
          }
        }
      }
    }

    // Clear all tracking data
    deferredShuffles.clear()
    executionShuffles.clear()
    executionsWithReuse.clear()
    earlyCleanupCandidates.clear()
    mayReuseShuffles.clear()
    shuffleConsumers.clear()
    stageProducedShuffle.clear()
    shuffleToExecution.clear()
    completedStages.clear()
    cleanedUpShuffles.clear()
  }

  // ========== Statistics for monitoring ==========

  /**
   * Get statistics about early cleanup performance.
   * Useful for monitoring and debugging.
   */
  def getStats: ShuffleCleanupStats = {
    ShuffleCleanupStats(
      earlyCleanupCandidates = earlyCleanupCandidates.size(),
      mayReuseShuffles = mayReuseShuffles.size(),
      cleanedUpShuffles = cleanedUpShuffles.size(),
      pendingExecutions = executionShuffles.size()
    )
  }
}

/**
 * Statistics about shuffle cleanup.
 */
case class ShuffleCleanupStats(
    earlyCleanupCandidates: Int,
    mayReuseShuffles: Int,
    cleanedUpShuffles: Int,
    pendingExecutions: Int)
