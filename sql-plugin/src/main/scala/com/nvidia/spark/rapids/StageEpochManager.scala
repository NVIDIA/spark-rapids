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

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, 
  ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{TaskContext}
import org.apache.spark.internal.Logging

/**
 * Common stage epoch management for monitoring systems that need to track
 * which stage is dominant based on running task counts.
 * 
 * This is a singleton that supports multiple monitoring systems registering
 * callbacks for stage transitions. Only one scheduler thread is used regardless
 * of how many monitoring systems are active.
 * 
 * Features:
 * 1. Task tracking across stages
 * 2. Dominant stage determination based on configurable threshold
 * 3. Epoch counting per stage
 * 4. Single scheduler for all monitoring systems
 * 5. Multiple callback registration support
 * 6. Automatic lifecycle management (start when first callback registered,
 *    stop when last callback unregistered)
 */
object StageEpochManager extends Logging {
  
  private case class MonitoringCallback(
      name: String,
      callback: (Int, Int, Int, Int) => Unit,
      epochInterval: Int)

  // Default configuration
  private val DefaultEpochInterval = 5
  private val DefaultDominantThreshold = 0.5
  
  // Task tracking for stage epoch determination
  private val runningTasks = new ConcurrentHashMap[Long, Int]() // taskId -> stageId
  
  // Epoch tracking for same stage multiple executions
  private val stageEpochCounters = new ConcurrentHashMap[Int, Int]() // stageId -> epochCount
  
  private var epochScheduler: Option[ScheduledExecutorService] = None
  private var epochTask: Option[ScheduledFuture[_]] = None
  private var isShutdown = false
  private var currentStage = -1
  
  // Multiple callback registration
  private val registeredCallbacks = new ConcurrentHashMap[String, MonitoringCallback]()
  
  // Single epoch interval (all monitoring systems use the same unified config)
  private var epochInterval = DefaultEpochInterval
  private val dominantThreshold = DefaultDominantThreshold
  
  /**
   * Registers a monitoring system for stage transition callbacks.
   * Automatically starts the epoch scheduler if this is the first registration.
   * 
   * @param name Unique name for the monitoring system
   * @param callback Function to call on stage transitions:
   *                 (oldStage, newStage, taskCount, totalTasks)
   * @param epochInterval Epoch interval in seconds (should be same for all systems)
   * @return true if registration succeeded, false if name already exists
   */
  def registerCallback(name: String, callback: (Int, Int, Int, Int) => Unit, 
      epochInterval: Int = DefaultEpochInterval): Boolean = synchronized {
    if (registeredCallbacks.containsKey(name)) {
      logWarning(s"StageEpochManager: Callback '$name' is already registered")
      return false
    }
    
    // Validate epoch interval consistency (all systems should use unified config)
    if (registeredCallbacks.size() > 0 && epochInterval != this.epochInterval) {
      logWarning(s"StageEpochManager: Callback '$name' has different epoch interval " +
        s"($epochInterval) than existing callbacks (${this.epochInterval}). " +
        s"Using existing interval.")
    } else if (registeredCallbacks.size() == 0) {
      // First registration sets the epoch interval
      this.epochInterval = epochInterval
    }
    
    val monitoringCallback = MonitoringCallback(name, callback, epochInterval)
    registeredCallbacks.put(name, monitoringCallback)
    
    // Start scheduler if this is the first callback
    if (registeredCallbacks.size() == 1) {
      start()
    }
    
    logInfo(s"StageEpochManager: Registered callback '$name' (interval: ${this.epochInterval}s)")
    true
  }
  
  /**
   * Unregisters a monitoring system.
   * Automatically stops the epoch scheduler if this was the last registration.
   * 
   * @param name Name of the monitoring system to unregister
   * @return true if unregistration succeeded, false if name not found
   */
  def unregisterCallback(name: String): Boolean = synchronized {
    val removed = Option(registeredCallbacks.remove(name))
    
    if (removed.isEmpty) {
      logWarning(s"StageEpochManager: Callback '$name' was not registered")
      return false
    }
    
    logInfo(s"StageEpochManager: Unregistered callback '$name'")
    
    // Stop scheduler if no callbacks remain
    if (registeredCallbacks.isEmpty) {
      stop()
    }
    // No need to restart - all systems use the same unified configuration
    
    true
  }
  

  
  /**
   * Starts the epoch scheduler. This is called automatically when the first
   * callback is registered.
   */
  private def start(): Unit = {
    if (epochScheduler.isEmpty) {
      val scheduler = Executors.newSingleThreadScheduledExecutor(r => {
        val thread = new Thread(r, "StageEpochManager-Scheduler")
        thread.setDaemon(true)
        thread
      })
      epochScheduler = Some(scheduler)
      
      val task = scheduler.scheduleAtFixedRate(
        () => {
          try {
            checkAndSwitchStageEpoch()
          } catch {
            case e: Exception =>
              logError(s"Error in StageEpochManager scheduler", e)
          }
        },
        epochInterval,
        epochInterval,
        TimeUnit.SECONDS
      )
      epochTask = Some(task)
      isShutdown = false
      
      logInfo(s"Started StageEpochManager with effective epoch interval ${epochInterval}s "
        + s"(${registeredCallbacks.size()} callbacks registered)")
    }
  }
  
  /**
   * Stops the epoch scheduler and cleans up resources. This is called
   * automatically when the last callback is unregistered.
   */
  private def stop(): Unit = {
    epochTask.foreach(_.cancel(false))
    epochTask = None
    
    epochScheduler.foreach { scheduler =>
      scheduler.shutdown()
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow()
        }
      } catch {
        case _: InterruptedException =>
          scheduler.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
    epochScheduler = None
    logInfo("Stopped StageEpochManager scheduler")
  }
  
  /**
   * Emergency shutdown for application cleanup. This forcefully stops
   * the scheduler and clears all callbacks.
   */
  def shutdown(): Unit = synchronized {
    isShutdown = true
    registeredCallbacks.clear()
    stop()
    runningTasks.clear()
    stageEpochCounters.clear()
    currentStage = -1
    logInfo("StageEpochManager shutdown completed")
  }
  
  /**
   * Called when a task starts. Should be called from onTaskStart() in the monitoring system.
   */
  def onTaskStart(): Unit = {
    val taskContext = TaskContext.get
    if (taskContext != null) {
      val stageId = taskContext.stageId()
      val taskId = taskContext.taskAttemptId()
      
      // Track this task
      runningTasks.put(taskId, stageId)
      
      // Add task completion listener to clean up tracking
      taskContext.addTaskCompletionListener[Unit] { _ =>
        runningTasks.remove(taskId)
      }
      
      logDebug(s"StageEpochManager: Task $taskId from stage $stageId started, " +
        s"total running tasks: ${runningTasks.size()}")
    }
  }
  
  /**
   * Gets the current epoch count for a given stage.
   */
  def getStageEpochCount(stageId: Int): Int = {
    stageEpochCounters.getOrDefault(stageId, 0)
  }
  
  /**
   * Increments and returns the new epoch count for a stage.
   */
  def incrementStageEpoch(stageId: Int): Int = {
    stageEpochCounters.compute(stageId, (_, current) => current + 1)
  }
  
  /**
   * Gets the current dominant stage, or -1 if none.
   */
  def getCurrentStage: Int = currentStage
  
  /**
   * Gets a snapshot of current running tasks per stage.
   */
  def getStageTaskCounts: Map[Int, Int] = {
    val stageTaskCounts = mutable.HashMap[Int, Int]()
    runningTasks.values().asScala.foreach { stageId =>
      stageTaskCounts(stageId) = stageTaskCounts.getOrElse(stageId, 0) + 1
    }
    stageTaskCounts.toMap
  }
  
  /**
   * Gets the total number of running tasks.
   */
  def getTotalRunningTasks: Int = runningTasks.size()
  
  /**
   * Determines which stage should be the current epoch based on running task counts
   * and triggers callback if a transition occurs. The dominant stage must have more than
   * the configured threshold of all running tasks to trigger a switch.
   */
  private def checkAndSwitchStageEpoch(): Unit = {
    if (isShutdown) {
      return
    }
    
    val stageTaskCounts = getStageTaskCounts
    
    if (stageTaskCounts.isEmpty) {
      logDebug(s"StageEpochManager: No running tasks, keeping current stage")
      return
    }
    
    val totalTasks = stageTaskCounts.values.sum
    val (dominantStage, taskCount) = stageTaskCounts.maxBy(_._2)
    val dominantRatio = taskCount.toDouble / totalTasks
    
    logDebug(s"StageEpochManager: Stage task counts: $stageTaskCounts, " +
      s"dominant stage: $dominantStage with $taskCount/$totalTasks tasks " +
      s"(${(dominantRatio * 100).toInt}%)")
    
    // Only switch if the dominant stage has more than the threshold of all running tasks
    if (dominantRatio > dominantThreshold) {
      // Switch to the dominant stage if it's different from current
      if (dominantStage != currentStage) {
        logInfo(s"StageEpochManager: Stage epoch transition: $currentStage -> $dominantStage " +
          s"(${taskCount}/${totalTasks} tasks, ${(dominantRatio * 100).toInt}%)")
        
        val oldStage = currentStage
        currentStage = dominantStage
        
        // Trigger all registered callbacks
        registeredCallbacks.values().asScala.foreach { monitoring =>
          try {
            monitoring.callback(oldStage, dominantStage, taskCount, totalTasks)
          } catch {
            case e: Exception => {
              logError(s"Error in callback '${monitoring.name}' during stage transition", e)
              println(s"Error in callback '${monitoring.name}' during stage transition: $e")
              val x = e.getStackTrace.mkString("\n")
              println(x)
            }
          }
        }
      }
    } else {
      logDebug(s"StageEpochManager: Dominant stage $dominantStage has only " +
        s"${(dominantRatio * 100).toInt}% of tasks, not switching " +
        s"(requires >${(dominantThreshold * 100).toInt}%)")
    }
  }
}
