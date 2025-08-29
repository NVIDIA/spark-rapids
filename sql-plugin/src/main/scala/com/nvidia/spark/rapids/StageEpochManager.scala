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
 * This class provides:
 * 1. Task tracking across stages
 * 2. Dominant stage determination based on configurable threshold
 * 3. Epoch counting per stage
 * 4. Scheduler management for periodic epoch switching
 * 5. Callback mechanism for stage transitions
 * 
 * Used by both AsyncProfilerOnExecutor and NVMLMonitorOnExecutor.
 */
class StageEpochManager(
    name: String,
    epochInterval: Int,
    dominantThreshold: Double = 0.5) extends Logging {
  
  // Task tracking for stage epoch determination
  private val runningTasks = new ConcurrentHashMap[Long, Int]() // taskId -> stageId
  
  // Epoch tracking for same stage multiple executions
  private val stageEpochCounters = new ConcurrentHashMap[Int, Int]() // stageId -> epochCount
  
  private var epochScheduler: Option[ScheduledExecutorService] = None
  private var epochTask: Option[ScheduledFuture[_]] = None
  private var isShutdown = false
  private var currentStage = -1
  
  // Callback for stage transitions
  private var stageTransitionCallback: Option[(Int, Int, Int, Int) => Unit] = None
  
  /**
   * Sets the callback function to be called when stage transition occurs.
   * Parameters: (oldStage, newStage, taskCount, totalTasks)
   */
  def setStageTransitionCallback(callback: (Int, Int, Int, Int) => Unit): Unit = {
    stageTransitionCallback = Some(callback)
  }
  
  /**
   * Starts the epoch scheduler that periodically determines the current stage epoch
   * based on running task counts.
   */
  def start(): Unit = {
    if (epochScheduler.isEmpty && epochInterval > 0) {
      val scheduler = Executors.newSingleThreadScheduledExecutor(r => {
        val thread = new Thread(r, s"$name-EpochScheduler")
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
              logError(s"Error in $name epoch scheduler", e)
          }
        },
        epochInterval, // initial delay
        epochInterval, // period
        TimeUnit.SECONDS
      )
      epochTask = Some(task)
      
      logInfo(s"Started $name stage epoch scheduler with interval ${epochInterval}s")
    }
  }
  
  /**
   * Stops the epoch scheduler and cleans up resources.
   */
  def stop(): Unit = {
    isShutdown = true
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
      
      logDebug(s"$name: Task $taskId from stage $stageId started, " +
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
      logDebug(s"$name: No running tasks, keeping current stage")
      return
    }
    
    val totalTasks = stageTaskCounts.values.sum
    val (dominantStage, taskCount) = stageTaskCounts.maxBy(_._2)
    val dominantRatio = taskCount.toDouble / totalTasks
    
    logDebug(s"$name: Stage task counts: $stageTaskCounts, " +
      s"dominant stage: $dominantStage with $taskCount/$totalTasks tasks " +
      s"(${(dominantRatio * 100).toInt}%)")
    
    // Only switch if the dominant stage has more than the threshold of all running tasks
    if (dominantRatio > dominantThreshold) {
      // Switch to the dominant stage if it's different from current
      if (dominantStage != currentStage) {
        logInfo(s"$name: Stage epoch transition: $currentStage -> $dominantStage " +
          s"(${taskCount}/${totalTasks} tasks, ${(dominantRatio * 100).toInt}%)")
        
        val oldStage = currentStage
        currentStage = dominantStage
        
        // Trigger callback if set
        stageTransitionCallback.foreach(_(oldStage, dominantStage, taskCount, totalTasks))
      }
    } else {
      logDebug(s"$name: Dominant stage $dominantStage has only " +
        s"${(dominantRatio * 100).toInt}% of tasks, not switching " +
        s"(requires >${(dominantThreshold * 100).toInt}%)")
    }
  }
}
