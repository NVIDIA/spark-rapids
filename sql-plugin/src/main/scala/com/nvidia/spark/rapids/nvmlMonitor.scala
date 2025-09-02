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

import com.nvidia.spark.rapids.jni.{GPUInfo, NVMLMonitor}

import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

/**
 * NVML GPU Monitor for tracking GPU utilization and memory usage during Spark execution.
 *
 * This class provides two monitoring modes:
 * 1. Executor lifecycle mode (default): Monitors GPU usage from executor startup to shutdown
 * 2. Stage-based mode: Similar to AsyncProfiler, monitors each stage independently and 
 * switches monitoring contexts based on which stage has the most running tasks
 *
 * The monitor uses the NVIDIA Management Library (NVML) through JNI to collect real-time
 * GPU statistics including utilization and memory usage.
 */
object NVMLMonitorOnExecutor extends Logging {

  // Default configuration constants
  private val DefaultMonitorIntervalMs = 1000
  private val DefaultLogFrequency = 5

  private var pluginCtx: PluginContext = _
  private var conf: RapidsConf = _
  private var nvmlMonitor: Option[NVMLMonitor] = None
  private var isStageMode: Boolean = false
  private var monitorIntervalMs: Int = DefaultMonitorIntervalMs
  private var logFrequency: Int = DefaultLogFrequency
  private var stageEpochInterval: Int = StageEpochManager.DefaultEpochInterval

  // Stage-based monitoring variables
  private var currentMonitoringStage = -1
  private var isShutdown = false
  private var isEpochCallbackRegistered = false

  // Monitoring callback state
  private var updateCount = 0

  def init(ctx: PluginContext, rapidsConf: RapidsConf): Unit = {
    pluginCtx = ctx
    conf = rapidsConf

    if (!conf.nvmlMonitorEnabled) {
      logInfo("NVML monitoring is disabled")
      return
    }

    isStageMode = conf.nvmlMonitorStageMode
    monitorIntervalMs = conf.nvmlMonitorIntervalMs
    logFrequency = conf.nvmlMonitorLogFrequency
    stageEpochInterval = conf.stageEpochInterval

    logInfo(s"Initializing NVML Monitor: stageMode=$isStageMode, " +
      s"intervalMs=$monitorIntervalMs, logFreq=$logFrequency")

    try {
      if (!NVMLMonitor.initialize()) {
        logError("Failed to initialize NVML")
        return
      }

      val deviceCount = NVMLMonitor.getDeviceCount
      if (deviceCount == 0) {
        logWarning("No GPUs found for NVML monitoring")
        NVMLMonitor.shutdown()
        return
      }

      logInfo(s"NVML detected $deviceCount GPU device(s)")

      // Create the monitor
      val monitor = new NVMLMonitor(monitorIntervalMs, false)

      // Set up callback
      monitor.setCallback(new NVMLMonitor.MonitoringCallback() {
        override def onGPUUpdate(gpuInfos: Array[GPUInfo], timestamp: Long): Unit = {
          updateCount += 1
          if (logFrequency > 0 && updateCount % logFrequency == 0) {
            logInfo(s"NVML Periodic Update #$updateCount:")
            gpuInfos.foreach { info =>
              logInfo(s"  ${info.toString}")
            }
          }
        }

        private def getContextDescription: String = {
          if (isStageMode && currentMonitoringStage != -1) {
            val epoch = StageEpochManager.getStageEpochCount(currentMonitoringStage) - 1
            s"for stage $currentMonitoringStage epoch $epoch"
          } else if (isStageMode) {
            "in stage mode"
          } else {
            s"for executor ${pluginCtx.executorID()}"
          }
        }

        override def onMonitoringStarted(): Unit = {
          logInfo(s"NVML monitoring started ${getContextDescription}")
        }

        override def onMonitoringStopped(): Unit = {
          logInfo(s"NVML monitoring stopped ${getContextDescription}")
        }

        override def onError(error: String): Unit = {
          logError(s"NVML monitoring error ${getContextDescription}: $error")
        }
        
      })

      nvmlMonitor = Some(monitor)
      isShutdown = false

      if (isStageMode) {
        // Register with the global StageEpochManager
        isEpochCallbackRegistered = StageEpochManager.registerCallback(
          name = "NVMLMonitor",
          callback = onStageTransition,
          epochInterval = stageEpochInterval
        )
      } else {
        // Start monitoring immediately in executor mode
        startMonitoring()
      }

    } catch {
      case e: Exception =>
        logError("Failed to initialize NVML monitor", e)
    }
  }



  def shutdown(): Unit = {
    isShutdown = true
    if (isEpochCallbackRegistered) {
      StageEpochManager.unregisterCallback("NVMLMonitor")
      isEpochCallbackRegistered = false
    }

    nvmlMonitor.foreach { monitor =>
      try {
        val reportName = if (isStageMode && currentMonitoringStage != -1) {
          val epochCount = StageEpochManager.getStageEpochCount(currentMonitoringStage)
          s"Stage-${currentMonitoringStage}-Epoch-${epochCount - 1}"
        } else if (isStageMode) {
          "StageMode-NoStages"
        } else {
          s"Executor-${pluginCtx.executorID()}"
        }
        monitor.printLifecycleReport(reportName)
        monitor.stopMonitoring()
        NVMLMonitor.shutdown()
        logInfo("NVML monitoring shutdown completed")
      } catch {
        case e: Exception =>
          logError("Error during NVML shutdown", e)
      }
    }
    
    // Clean up all variables to avoid potential memory leaks and reset state
    nvmlMonitor = None
    currentMonitoringStage = -1
    updateCount = 0
    pluginCtx = null
    conf = null
    // Reset config variables to defaults (optional but cleaner)
    isStageMode = false
    monitorIntervalMs = DefaultMonitorIntervalMs
    logFrequency = DefaultLogFrequency
    stageEpochInterval = StageEpochManager.DefaultEpochInterval
  }

  private def startMonitoring(): Unit = {
    nvmlMonitor.foreach(_.startMonitoring())
  }

  private def stopCurrentMonitoring(): Unit = {
    nvmlMonitor.foreach { monitor =>
      try {
        if (isStageMode && currentMonitoringStage != -1) {
          // Print lifecycle report for the current stage with stage ID and epoch ID
          val epochCount = StageEpochManager.getStageEpochCount(currentMonitoringStage)
          val reportName = s"Stage-${currentMonitoringStage}-Epoch-${epochCount - 1}"
          monitor.printLifecycleReport(reportName)
        }
        monitor.stopMonitoring()
      } catch {
        case e: Exception =>
          logError(s"Error stopping NVML monitoring for stage $currentMonitoringStage", e)
      }
    }
  }

  /**
   * Called when stage transition occurs from the StageEpochManager.
   */
  private def onStageTransition(oldStage: Int, newStage: Int,
      taskCount: Int, totalTasks: Int): Unit = {
    if (isShutdown) return

    logInfo(s"NVML stage epoch transition: $currentMonitoringStage -> $newStage " +
      s"(${taskCount}/${totalTasks} tasks)")

    // Stop current monitoring and print report
    if (currentMonitoringStage != -1) {
      stopCurrentMonitoring()
    }

    // Switch to new stage
    currentMonitoringStage = newStage

    // Start monitoring for new stage
    startMonitoring()
  }

}
