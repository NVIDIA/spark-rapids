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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.util.TaskCompletionListener

class NVMLMonitorSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with Logging {

  private var mockPluginContext: PluginContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create mock PluginContext
    mockPluginContext = mock(classOf[PluginContext])
    when(mockPluginContext.executorID()).thenReturn("test-executor-1")
    when(mockPluginContext.conf).thenReturn(new SparkConf())
  }

  override def afterEach(): Unit = {
    super.afterEach()
    NVMLMonitorOnExecutor.shutdown()
    StageEpochManager.shutdown()
    TrampolineUtil.unsetTaskContext()
  }

  private def createTestConf(
      enabled: Boolean,
      stageMode: Boolean,
      intervalMs: Int,
      logFrequency: Int,
      stageEpochInterval: Int = 5): RapidsConf = {

    val sparkConf = new SparkConf()
      .set("spark.rapids.monitor.nvml.enabled", enabled.toString)
      .set("spark.rapids.monitor.nvml.stageMode", stageMode.toString)
      .set("spark.rapids.monitor.nvml.intervalMs", intervalMs.toString)
      .set("spark.rapids.monitor.nvml.logFrequency", logFrequency.toString)
      .set("spark.rapids.monitor.stageEpochInterval", stageEpochInterval.toString)

    new RapidsConf(sparkConf)
  }

  // Helper class to simulate task context with completion listeners
  private class MockTaskContextWithListeners(val stageId: Int, val taskId: Long) {
    private val completionListeners = scala.collection.mutable.ListBuffer[TaskCompletionListener]()
    val mockTaskContext: TaskContext = mock(classOf[TaskContext])

    // Setup mock behavior
    when(mockTaskContext.stageId()).thenReturn(stageId)
    when(mockTaskContext.taskAttemptId()).thenReturn(taskId)

    // Mock addTaskCompletionListener to store listeners
    when(mockTaskContext.addTaskCompletionListener(any[TaskCompletionListener]()))
      .thenAnswer { invocation =>
        val listener = invocation.getArgument[TaskCompletionListener](0)
        completionListeners += listener
        mockTaskContext
      }

    // Method to trigger completion
    def triggerCompletion(): Unit = {
      // Remove task from StageEpochManager tracking
      simulateTaskCompletion(taskId)

      // Trigger any registered completion listeners
      completionListeners.foreach(_.onTaskCompletion(mockTaskContext))
    }
  }

  // Helper method to simulate a task by setting TaskContext and calling onTaskStart
  private def simulateTask(stageId: Int, taskId: Long): MockTaskContextWithListeners = {
    val taskWrapper = new MockTaskContextWithListeners(stageId, taskId)

    // Set the TaskContext first
    TrampolineUtil.setTaskContext(taskWrapper.mockTaskContext)

    // Then call StageEpochManager.onTaskStart to properly initialize task tracking
    StageEpochManager.onTaskStart()

    taskWrapper
  }


  // Direct simulation of task completion
  private def simulateTaskCompletion(taskId: Long): Unit = {
    // Remove task from StageEpochManager's internal tracking
    try {
      val stageEpochManagerClass = StageEpochManager.getClass
      val runningTasksField = stageEpochManagerClass.getDeclaredField("runningTasks")
      runningTasksField.setAccessible(true)
      val runningTasks = runningTasksField.get(StageEpochManager)
        .asInstanceOf[java.util.concurrent.ConcurrentHashMap[Long, Int]]

      // Remove task from running tasks map
      runningTasks.remove(taskId)

    } catch {
      case e: Exception =>
        System.err.println(s"Warning: Failed to simulate task completion: ${e.getMessage}")
    }
  }

  test("NVML monitor with simulated task execution and reporting") {
    // Use shorter intervals for faster testing
    val conf = createTestConf(
      enabled = true,
      stageMode = true,
      intervalMs = 200, // Faster monitoring updates
      logFrequency = 1, // Log every update for visibility
      stageEpochInterval = 1 // Stage transitions every 1 second
    )

    // Initialize NVML monitor
    NVMLMonitorOnExecutor.init(mockPluginContext, conf)

    try {
      // Simulate task execution scenario
      System.out.println("=== NVML Monitor Task Simulation Test ===")

      // Phase 1: Simulate Stage 0 with 3 tasks
      val stage0Tasks = (1L to 3L).map { taskId =>
        simulateTask(stageId = 0, taskId = taskId)
      }

      // Wait for NVML monitor to detect and switch to Stage 0
      Thread.sleep(1500) // Wait for 1.5 seconds (longer than stage epoch interval)

      // Verify Stage 0 is now being monitored
      StageEpochManager.getCurrentStage shouldBe 0
      StageEpochManager.getStageTaskCounts should contain(0 -> 3)
      // Phase 2: Complete 2 tasks from Stage 0, start 4 tasks from Stage 1
      stage0Tasks.take(2).foreach(_.triggerCompletion())

      val stage1Tasks = (1L to 4L).map { taskId =>
        simulateTask(stageId = 1, taskId = taskId + 10) // Use different task IDs
      }

      // Wait for stage transition
      Thread.sleep(1500)

      // Verify Stage 1 is now dominant
      StageEpochManager.getCurrentStage shouldBe 1
      val currentCounts = StageEpochManager.getStageTaskCounts
      currentCounts should contain(1 -> 4)
      // Phase 3: Start more tasks from Stage 2 to trigger another transition
      val stage2Tasks = (1L to 6L).map { taskId =>
        simulateTask(stageId = 2, taskId = taskId + 20) // Use different task IDs
      }

      // Wait for stage transition
      Thread.sleep(1500)

      // Verify Stage 2 is now dominant
      StageEpochManager.getCurrentStage shouldBe 2
      val finalCounts = StageEpochManager.getStageTaskCounts
      finalCounts should contain(2 -> 6)
      // Phase 4: Complete all tasks
      stage0Tasks.drop(2).foreach(_.triggerCompletion()) // Complete remaining Stage 0 task
      stage1Tasks.foreach(_.triggerCompletion()) // Complete all Stage 1 tasks
      stage2Tasks.foreach(_.triggerCompletion()) // Complete all Stage 2 tasks

      // Wait a bit more to ensure all monitoring updates are processed
      Thread.sleep(1000)

      // Verify epoch counting
      StageEpochManager.getStageEpochCount(0) should be >= 1
      StageEpochManager.getStageEpochCount(1) should be >= 1
      StageEpochManager.getStageEpochCount(2) should be >= 1

      System.out.println("Task simulation completed successfully!")
      System.out.println(s"Final epoch counts - " +
        s"Stage 0: ${StageEpochManager.getStageEpochCount(0)}, " +
        s"Stage 1: ${StageEpochManager.getStageEpochCount(1)}, " +
        s"Stage 2: ${StageEpochManager.getStageEpochCount(2)}")

      // The NVML monitor should have generated lifecycle reports for each stage transition
      // These reports are printed to logs and would be visible in a real environment

    } finally {

    }
  }

  test("NVML monitor report generation with extended monitoring") {
    // Test longer monitoring duration to capture more GPU metrics
    val conf = createTestConf(
      enabled = true,
      stageMode = true,
      intervalMs = 100, // Very fast updates to capture more data points
      logFrequency = 2, // Log every 2 updates
      stageEpochInterval = 2 // Stage transitions every 2 seconds
    )

    // Initialize NVML monitor
    NVMLMonitorOnExecutor.init(mockPluginContext, conf)

    try {
      System.out.println("=== NVML Extended Monitoring Test ===")
      logInfo("=== NVML Extended Monitoring Test ===")

      // Create a long-running stage to accumulate more monitoring data
      val heavyTasks = (1L to 8L).map { taskId =>
        simulateTask(stageId = 1, taskId = taskId)
      }

      // Wait longer to let NVML monitor collect substantial data
      Thread.sleep(3000) // 3 seconds of continuous monitoring

      // Verify we're monitoring Stage 1
      StageEpochManager.getCurrentStage shouldBe 1
      StageEpochManager.getStageTaskCounts should contain(1 -> 8)

      // Simulate a transition to another stage with fewer tasks
      heavyTasks.take(6).foreach(_.triggerCompletion()) // Complete most tasks

      val lightTasks = (1L to 3L).map { taskId =>
        simulateTask(stageId = 2, taskId = taskId + 50)
      }

      // Wait for transition
      Thread.sleep(4000)

      // Should now be monitoring Stage 2
      StageEpochManager.getCurrentStage shouldBe 2

      // Complete all tasks
      heavyTasks.drop(6).foreach(_.triggerCompletion())
      lightTasks.foreach(_.triggerCompletion())

      // Wait for final monitoring data collection
      Thread.sleep(1000)

      // Verify we collected data from multiple stages
      StageEpochManager.getStageEpochCount(1) should be >= 1
      StageEpochManager.getStageEpochCount(2) should be >= 1

      System.out.println("Extended monitoring test completed - " +
        "reports should show detailed GPU usage data")

    } finally {
      System.out.println("Expected NVML reports:")
      System.out.println("  1. Stage-1-Epoch-N report (heavy workload)")
      System.out.println("  2. Stage-2-Epoch-N report (light workload)")
      System.out.println("  3. Stage-2-Epoch-N-Final report (shutdown)")
    }
  }


  test("NVML monitor executor mode with task simulation") {
    // Test executor mode (non-stage-based) monitoring
    val conf = createTestConf(
      enabled = true,
      stageMode = false, // Executor lifecycle mode
      intervalMs = 200,
      logFrequency = 1
    )

    // Initialize NVML monitor in executor mode
    NVMLMonitorOnExecutor.init(mockPluginContext, conf)

    try {
      System.out.println("=== NVML Executor Mode Test ===")

      // In executor mode, monitoring starts immediately and doesn't depend on tasks
      // But we can still simulate task execution to create realistic GPU load

      // Simulate some tasks running (these won't affect monitoring in executor mode)
      val executorTasks = (1L to 4L).map { taskId =>
        simulateTask(stageId = 0, taskId = taskId)
      }

      // Let monitor run for a while to collect data
      Thread.sleep(1000)

      // Complete some tasks
      executorTasks.take(2).foreach(_.triggerCompletion())

      // Wait more
      Thread.sleep(1000)

      // Complete remaining tasks
      executorTasks.drop(2).foreach(_.triggerCompletion())

      System.out.println("Executor mode monitoring test completed")

    } finally {
      System.out.println("Expected report: Executor-test-executor-1")
    }
  }
}
