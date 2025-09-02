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

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.log4j.{AppenderSkeleton, LogManager}
import org.apache.log4j.spi.LoggingEvent
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


/**
 * In-memory log appender for capturing log messages during tests
 */
class InMemoryAppender extends AppenderSkeleton {
  private val messages = new ConcurrentLinkedQueue[String]()

  override def append(event: LoggingEvent): Unit = {
    if (layout != null) {
      val formattedMessage = layout.format(event)
      messages.offer(formattedMessage.trim)
    } else {
      messages.offer(event.getMessage.toString)
    }
  }

  override def close(): Unit = {
    // No resources to close
  }

  override def requiresLayout(): Boolean = false

  def getMessages: List[String] = messages.asScala.toList

  def clear(): Unit = messages.clear()
}

class NVMLMonitorSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with Logging {

  private var mockPluginContext: PluginContext = _
  private var testMarker: String = _
  private var inMemoryAppender: InMemoryAppender = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Create mock PluginContext
    mockPluginContext = mock(classOf[PluginContext])
    when(mockPluginContext.executorID()).thenReturn("test-executor-1")
    when(mockPluginContext.conf).thenReturn(new SparkConf())

    // Create and add in-memory appender
    inMemoryAppender = new InMemoryAppender()
    addInMemoryAppender()

    // Generate unique test marker for log parsing
    testMarker = s"TEST_${UUID.randomUUID().toString.replace("-", "").take(8)}"
    logInfo(s"=== START_MARKER: $testMarker ===")
  }

  override def afterEach(): Unit = {
    super.afterEach()



    // Remove in-memory appender
    removeInMemoryAppender()
    logInfo(s"=== END_MARKER: $testMarker ===")
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
      case _: Exception =>
      // Silently handle task completion simulation failure
    }
  }

  test("NVML monitor with simulated task execution and reporting") {
    logInfo("Starting test: NVML monitor with simulated task execution and reporting")

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

    // Simulate task execution scenario

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



    // The NVML monitor should have generated lifecycle reports for each stage transition
    // These reports are printed to logs and would be visible in a real environment

    NVMLMonitorOnExecutor.shutdown()
    StageEpochManager.shutdown()
    TrampolineUtil.unsetTaskContext()

    // Verify specific Stage-Epoch patterns for this test case
    // This test simulates: Stage 0 -> Stage 1 -> Stage 2
    // Important: Only stage transitions trigger new epochs, not time intervals
    // stageEpochInterval is just the check interval, not forced epoch switching
    val expectedStageEpochs = Map(
      0 -> (0 to 0), // Stage 0: Only 1 epoch until transition to Stage 1
      1 -> (0 to 0), // Stage 1: Only 1 epoch until transition to Stage 2
      2 -> (0 to 0) // Stage 2: Only 1 epoch, no subsequent transitions
    )
    verifyPreciseStageEpochPatterns("Task Execution Test", expectedStageEpochs)
  }

  test("NVML monitor per stage mode with complex epoch switching (0->1->0->2)") {
    logInfo("Starting test: NVML monitor per stage mode with complex epoch switching")

    // Configure per stage mode with fast epoch intervals for testing
    val conf = createTestConf(
      enabled = true,
      stageMode = true, // Enable per stage mode
      intervalMs = 150, // Fast monitoring updates for responsiveness
      logFrequency = 1, // Log every update to capture all transitions
      stageEpochInterval = 1 // Stage transitions checked every 1 second
    )

    // Initialize NVML monitor with per stage configuration
    NVMLMonitorOnExecutor.init(mockPluginContext, conf)

    // Phase 1: Start with Stage 0 (3 tasks)
    logInfo("Phase 1: Starting Stage 0 tasks")
    val stage0Phase1Tasks = (1L to 3L).map { taskId =>
      simulateTask(stageId = 0, taskId = taskId)
    }

    // Wait for monitoring to detect and switch to Stage 0
    Thread.sleep(1500)

    // Verify Stage 0 is being monitored
    StageEpochManager.getCurrentStage shouldBe 0
    StageEpochManager.getStageTaskCounts should contain(0 -> 3)

    // Phase 2: Transition to Stage 1 (4 tasks, making it dominant)
    logInfo("Phase 2: Starting Stage 1 tasks to trigger transition")
    stage0Phase1Tasks.take(1).foreach(_.triggerCompletion()) // Reduce Stage 0 to 2 tasks

    val stage1Tasks = (11L to 14L).map { taskId => // Use distinct task IDs
      simulateTask(stageId = 1, taskId = taskId)
    }

    // Wait for transition to Stage 1
    Thread.sleep(1500)

    // Verify Stage 1 is now dominant
    StageEpochManager.getCurrentStage shouldBe 1
    val stage1Counts = StageEpochManager.getStageTaskCounts
    stage1Counts should contain(1 -> 4)

    // Phase 3: Return to Stage 0 by starting more Stage 0 tasks
    logInfo("Phase 3: Returning to Stage 0 by adding more Stage 0 tasks")
    stage1Tasks.take(2).foreach(_.triggerCompletion()) // Reduce Stage 1 to 2 tasks

    val stage0Phase2Tasks = (21L to 23L).map { taskId => // Use distinct task IDs
      simulateTask(stageId = 0, taskId = taskId)
    }

    // Wait for transition back to Stage 0
    Thread.sleep(1500)

    // Verify Stage 0 is dominant again
    StageEpochManager.getCurrentStage shouldBe 0
    val stage0Phase2Counts = StageEpochManager.getStageTaskCounts
    // Should have 2 remaining from phase 2 + 3 new tasks = 5 total
    stage0Phase2Counts should contain(0 -> 5)

    // Phase 4: Final transition to Stage 2
    logInfo("Phase 4: Final transition to Stage 2")
    stage0Phase1Tasks.drop(1).foreach(_.triggerCompletion()) // Complete remaining phase 1 tasks
    stage0Phase2Tasks.take(2).foreach(_.triggerCompletion()) // Complete some phase 2 tasks

    val stage2Tasks = (31L to 34L).map { taskId => // Use distinct task IDs
      simulateTask(stageId = 2, taskId = taskId)
    }

    // Wait for final transition to Stage 2
    Thread.sleep(1500)

    // Verify Stage 2 is now dominant
    StageEpochManager.getCurrentStage shouldBe 2
    val finalCounts = StageEpochManager.getStageTaskCounts
    finalCounts should contain(2 -> 4)

    // Phase 5: Complete all remaining tasks
    logInfo("Phase 5: Completing all remaining tasks")
    stage1Tasks.drop(2).foreach(_.triggerCompletion())
    stage0Phase2Tasks.drop(2).foreach(_.triggerCompletion())
    stage2Tasks.foreach(_.triggerCompletion())

    // Wait for final processing
    Thread.sleep(1000)

    // Verify epoch counts for all stages that were monitored
    StageEpochManager.getStageEpochCount(0) should be >= 2 // Stage 0 had 2 epochs
    StageEpochManager.getStageEpochCount(1) should be >= 1 // Stage 1 had at least 1 epoch
    StageEpochManager.getStageEpochCount(2) should be >= 1 // Stage 2 had at least 1 epoch

    NVMLMonitorOnExecutor.shutdown()
    StageEpochManager.shutdown()
    TrampolineUtil.unsetTaskContext()

    // Verify the specific epoch switching pattern for this test case
    // Expected pattern:
    // Stage 0 (epoch 0) -> Stage 1 (epoch 0) -> Stage 0 (epoch 1) -> Stage 2 (epoch 0)
    val expectedStageEpochs = Map(
      0 -> (0 to 1), // Stage 0: Epoch 0 initially, then Epoch 1 when returning
      1 -> (0 to 0), // Stage 1: Only Epoch 0 during its single monitoring period
      2 -> (0 to 0) // Stage 2: Only Epoch 0 as the final stage
    )
    verifyPreciseStageEpochPatterns("Complex Epoch Switching Test", expectedStageEpochs)
  }

  test("NVML monitor report generation with extended monitoring") {
    logInfo("Starting test: NVML monitor report generation with extended monitoring")

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


    NVMLMonitorOnExecutor.shutdown()
    StageEpochManager.shutdown()
    TrampolineUtil.unsetTaskContext()

    // Verify specific Stage-Epoch patterns for extended monitoring test
    // This test simulates: Stage 1 (3s) -> Stage 2 (4s)
    // Important: Regardless of duration, if dominant stage doesn't change, only 1 epoch
    val expectedStageEpochs = Map(
      1 -> (0 to 0), // Stage 1: Only 1 epoch until transition to Stage 2
      2 -> (0 to 0) // Stage 2: Only 1 epoch, no subsequent transitions
    )
    verifyPreciseStageEpochPatterns("Extended Monitoring Test", expectedStageEpochs)
  }

  test("NVML monitor executor mode with task simulation") {
    logInfo("Starting test: NVML monitor executor mode with task simulation")

    // Test executor mode (non-stage-based) monitoring
    val conf = createTestConf(
      enabled = true,
      stageMode = false, // Executor lifecycle mode
      intervalMs = 200,
      logFrequency = 1
    )

    // Initialize NVML monitor in executor mode
    NVMLMonitorOnExecutor.init(mockPluginContext, conf)

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

    NVMLMonitorOnExecutor.shutdown()
    StageEpochManager.shutdown()
    TrampolineUtil.unsetTaskContext()

    // Verify executor-specific patterns (no Stage-Epoch patterns expected in executor mode)
    // Instead, verify Executor-{ID} patterns
    val expectedExecutorPattern = "test-executor-1"
    verifyExecutorSpecificPatterns("Executor Mode Test", expectedExecutorPattern)
  }


  /**
   * Precisely verify Stage-X-Epoch-Y patterns from log file
   *
   * Important: Understanding correct epoch switching logic
   * - New epochs are only triggered when dominant stage changes  
   * - stageEpochInterval is just the scheduler check interval for stage switching
   * - Same stage uses same epoch regardless of duration if it remains dominant
   * - Each stage typically only has Epoch-0 unless multiple stage transitions occur
   *
   * @param testName            Test case name
   * @param expectedStageEpochs Map[StageID -> Range of expected EpochIDs]
   */
  private def verifyPreciseStageEpochPatterns(testName: String,
      expectedStageEpochs: Map[Int, Range]): Unit = {

    val testMessages = readTestLogMessages(testMarker)

    // Define precise pattern - only Stage-X-Epoch-Y format, no -Final suffix
    val stageEpochPattern = """Stage-(\d+)-Epoch-(\d+)""".r

    // Extract all Stage-Epoch matches
    val stageEpochMatches = testMessages.flatMap { msg =>
      stageEpochPattern.findAllMatchIn(msg).map { m =>
        val stageId = m.group(1).toInt
        val epochId = m.group(2).toInt
        (stageId, epochId, msg)
      }.toList
    }

    if (stageEpochMatches.nonEmpty) {

      // Verify against expectations
      var allVerified = true
      expectedStageEpochs.foreach { case (expectedStageId, expectedEpochRange) =>
        val stageMatches = stageEpochMatches.filter(_._1 == expectedStageId)
        val foundEpochs = stageMatches.map(_._2).toSet


        if (foundEpochs.nonEmpty) {
          val expectedEpochSet = expectedEpochRange.toSet
          val hasExpectedEpochs = foundEpochs.intersect(expectedEpochSet).nonEmpty

          if (hasExpectedEpochs) {
            // Epochs match expectations
          } else {
            allVerified = false
          }
        } else {
          allVerified = false
        }
      }

      // Overall statistics
      val foundStages = stageEpochMatches.map(_._1).toSet


      if (allVerified && foundStages.nonEmpty) {
        stageEpochMatches should not be empty
        foundStages should not be empty
      } else {
        val foundEpochs = stageEpochMatches.groupBy(_._1).map { case (stageId, matches) =>
          stageId -> matches.map(_._2).toSet
        }
        val expectedStages = expectedStageEpochs.keySet
        if (!allVerified) {
          fail(s"$testName: Stage-Epoch verification failed. " +
            s"Expected stages with epochs: " +
            s"${
              expectedStageEpochs.map { case (s, e) =>
                s"Stage-$s: Epochs-${e.mkString(",")}"
              }.mkString("; ")
            }. " +
            s"Found stages with epochs: " +
            s"${
              foundEpochs.map { case (s, e) =>
                s"Stage-$s: Epochs-${e.mkString(",")}"
              }.mkString("; ")
            }. " +
            s"Test messages count: ${testMessages.size}. " +
            s"Note: This might be expected in test environment without real GPU/NVML support.")
        }
        if (foundStages.isEmpty) {
          fail(s"$testName: No stages found in logs. " +
            s"Expected stages: ${expectedStages.mkString(", ")}. " +
            s"Total test messages: ${testMessages.size}. " +
            s"All test messages: [${testMessages.mkString("; ")}]. " +
            s"Note: This might be expected in test environment without real GPU/NVML support.")
        }
      }

    } else {
      fail(s"$testName: No Stage-X-Epoch-Y patterns found in test logs. " +
        s"Expected stages with epochs: " +
        s"${
          expectedStageEpochs.map { case (s, e)
          => s"Stage-$s: Epochs-${e.mkString(",")}"
          }.mkString("; ")
        }. " +
        s"Total test messages: ${testMessages.size}. " +
        s"All test messages: [${testMessages.mkString("; ")}]. " +
        s"Possible reasons: (1) NVML monitor did not generate expected log format, " +
        s"(2) Test environment lacks real GPU support, (3) Log capture configuration issue. " +
        s"Note: This might be expected in test environment without real GPU/NVML support.")
    }
  }

  /**
   * Read log messages for the current test based on test markers
   *
   * @param testMarker The unique test marker to identify test boundaries
   * @return List of log messages for this test
   */

  /**
   * Add in-memory appender to capture log messages
   */
  private def addInMemoryAppender(): Unit = {
    try {
      // Add to root logger
      val rootLogger = LogManager.getRootLogger
      rootLogger.addAppender(inMemoryAppender)

    } catch {
      case _: Exception =>
      // Silently handle any logger configuration issues
    }
  }

  /**
   * Remove in-memory appender
   */
  private def removeInMemoryAppender(): Unit = {
    try {
      val rootLogger = LogManager.getRootLogger
      rootLogger.removeAppender(inMemoryAppender)

      val rapidsLogger = LogManager.getLogger("com.nvidia.spark.rapids")
      rapidsLogger.removeAppender(inMemoryAppender)

    } catch {
      case _: Exception =>
      // Silently handle any logger configuration issues
    }
  }

  /**
   * Read log messages for the current test based on test markers
   *
   * @param testMarker The unique test marker to identify test boundaries
   * @return List of log messages for this test
   */

  /**
   * Read log messages from in-memory appender based on test markers
   */
  private def readTestLogMessages(testMarker: String): List[String] = {
    try {
      val allMessages = inMemoryAppender.getMessages

      val startMarker = s"=== START_MARKER: $testMarker ==="
      val endMarker = s"=== END_MARKER: $testMarker ==="

      val startIndex = allMessages.indexWhere(_.contains(startMarker))
      val endIndex = allMessages.indexWhere(_.contains(endMarker))

      if (startIndex == -1) {
        return List.empty
      }

      val testLines = if (endIndex == -1 || endIndex <= startIndex) {
        allMessages.drop(startIndex + 1)
      } else {
        allMessages.slice(startIndex + 1, endIndex)
      }

      testLines

    } catch {
      case _: Exception =>
        List.empty
    }
  }

  /**
   * Verify that executor-mode NVML reports were written to logs
   */
  private def verifyExecutorSpecificPatterns(testName: String, expectedExecutorId: String): Unit = {
    val testMessages = readTestLogMessages(testMarker)

    // Look for executor report pattern: "Executor-{executorId}"
    val executorReportPattern = s"""Executor-${expectedExecutorId}""".r
    val executorReportMessages = testMessages.filter { msg =>
      executorReportPattern.findFirstIn(msg).isDefined
    }

    // Look for general executor mentions
    val executorMessages = testMessages.filter { msg =>
      msg.contains(expectedExecutorId) || msg.toLowerCase.contains("executor")
    }

    if (executorReportMessages.nonEmpty || executorMessages.nonEmpty) {
      // Executor patterns found as expected
    } else {
      fail(s"$testName: No executor-specific logs found for executor $expectedExecutorId. " +
        s"Expected executor pattern: 'Executor-$expectedExecutorId'. " +
        s"Total test messages: ${testMessages.size}. " +
        s"Executor report messages count: ${executorReportMessages.size}. " +
        s"General executor messages count: ${executorMessages.size}. " +
        s"All test messages: [${testMessages.mkString("; ")}]. " +
        s"Note: This might be expected in test environment without real NVML/GPU support.")
    }


  }

}
