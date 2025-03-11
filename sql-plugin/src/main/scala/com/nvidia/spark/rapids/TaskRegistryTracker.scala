/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.jni.RmmSpark
import java.util

import org.apache.spark.TaskContext

/**
 * This handles keeping track of task threads and registering them with RMMSpark
 * as needed. This is here to provide an efficient and lazy way to make sure that
 * we can use the Retry API behind the scenes without having to have callbacks
 * whenever a task starts, or trying to inject code in all of the operators that
 * would first start on the GPU.
 */
object TaskRegistryTracker {
  private val taskToThread = new util.HashMap[Long, ArrayBuffer[Long]]()
  private val registeredThreads = new util.HashSet[Long]()

  /**
   * Clear the registry. This is used for tests
   */
  def clearRegistry(): Unit = synchronized {
    val copied = new java.util.HashSet(taskToThread.keySet())
    copied.forEach { taskId =>
      taskIsDone(taskId)
    }
  }

  private def taskIsDone(taskId: Long): Unit = synchronized {
    val threads = taskToThread.remove(taskId)
    if (threads != null) {
      threads.foreach(registeredThreads.remove)
      RmmSpark.taskDone(taskId)
    }
  }

  /**
   * @return true if this attempt actually done registering
   */
  def registerThreadForRetry(): Boolean = synchronized {
    val tc = TaskContext.get()
    if (tc != null) {
      // If we don't have a TaskContext we are either in a test or in some other thread
      // If it is some other thread, then they are responsible to amke sure things are
      // registered properly themselves. If it is a test, well you need to update your
      // test code to make this work properly.
      val threadId = RmmSpark.getCurrentThreadId
      val taskId = tc.taskAttemptId()
      if (registeredThreads.add(threadId)) {
        RmmSpark.currentThreadIsDedicatedToTask(taskId)
        if (!taskToThread.containsKey(taskId)) {
          taskToThread.put(taskId, ArrayBuffer(threadId))
          ScalableTaskCompletion.onTaskCompletion(tc) {
            taskIsDone(taskId)
          }
        } else {
          taskToThread.get(taskId) += threadId
        }
        return true
      }
    }
    false
  }

  /**
   * @return true if this attempt actually done unregistering
   */
  def unregisterThreadForRetry(): Boolean = synchronized {
    val tc = TaskContext.get()
    if (tc != null) {
      val threadId = RmmSpark.getCurrentThreadId
      val taskId = tc.taskAttemptId()
      if (registeredThreads.remove(threadId)) {
        RmmSpark.removeAllCurrentThreadAssociation()
        if (taskToThread.containsKey(taskId)) {
          taskToThread.get(taskId) -= threadId
        }
        return true
      }
    }
    false
  }
}
