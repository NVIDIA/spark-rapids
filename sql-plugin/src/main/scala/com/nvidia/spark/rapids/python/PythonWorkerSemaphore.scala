/*
<<<<<<< HEAD
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
=======
 * Copyright (c) 2020, NVIDIA CORPORATION.
>>>>>>> 3f94ac8b608e311c181892fc72756d894627037f
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

package com.nvidia.spark.rapids.python

import com.nvidia.spark.rapids.{GpuSemaphore, RapidsConf}
import com.nvidia.spark.rapids.python.PythonConfEntries.CONCURRENT_PYTHON_WORKERS

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging

/*
 * PythonWorkerSemaphore is used to limit the number of Python workers(processes) to be started
 * by an executor.
 *
 * This PythonWorkerSemaphore will not initialize the GPU, different from GpuSemaphore. Since
 * tasks calling the API `acquireIfNecessary` are supposed not to use the GPU directly, but
 * delegate the permits to the Python workers respectively.
 *
 * Call `acquireIfNecessary` or `releaseIfNecessary` directly when needed, since the inner
 * semaphore will be initialized implicitly, but need to call `shutdown` explicitly to release
 * the inner semaphore when no longer needed.
 *
 */
object PythonWorkerSemaphore extends Logging {

  private lazy val workersPerGpu = new RapidsConf(SparkEnv.get.conf)
    .get(CONCURRENT_PYTHON_WORKERS)
  private lazy val enabled = workersPerGpu > 0

  // DO NOT ACCESS DIRECTLY!  Use `getInstance` instead.
  @volatile
  private var instance: GpuSemaphore = _

  private def getInstance(): GpuSemaphore = {
    if (instance == null) {
      GpuSemaphore.synchronized {
        if (instance == null) {
          logDebug(s"Initialize the python workers semaphore with number: $workersPerGpu")
          instance = new GpuSemaphore(workersPerGpu, false)
        }
      }
    }
    instance
  }

  /*
   * Tasks must call this when they begin to start a Python worker who will use GPU.
   * If the task has not already acquired the GPU semaphore then it is acquired,
   * blocking if necessary.
   * NOTE: A task completion listener will automatically be installed to ensure
   *       the semaphore is always released by the time the task completes.
   */
  def acquireIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.acquireIfNecessary(context)
    }
  }

  /*
   * Tasks must call this when they are finished using the GPU.
   */
  def releaseIfNecessary(context: TaskContext): Unit = {
    if (enabled && context != null) {
      getInstance.releaseIfNecessary(context)
    }
  }

  /*
   * Release the inner semaphore.
   * NOTE: This does not wait for active tasks to release!
   */
  def shutdown(): Unit = synchronized {
    if (instance != null) {
      instance.shutdown()
      instance = null
    }
  }
}
