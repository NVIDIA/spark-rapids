/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import ai.rapids.cudf.Cuda
import com.nvidia.spark.rapids.{GpuDeviceManager, RapidsConf}
import com.nvidia.spark.rapids.python.PythonConfEntries._

import org.apache.spark.SparkEnv
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python.{PYTHON_USE_DAEMON, PYTHON_WORKER_MODULE}

object GpuPythonHelper extends Logging {

  private lazy val sparkConf = SparkEnv.get.conf
  private lazy val rapidsConf = new RapidsConf(sparkConf)
  private lazy val gpuId = GpuDeviceManager.getDeviceId()
    .getOrElse(throw new IllegalStateException("No gpu id!"))
    .toString
  private lazy val isPythonPooledMemEnabled = rapidsConf.get(PYTHON_POOLED_MEM)
    .getOrElse(rapidsConf.isPooledMemEnabled)
    .toString
  private lazy val isPythonUvmEnabled = rapidsConf.get(PYTHON_UVM_ENABLED)
    .getOrElse(rapidsConf.isUvmEnabled)
    .toString
  private lazy val (initAllocPerWorker, maxAllocPerWorker) = {
    val info = Cuda.memGetInfo()
    val maxFactionTotal = rapidsConf.get(PYTHON_RMM_MAX_ALLOC_FRACTION)
    val maxAllocTotal = (maxFactionTotal * info.total).toLong
    // Initialize pool size for all pythons workers. If the fraction is not set,
    // use half of the free memory as default.
    val initAllocTotal = rapidsConf.get(PYTHON_RMM_ALLOC_FRACTION)
      .map { fraction =>
        if (0 < maxFactionTotal && maxFactionTotal < fraction) {
          throw new IllegalArgumentException(s"The value of '$PYTHON_RMM_MAX_ALLOC_FRACTION' " +
            s"should not be less than that of '$PYTHON_RMM_ALLOC_FRACTION', but found " +
            s"$maxFactionTotal < $fraction")
        }
        (fraction * info.total).toLong
      }
      .getOrElse((0.5 * info.free).toLong)
    if (initAllocTotal > info.free) {
      logWarning(s"Initial RMM allocation(${initAllocTotal / 1024.0 / 1024} MB) for " +
        s"all the Python workers is larger than free memory(${info.free / 1024.0 / 1024} MB)")
    } else {
      logDebug(s"Configure ${initAllocTotal / 1024.0 / 1024}MB GPU memory for " +
        s"all the Python workers.")
    }

    // Calculate the pool size for each Python worker.
    val concurrentPythonWorkers = rapidsConf.get(CONCURRENT_PYTHON_WORKERS)
    // Spark does not throw exception even the value of CPUS_PER_TASK is negative, so
    // return 1 if it is less than zero to continue the task.
    val cpuTaskSlots = sparkConf.get(EXECUTOR_CORES) / Math.max(1, sparkConf.get(CPUS_PER_TASK))
    if (0 < concurrentPythonWorkers && concurrentPythonWorkers <= cpuTaskSlots) {
      (initAllocTotal / concurrentPythonWorkers, maxAllocTotal / concurrentPythonWorkers)
    } else {
      // When semaphore is disabled or invalid, use the number of cpu task slots instead.
      (initAllocTotal / cpuTaskSlots, maxAllocTotal / cpuTaskSlots)
    }
  }

  def injectGpuInfo(funcs: Seq[ChainedPythonFunctions]): Unit = {
    // Insert GPU related env(s) into `envVars` for all the PythonFunction(s).
    // Yes `PythonRunner` will only use the first one, but just make sure it will
    // take effect no matter the order changes or not.
    funcs.foreach(_.funcs.foreach { pyF =>
      pyF.envVars.put("CUDA_VISIBLE_DEVICES", gpuId)
      pyF.envVars.put("RAPIDS_UVM_ENABLED", isPythonUvmEnabled)
      pyF.envVars.put("RAPIDS_POOLED_MEM_ENABLED", isPythonPooledMemEnabled)
      pyF.envVars.put("RAPIDS_POOLED_MEM_SIZE", initAllocPerWorker.toString)
      pyF.envVars.put("RAPIDS_POOLED_MEM_MAX_SIZE", maxAllocPerWorker.toString)
    })

    // Check and overwrite the related conf(s):
    // - pyspark worker module.
    //   For GPU case, need to customize the worker module for the GPU initialization
    //   and de-initialization
    sparkConf.get(PYTHON_WORKER_MODULE).foreach(value =>
      if (value != "rapids.worker") {
        logWarning(s"Found PySpark worker is set to '$value', overwrite it to 'rapids.worker'.")
      }
    )
    sparkConf.set(PYTHON_WORKER_MODULE, "rapids.worker")
    logWarning("Disable python daemon to enable customized 'rapids.worker'.")
    sparkConf.set(PYTHON_USE_DAEMON, false)
  }
}
