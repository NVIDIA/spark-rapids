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

import com.nvidia.spark.rapids.RapidsConf.conf

object PythonConfEntries {

  val CONCURRENT_PYTHON_WORKERS = conf("spark.rapids.python.concurrentPythonWorkers")
    .doc("Set the number of Python worker processes that can execute concurrently per GPU. " +
      "Python worker processes may temporarily block when the number of concurrent Python " +
      "worker processes started by the same executor exceeds this amount. Allowing too " +
      "many concurrent tasks on the same GPU may lead to GPU out of memory errors. " +
      ">0 means enabled, while <=0 means unlimited")
    .integerConf
    .createWithDefault(0)

  val PYTHON_RMM_ALLOC_FRACTION = conf("spark.rapids.python.memory.gpu.allocFraction")
    .doc("The fraction of total GPU memory that should be initially allocated " +
      "for pooled memory for all the Python workers. It supposes to be less than " +
      "(1 - $(spark.rapids.memory.gpu.allocFraction)), since the executor will share the " +
      "GPU with its owning Python workers.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The fraction value for Python workers must be in [0, 1].")
    .createOptional

  val PYTHON_RMM_MAX_ALLOC_FRACTION = conf("spark.rapids.python.memory.gpu.maxAllocFraction")
    .doc("The fraction of total GPU memory that limits the maximum size of the RMM pool " +
      "for all the Python workers. It supposes to be less than " +
      "(1 - $(spark.rapids.memory.gpu.maxAllocFraction)), since the executor will share the " +
      "GPU with its owning Python workers. when setting to 0 means no limit.")
    .doubleConf
    .checkValue(v => v >= 0 && v <= 1, "The value of maxAllocFraction for Python workers must be" +
      " in [0, 1].")
    .createWithDefault(0.0)

  val PYTHON_POOLED_MEM = conf("spark.rapids.python.memory.gpu.pooling.enabled")
    .doc("Should RMM in Python workers act as a pooling allocator for GPU memory, or" +
      " should it just pass through to CUDA memory allocation directly.")
    .booleanConf
    .createOptional

  val PYTHON_UVM_ENABLED = conf("spark.rapids.python.memory.uvm.enabled")
    .doc("Similar with `spark.rapids.python.memory.uvm.enabled`, but this conf is for " +
      "python workers. This is an experimental feature.")
    .booleanConf
    .createOptional
<<<<<<< HEAD
=======

  // An empty function called by RapidsConf to initialize the config definitions above for
  // doc generation
  def init(): Unit = {}
>>>>>>> 3f94ac8b608e311c181892fc72756d894627037f
}
