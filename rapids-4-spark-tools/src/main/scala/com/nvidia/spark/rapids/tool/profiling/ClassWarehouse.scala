/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.profiling

/**
 * This is a warehouse to store all Case Classes
 * used to create Spark DataFrame.
 */

case class ResourceProfileCase(
  id: Int, exec_cpu: Int, exec_mem: Long, exec_gpu: Int,
  exec_offheap: Long, task_cpu: Int, task_gpu: Double)

case class BlockManagerCase(
  executorID: String, host: String, port: Int,
  maxMem: Long, maxOnHeapMem: Long, maxOffHeapMem: Long)

case class BlockManagerRemovedCase(
  executorID: String, host: String, port: Int, time: Long)

case class PropertiesCase(
  source: String, key: String, value: String)

case class ApplicationCase(
  appName: String, appId: Option[String], startTime: Long,
  sparkUser: String, endTime: Option[Long], duration: Option[Long],
  durationStr: String, sparkVersion: String, gpuMode: Boolean)

case class ExecutorCase(
  executorID: String, host: String, totalCores: Int, resourceProfileId: Int)

case class ExecutorRemovedCase(
  executorID: String, reason: String, time: Long)