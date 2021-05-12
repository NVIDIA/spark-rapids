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

package org.apache.spark.sql.rapids.tool.profiling

/**
 * This is a warehouse to store all Case Classes
 * used to create Spark DataFrame.
 */

protected case class ResourceProfileCase(
  id: Int, exec_cpu: Int, exec_mem: Long, exec_gpu: Int,
  exec_offheap: Long, task_cpu: Int, task_gpu: Double)

protected case class BlockManagerCase(
  executorID: String, host: String, port: Int,
  maxMem: Long, maxOnHeapMem: Long, maxOffHeapMem: Long)

protected case class BlockManagerRemovedCase(
  executorID: String, host: String, port: Int, time: Long)

protected case class PropertiesCase(
  source: String, key: String, value: String)

protected case class ApplicationCase(
  appName: String, appId: Option[String], startTime: Long,
  sparkUser: String, endTime: Option[Long], duration: Option[Long],
  durationStr: String, sparkVersion: String, gpuMode: Boolean)

protected case class ExecutorCase(
  executorID: String, host: String, totalCores: Int, resourceProfileId: Int)

protected case class ExecutorRemovedCase(
  executorID: String, reason: String, time: Long)