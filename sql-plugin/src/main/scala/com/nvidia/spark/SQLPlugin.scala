/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package com.nvidia.spark

import com.nvidia.spark.rapids.{RapidsDriverPlugin, RapidsExecutorPlugin}

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging

/**
 * The RAPIDS plugin for Spark.
 * To enable this plugin, set the config "spark.plugins" to `com.nvidia.spark.SQLPlugin`
 */
class SQLPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new RapidsDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new RapidsExecutorPlugin
}
