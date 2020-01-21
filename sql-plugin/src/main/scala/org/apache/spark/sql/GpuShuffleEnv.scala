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

package org.apache.spark.sql

import ai.rapids.spark.RapidsConf
import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, config}

object GpuShuffleEnv extends Logging {
  private var isRapidsShuffleManagerInitialized: Boolean  = false

  // the shuffle plugin will call this on initialize
  def setRapidsShuffleManagerInitialized(initialized: Boolean): Unit = {
    logInfo("RapidsShuffleManager is initialized")
    isRapidsShuffleManagerInitialized = initialized
  }

  lazy val isRapidsShuffleEnabled: Boolean = {
    val env = SparkEnv.get
    val isRapidsManager = isRapidsShuffleManagerInitialized
    val externalShuffle = env.blockManager.externalShuffleServiceEnabled
    isRapidsManager && !externalShuffle
  }
}
