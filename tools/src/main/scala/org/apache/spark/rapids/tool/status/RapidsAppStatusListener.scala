/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.rapids.tool.status

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.ElementTrackingStore

class RapidsAppStatusListener(
  conf: SparkConf,
  kvstore: ElementTrackingStore,
  live: Boolean) extends SparkListener with Logging {
  // TODO: add handling of events here
  /*
  private var appInfo: ApplicationInfo = null

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    val app = new ApplicationInfo(null, "", 1)
  }
  */
}
