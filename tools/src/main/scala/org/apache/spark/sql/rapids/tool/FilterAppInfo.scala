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

package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.EventLogInfo
import org.apache.hadoop.conf.Configuration

import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEvent}

case class ApplicationStartInfo(
    appName: String,
    startTime: Long,
    userName: String)

class FilterAppInfo(
    numOutputRows: Int,
    eventLogInfo: EventLogInfo,
    hadoopConf: Configuration) extends AppBase(numOutputRows, eventLogInfo, hadoopConf) {

  def doSparkListenerApplicationStart(
      event: SparkListenerApplicationStart): Unit = {
    logDebug("Processing event: " + event.getClass)
    val thisAppInfo = ApplicationStartInfo(
      event.appName,
      event.time,
      event.sparkUser
    )
    appInfo = Some(thisAppInfo)
  }

  var appInfo: Option[ApplicationStartInfo] = None

  override def processEvent(event: SparkListenerEvent): Boolean = {
    if (event.isInstanceOf[SparkListenerApplicationStart]) {
      doSparkListenerApplicationStart(event.asInstanceOf[SparkListenerApplicationStart])
      true
    } else {
      false
    }
  }

  processEvents()
}
