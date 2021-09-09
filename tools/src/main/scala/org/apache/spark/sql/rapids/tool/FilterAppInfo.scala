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

import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerEvent}

case class ApplicationStartInfo(
    appName: String,
    startTime: Long,
    userName: String)

case class EnvironmentInfo(configName: Map[String, String])

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
    appStartInfo = Some(thisAppInfo)
  }

  def doSparkListenerEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logDebug("Processing event: " + event.getClass)
    val sparkProperties = event.environmentDetails("Spark Properties").toMap
    configInfo = Some(EnvironmentInfo(sparkProperties))
  }

  var appStartInfo: Option[ApplicationStartInfo] = None
  var configInfo: Option[EnvironmentInfo] = None
  // This acts as a counter and it returns true when both the Listener events are processed.
  var eventsToProcess: Int = Some(appStartInfo).size + Some(configInfo).size

  override def processEvent(event: SparkListenerEvent): Boolean = {
    if (event.isInstanceOf[SparkListenerApplicationStart]) {
      doSparkListenerApplicationStart(event.asInstanceOf[SparkListenerApplicationStart])
      (eventsToProcess -= 1) == 0
    } else if (event.isInstanceOf[SparkListenerEnvironmentUpdate]) {
      doSparkListenerEnvironmentUpdate(event.asInstanceOf[SparkListenerEnvironmentUpdate])
      (eventsToProcess -= 1) == 0
    } else {
      false
    }
  }

  processEvents()
}
