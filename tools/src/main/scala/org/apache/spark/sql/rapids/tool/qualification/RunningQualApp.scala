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

package org.apache.spark.sql.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkEnv

/**
 * A Qualification app that is processing events while the application is 
 * actively running.
 */
class RunningQualApp(
    hadoopConf: Configuration,
    pluginTypeChecker: Option[PluginTypeChecker],
    readScorePercent: Int)
  extends QualAppInfo(None, hadoopConf, pluginTypeChecker, readScorePercent) {

  def initApp(): Unit = {
    val appName = SparkEnv.get.conf.get("spark.app.name", "")
    val appIdConf = SparkEnv.get.conf.getOption("spark.app.id")
    val appStartTime = SparkEnv.get.conf.get("spark.app.startTime", "-1")

    // start event doesn't happen so initial it
    val thisAppInfo = QualApplicationInfo(
      appName,
      appIdConf,
      appStartTime.toLong,
      "",
      None,
      None,
      endDurationEstimated = false
    )
    appInfo = Some(thisAppInfo)
  }

  initApp()
}
