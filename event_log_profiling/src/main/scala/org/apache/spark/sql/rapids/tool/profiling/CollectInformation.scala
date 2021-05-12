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

import scala.collection.mutable.ArrayBuffer

/**
 * CollectInformation mainly print information based on this event log:
 * Such as executors, parameters, etc.
 */
class CollectInformation(apps: ArrayBuffer[ApplicationInfo]) {

  require(apps.nonEmpty)
  private val logger = apps.head.logger

  // Print Application Information
  def printAppInfo(): Unit = {
    logger.info("Application Information:")
    for (app <- apps) {
      app.runQuery(app.generateAppInfo)
    }
  }

  // Print rapids-4-spark and cuDF jar if CPU Mode is on.
  def printRapidsJAR(): Unit = {
    for (app <- apps) {
      if (app.gpuMode) {
        logger.info(s"Application ${app.appId} (index=${app.index}) 's" +
            " Rapids Accelerator Jar and cuDF Jar:")
        // Look for rapids-4-spark and cuDF jar
        val rapidsJar = app.classpathEntries.filterKeys(_ matches ".*rapids-4-spark.*jar")
        val cuDFJar = app.classpathEntries.filterKeys(_ matches ".*cudf.*jar")
        if (rapidsJar.nonEmpty) {
          rapidsJar.keys.foreach(k => logger.info(k))
        }
        if (cuDFJar.nonEmpty) {
          cuDFJar.keys.foreach(k => logger.info(k))
        }
      }
    }
  }

  // Print executor related information
  def printExecutorInfo(): Unit = {
    logger.info("Executor Information:")
    for (app <- apps) {
      app.runQuery(app.generateExecutorInfo + " order by cast(executorID as long)")
    }
  }

  // Print Rapids related Spark Properties
  def printRapidsProperties(): Unit = {
    logger.info("Spark Rapids parameters set explicitly:")
    for (app <- apps) {
      app.runQuery(app.generateRapidsProperties + " order by key")
    }
  }
}