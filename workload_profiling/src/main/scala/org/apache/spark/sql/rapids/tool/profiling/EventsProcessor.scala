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

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._

/**
 * This object is to process all events and do validation in the end.
 */
object EventsProcessor {

  def processAnyEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    event match {
      case _: SparkListenerLogStart =>
        doSparkListenerLogStart(app, event.asInstanceOf[SparkListenerLogStart])
      case _: SparkListenerResourceProfileAdded =>
        doSparkListenerResourceProfileAdded(app,
          event.asInstanceOf[SparkListenerResourceProfileAdded])
      case _: SparkListenerBlockManagerAdded =>
        doSparkListenerBlockManagerAdded(app,
          event.asInstanceOf[SparkListenerBlockManagerAdded])
      case _: SparkListenerBlockManagerRemoved =>
        doSparkListenerBlockManagerRemoved(app,
          event.asInstanceOf[SparkListenerBlockManagerRemoved])
      case _: SparkListenerEnvironmentUpdate =>
        doSparkListenerEnvironmentUpdate(app,
          event.asInstanceOf[SparkListenerEnvironmentUpdate])
      case _: SparkListenerApplicationStart =>
        doSparkListenerApplicationStart(app,
          event.asInstanceOf[SparkListenerApplicationStart])
      case _: SparkListenerApplicationEnd =>
        doSparkListenerApplicationEnd(app,
          event.asInstanceOf[SparkListenerApplicationEnd])
      case _: SparkListenerExecutorAdded =>
        doSparkListenerExecutorAdded(app,
          event.asInstanceOf[SparkListenerExecutorAdded])
      case _: SparkListenerExecutorRemoved =>
        doSparkListenerExecutorRemoved(app,
          event.asInstanceOf[SparkListenerExecutorRemoved])
      case _ => doOtherEvent(app, event)
    }
  }

  def doSparkListenerLogStart(app: ApplicationInfo, event: SparkListenerLogStart): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    app.sparkVersion = event.sparkVersion
  }

  def doSparkListenerResourceProfileAdded(
      app: ApplicationInfo,
      event: SparkListenerResourceProfileAdded): Unit = {

    app.logger.debug("Processing event: " + event.getClass)

    val res = event.resourceProfile
    val thisResourceProfile = ResourceProfileCase(
      res.id,
      res.getExecutorCores.getOrElse(0),
      res.executorResources.get(ResourceProfile.MEMORY).map(_.amount.toLong).getOrElse(0),
      res.executorResources.get("gpu").map(_.amount.toInt).getOrElse(0),
      res.executorResources.get(ResourceProfile.OVERHEAD_MEM).map(_.amount.toLong).getOrElse(0),
      res.getTaskCpus.getOrElse(0),
      res.taskResources.get("gpu").map(_.amount.toDouble).getOrElse(0)
    )
    app.resourceProfiles += thisResourceProfile
  }

  def doSparkListenerBlockManagerAdded(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerAdded): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    val thisBlockManager = BlockManagerCase(
      event.blockManagerId.executorId,
      event.blockManagerId.host,
      event.blockManagerId.port,
      event.maxMem,
      event.maxOnHeapMem.getOrElse(0),
      event.maxOffHeapMem.getOrElse(0)
    )
    app.blockManagers += thisBlockManager
  }

  def doSparkListenerBlockManagerRemoved(
      app: ApplicationInfo,
      event: SparkListenerBlockManagerRemoved): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    val thisBlockManagerRemoved = BlockManagerRemovedCase(
      event.blockManagerId.executorId,
      event.blockManagerId.host,
      event.blockManagerId.port,
      event.time
    )
    app.blockManagersRemoved += thisBlockManagerRemoved
  }

  def doSparkListenerEnvironmentUpdate(
      app: ApplicationInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    app.sparkProperties = event.environmentDetails("Spark Properties").toMap
    app.hadoopProperties = event.environmentDetails("Hadoop Properties").toMap
    app.systemProperties = event.environmentDetails("System Properties").toMap
    app.jvmInfo = event.environmentDetails("JVM Information").toMap
    app.classpathEntries = event.environmentDetails("Classpath Entries").toMap

    //Decide if this application is on GPU Mode
    if (ProfileUtils.isGPUMode(collection.mutable.Map() ++= app.sparkProperties)) {
      app.gpuMode = true
      app.logger.debug("App's GPU Mode = TRUE")
    } else {
      app.logger.debug("App's GPU Mode = FALSE")
    }
  }

  def doSparkListenerApplicationStart(
      app: ApplicationInfo,
      event: SparkListenerApplicationStart): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    val thisAppStart = ApplicationCase(
      event.appName,
      event.appId,
      event.time,
      event.sparkUser,
      None,
      None,
      "",
      "",
      gpuMode = false
    )
    app.appStart += thisAppStart
    app.appId = event.appId.getOrElse("")
  }

  def doSparkListenerApplicationEnd(
      app: ApplicationInfo,
      event: SparkListenerApplicationEnd): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  def doSparkListenerExecutorAdded(
      app: ApplicationInfo,
      event: SparkListenerExecutorAdded): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    val executor = ExecutorCase(
      event.executorId,
      event.executorInfo.executorHost,
      event.executorInfo.totalCores,
      event.executorInfo.resourceProfileId
    )
    app.executors += executor
  }

  def doSparkListenerExecutorRemoved(
      app: ApplicationInfo,
      event: SparkListenerExecutorRemoved): Unit = {
    app.logger.debug("Processing event: " + event.getClass)
    val thisExecutorRemoved = ExecutorRemovedCase(
      event.executorId,
      event.reason,
      event.time
    )
    app.executorsRemoved += thisExecutorRemoved
  }

  // To process all other unknown events
  def doOtherEvent(app: ApplicationInfo, event: SparkListenerEvent): Unit = {
    app.logger.info("Processing other event: " + event.getClass)
    app.otherEvents += event
  }

  // Print all details the ApplicationInfo object to validate data.
  // This is just for debug
  def validateApp(app: ApplicationInfo): Unit = {
    val logger = app.logger
    logger.debug("Application version: " + app.sparkVersion)

    //Print all Dataframes
    for ((name, df) <- app.allDataFrames) {
      logger.debug(name + ":")
      df.createOrReplaceTempView(name)
      logger.debug("\n" + df.showString(10))
      logger.debug("\n" + df.schema.treeString)
    }

    logger.debug("sparkProperties: " + app.sparkProperties)
    logger.debug("hadoopProperties: " + app.hadoopProperties)
    logger.debug("systemProperties: " + app.systemProperties)
    logger.debug("jvmInfo: " + app.jvmInfo)
    logger.debug("classpathEntries: " + app.classpathEntries)

    logger.debug("Rapids Accelerator Parameters:")
    logger.debug("spark.plugins=" + app.sparkProperties.getOrElse("spark.plugins", ""))
    logger.debug("spark.rapids.sql.enabled=" +
        app.sparkProperties.getOrElse("spark.rapids.sql.enabled", ""))
  }
}