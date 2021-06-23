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

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui._

abstract class EventProcessorBase extends Logging {

  type aInfo <: AppBase

  def processAnyEvent(app: aInfo, event: SparkListenerEvent): Unit = {
    event match {
      case _: SparkListenerLogStart =>
        doSparkListenerLogStart(app, event.asInstanceOf[SparkListenerLogStart])
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
      case _: SparkListenerTaskStart =>
        doSparkListenerTaskStart(app,
          event.asInstanceOf[SparkListenerTaskStart])
      case _: SparkListenerTaskEnd =>
        doSparkListenerTaskEnd(app,
          event.asInstanceOf[SparkListenerTaskEnd])
      case _: SparkListenerSQLExecutionStart =>
        doSparkListenerSQLExecutionStart(app,
          event.asInstanceOf[SparkListenerSQLExecutionStart])
      case _: SparkListenerSQLExecutionEnd =>
        doSparkListenerSQLExecutionEnd(app,
          event.asInstanceOf[SparkListenerSQLExecutionEnd])
      case _: SparkListenerDriverAccumUpdates =>
        doSparkListenerDriverAccumUpdates(app,
          event.asInstanceOf[SparkListenerDriverAccumUpdates])
      case _: SparkListenerJobStart =>
        doSparkListenerJobStart(app,
          event.asInstanceOf[SparkListenerJobStart])
      case _: SparkListenerJobEnd =>
        doSparkListenerJobEnd(app,
          event.asInstanceOf[SparkListenerJobEnd])
      case _: SparkListenerStageSubmitted =>
        doSparkListenerStageSubmitted(app,
          event.asInstanceOf[SparkListenerStageSubmitted])
      case _: SparkListenerStageCompleted =>
        doSparkListenerStageCompleted(app,
          event.asInstanceOf[SparkListenerStageCompleted])
      case _: SparkListenerTaskGettingResult =>
        doSparkListenerTaskGettingResult(app,
          event.asInstanceOf[SparkListenerTaskGettingResult])
      case _: SparkListenerSQLAdaptiveExecutionUpdate =>
        doSparkListenerSQLAdaptiveExecutionUpdate(app,
          event.asInstanceOf[SparkListenerSQLAdaptiveExecutionUpdate])
      case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
        doSparkListenerSQLAdaptiveSQLMetricUpdates(app,
          event.asInstanceOf[SparkListenerSQLAdaptiveSQLMetricUpdates])
      case _ =>
        val wasResourceProfileAddedEvent = doSparkListenerResourceProfileAddedReflect(app, event)
        if (!wasResourceProfileAddedEvent) doOtherEvent(app, event)
    }
  }

  def doSparkListenerResourceProfileAddedReflect(
      app: aInfo,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case _: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app,
              event.asInstanceOf[SparkListenerResourceProfileAdded])
            true
          case _ => false
        }
      } catch {
        case _: ClassNotFoundException =>
          logWarning("Error trying to parse SparkListenerResourceProfileAdded, Spark" +
            " version likely older than 3.1.X, unable to parse it properly.")
          false
      }
    } else {
      false
    }
  }

  def doSparkListenerLogStart(
      app: aInfo,
      event: SparkListenerLogStart): Unit  = {
    app.sparkVersion = event.sparkVersion
  }

  def doSparkListenerResourceProfileAdded(
      app: aInfo,
      event: SparkListenerResourceProfileAdded): Unit = {}

  def doSparkListenerBlockManagerAdded(
      app: aInfo,
      event: SparkListenerBlockManagerAdded): Unit = {}

  def doSparkListenerBlockManagerRemoved(
      app: aInfo,
      event: SparkListenerBlockManagerRemoved): Unit = {}

  def doSparkListenerEnvironmentUpdate(
      app: aInfo,
      event: SparkListenerEnvironmentUpdate): Unit = {}

  def doSparkListenerApplicationStart(
      app: aInfo,
      event: SparkListenerApplicationStart): Unit = {}

  def doSparkListenerApplicationEnd(
      app: aInfo,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  def doSparkListenerExecutorAdded(
      app: aInfo,
      event: SparkListenerExecutorAdded): Unit = {}

  def doSparkListenerExecutorRemoved(
      app: aInfo,
      event: SparkListenerExecutorRemoved): Unit = {}

  def doSparkListenerTaskStart(
      app: aInfo,
      event: SparkListenerTaskStart): Unit = {}

  def doSparkListenerTaskEnd(
      app: aInfo,
      event: SparkListenerTaskEnd): Unit = {}

  def doSparkListenerSQLExecutionStart(
      app: aInfo,
      event: SparkListenerSQLExecutionStart): Unit = {}

  def doSparkListenerSQLExecutionEnd(
      app: aInfo,
      event: SparkListenerSQLExecutionEnd): Unit = {}

  def doSparkListenerDriverAccumUpdates(
      app: aInfo,
      event: SparkListenerDriverAccumUpdates): Unit = {}

  def doSparkListenerJobStart(
      app: aInfo,
      event: SparkListenerJobStart): Unit = {}

  def doSparkListenerJobEnd(
      app: aInfo,
      event: SparkListenerJobEnd): Unit = {}

  def doSparkListenerStageSubmitted(
      app: aInfo,
      event: SparkListenerStageSubmitted): Unit = {}

  def doSparkListenerStageCompleted(
      app: aInfo,
      event: SparkListenerStageCompleted): Unit = {}

  def doSparkListenerTaskGettingResult(
      app: aInfo,
      event: SparkListenerTaskGettingResult): Unit = {}

  def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: aInfo,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {}

  def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: aInfo,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {}

  // To process all other unknown events
  def doOtherEvent(
      app: aInfo,
      event: SparkListenerEvent): Unit = {}
}
