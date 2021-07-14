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

trait EventProcessorBase extends Logging {

  type T <: AppBase

  def processAnyEvent(app: T, event: SparkListenerEvent): Unit = {
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
      app: T,
      event: SparkListenerEvent): Boolean = {
    val rpAddedClass = "org.apache.spark.scheduler.SparkListenerResourceProfileAdded"
    if (event.getClass.getName.equals(rpAddedClass)) {
      try {
        event match {
          case rpAdded: SparkListenerResourceProfileAdded =>
            doSparkListenerResourceProfileAdded(app, rpAdded)
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
      app: T,
      event: SparkListenerLogStart): Unit  = {
    app.sparkVersion = event.sparkVersion
  }

  def doSparkListenerResourceProfileAdded(
      app: T,
      event: SparkListenerResourceProfileAdded): Unit = {}

  def doSparkListenerBlockManagerAdded(
      app: T,
      event: SparkListenerBlockManagerAdded): Unit = {}

  def doSparkListenerBlockManagerRemoved(
      app: T,
      event: SparkListenerBlockManagerRemoved): Unit = {}

  def doSparkListenerEnvironmentUpdate(
      app: T,
      event: SparkListenerEnvironmentUpdate): Unit = {}

  def doSparkListenerApplicationStart(
      app: T,
      event: SparkListenerApplicationStart): Unit = {}

  def doSparkListenerApplicationEnd(
      app: T,
      event: SparkListenerApplicationEnd): Unit = {
    logDebug("Processing event: " + event.getClass)
    app.appEndTime = Some(event.time)
  }

  def doSparkListenerExecutorAdded(
      app: T,
      event: SparkListenerExecutorAdded): Unit = {}

  def doSparkListenerExecutorRemoved(
      app: T,
      event: SparkListenerExecutorRemoved): Unit = {}

  def doSparkListenerTaskStart(
      app: T,
      event: SparkListenerTaskStart): Unit = {}

  def doSparkListenerTaskEnd(
      app: T,
      event: SparkListenerTaskEnd): Unit = {}

  def doSparkListenerSQLExecutionStart(
      app: T,
      event: SparkListenerSQLExecutionStart): Unit = {}

  def doSparkListenerSQLExecutionEnd(
      app: T,
      event: SparkListenerSQLExecutionEnd): Unit = {}

  def doSparkListenerDriverAccumUpdates(
      app: T,
      event: SparkListenerDriverAccumUpdates): Unit = {}

  def doSparkListenerJobStart(
      app: T,
      event: SparkListenerJobStart): Unit = {}

  def doSparkListenerJobEnd(
      app: T,
      event: SparkListenerJobEnd): Unit = {}

  def doSparkListenerStageSubmitted(
      app: T,
      event: SparkListenerStageSubmitted): Unit = {}

  def doSparkListenerStageCompleted(
      app: T,
      event: SparkListenerStageCompleted): Unit = {}

  def doSparkListenerTaskGettingResult(
      app: T,
      event: SparkListenerTaskGettingResult): Unit = {}

  def doSparkListenerSQLAdaptiveExecutionUpdate(
      app: T,
      event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {}

  def doSparkListenerSQLAdaptiveSQLMetricUpdates(
      app: T,
      event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {}

  // To process all other unknown events
  def doOtherEvent(
      app: T,
      event: SparkListenerEvent): Unit = {}
}
