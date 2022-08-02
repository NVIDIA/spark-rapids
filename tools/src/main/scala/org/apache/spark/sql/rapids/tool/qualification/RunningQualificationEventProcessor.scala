/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.qualification.{QualOutputWriter, RunningQualOutputWriter}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class RunningQualificationEventProcessor(sparkConf: SparkConf) extends SparkListener with Logging {

  private val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp()
  private val listener = qualApp.getEventListener

  private val outputFileFromConfig = sparkConf.get("spark.rapids.qualification.outputFile", "")
  private lazy val appName = qualApp.appInfo.map(_.appName).getOrElse("")
  private lazy val fileWriter: Option[RunningQualOutputWriter] = if (outputFileFromConfig.nonEmpty) {
    Some(new RunningQualOutputWriter(qualApp.appId, appName, outputFileFromConfig))
  } else {
    None
  }

  private val outputFunc = (output: String) => {
    if (outputFileFromConfig.nonEmpty) {
      fileWriter.foreach { writer =>
        writer.
      }
    } else {
      logWarning(output)
    }
  }

  private def close(): Unit = {
    fileWriter.foreach(_.close())
  }

  private def initWriter: Unit = {
    if (outputFileFromConfig.nonEmpty) {
      fileWriter.foreach(_.init())
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    listener.onStageCompleted(stageCompleted)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    listener.onStageSubmitted(stageSubmitted)
  }

  override def onTaskStart(onTaskStart: SparkListenerTaskStart): Unit = {
    listener.onTaskStart(onTaskStart)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    listener.onTaskGettingResult(taskGettingResult)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    listener.onTaskEnd(taskEnd)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    listener.onJobStart(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    listener.onJobEnd(jobEnd)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    listener.onEnvironmentUpdate(environmentUpdate)
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    listener.onBlockManagerAdded(blockManagerAdded)
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    listener.onBlockManagerRemoved(blockManagerRemoved)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    listener.onApplicationStart(applicationStart)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    listener.onApplicationEnd(applicationEnd)
    fileWriter.foreach { writer =>
      qualApp.getApplicationDetails.foreach(writer.writeApplicationReport(_))
    }
    close()
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    listener.onExecutorMetricsUpdate(executorMetricsUpdate)
  }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = {
    listener.onStageExecutorMetrics(executorMetrics)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    listener.onExecutorAdded(executorAdded)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    listener.onExecutorRemoved(executorRemoved)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart =>
        logWarning("starting new SQL query")
      case e: SparkListenerSQLExecutionEnd =>
        val (csvSQLInfo, textSQLInfo) = qualApp.getPerSqlTextAndCSVSummary(e.executionId)
        fileWriter.foreach(_.writePerSqlCSVReport(csvSQLInfo))
        logWarning(s"Done with SQL query ${e.executionId} \n summary: $textSQLInfo")
        fileWriter.foreach(_.writePerSqlTextReport(textSQLInfo))
    }
    listener.onOtherEvent(event)
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    listener.onResourceProfileAdded(event)
  }
}
