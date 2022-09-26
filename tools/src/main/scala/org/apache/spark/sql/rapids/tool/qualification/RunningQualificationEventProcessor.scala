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

import java.util.concurrent.atomic.AtomicBoolean

import com.nvidia.spark.rapids.tool.qualification.RunningQualOutputWriter

import org.apache.spark.{CleanerListener, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class RunningQualificationEventProcessor(sparkConf: SparkConf) extends SparkListener with Logging {

  private val qualApp = new com.nvidia.spark.rapids.tool.qualification.RunningQualificationApp(true)
  private val listener = qualApp.getEventListener
  private val isInited = new AtomicBoolean(false)

  class QualCleanerListener extends SparkListener with CleanerListener with Logging {
    def accumCleaned(accId: Long): Unit = {
      // TODO - remove the accums
      logWarning("TOM ACCUMULATOR CLEANED")
      qualApp.cleanupAccumId(accId) // TODO write this function
    }
    def rddCleaned(rddId: Int): Unit = {}
    def shuffleCleaned(shuffleId: Int): Unit = {}
    def broadcastCleaned(broadcastId: Long): Unit = {}
    def checkpointCleaned(rddId: Long): Unit = {}
  }

  private val outputFileFromConfig = sparkConf.get("spark.rapids.qualification.outputDir", "")
  logWarning("Tom output file is: " + outputFileFromConfig)
  private lazy val appName = qualApp.appInfo.map(_.appName).getOrElse("")
  private lazy val fileWriter: Option[RunningQualOutputWriter] =
    if (outputFileFromConfig.nonEmpty) {
      logWarning("create output writer")
      val writer = Some(new RunningQualOutputWriter(qualApp.appId, appName, outputFileFromConfig))
      writer.foreach(_.init())
      writer
    } else {
      None
    }

  private val outputFuncApplicationDetails = () => {
    if (outputFileFromConfig.nonEmpty) {
      fileWriter.foreach { writer =>
        val appInfo = qualApp.getApplicationDetails
        appInfo.foreach(writer.writeApplicationReport(_))
      }
    } else {
      // TODO - fix
      logError("Qualification tool doesn't have an output location, no output written!")
    }
  }

  private def writeSQLDetails(sqlID: Long): Unit = {
    val (csvSQLInfo, textSQLInfo) = qualApp.getPerSqlTextAndCSVSummary(sqlID)
    if (outputFileFromConfig.nonEmpty) {
      fileWriter.foreach { writer =>
        writer.writePerSqlCSVReport(csvSQLInfo)
        logWarning(s"Done with SQL query ${sqlID} \n summary: $textSQLInfo")
        writer.writePerSqlTextReport(textSQLInfo)
      }
    } else {
      logWarning(textSQLInfo)
    }
    qualApp.cleanupSQL(sqlID)
  }

  private def close(): Unit = {
    fileWriter.foreach(_.close())
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
    if (!isInited.get()) {
      // install after startup when SparkContext is available
      val sc = SparkContext.getOrCreate(sparkConf)
      sc.cleaner.foreach(x => x.attachListener(new QualCleanerListener()))
      isInited.set(true)
    }
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
    // don't write application details because requires storing everything in memory to long
    // outputFuncApplicationDetails()
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
    listener.onOtherEvent(event)
    event match {
      case e: SparkListenerSQLExecutionStart =>
        logWarning("Tom otuput file is: " + outputFileFromConfig)
        logWarning("starting new SQL query")
      case e: SparkListenerSQLExecutionEnd =>
        writeSQLDetails(e.executionId)
      case _ =>
    }
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    listener.onResourceProfileAdded(event)
  }
}
