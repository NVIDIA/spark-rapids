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

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import com.nvidia.spark.rapids.tool.qualification.{RunningQualificationApp, RunningQualOutputWriter}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{CleanerListener, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class RunningQualificationEventProcessor(sparkConf: SparkConf) extends SparkListener with Logging {

  private val qualApp = new RunningQualificationApp(true)
  private val listener = qualApp.getEventListener
  private val isInited = new AtomicBoolean(false)
  private val maxSQLQueriesPerFile: Long =
    sparkConf.get("spark.rapids.qualification.output.numSQLQueriesPerFile", "100").toLong
  private var maxNumFiles: Long =
    sparkConf.get("spark.rapids.qualification.output.maxNumFiles", "10").toLong
  private var fileWriter: Option[RunningQualOutputWriter] = None
  private var currentFile = 0
  private var currentSQLQueriesWritten = 0
  private val filesWritten: Array[Seq[String]] = Array.fill[Seq[String]](maxNumFiles)(Seq.empty)

  class QualCleanerListener extends SparkListener with CleanerListener with Logging {
    def accumCleaned(accId: Long): Unit = {
      // remove the accums when no longer referenced
      qualApp.cleanupAccumId(accId)
    }
    def rddCleaned(rddId: Int): Unit = {}
    def shuffleCleaned(shuffleId: Int): Unit = {}
    def broadcastCleaned(broadcastId: Long): Unit = {}
    def checkpointCleaned(rddId: Long): Unit = {}
  }

  private val outputFileFromConfig = sparkConf.get("spark.rapids.qualification.outputDir", "")
  private lazy val appName = qualApp.appInfo.map(_.appName).getOrElse("")

  private def initListener(): Unit = {
    // install after startup when SparkContext is available
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.cleaner.foreach(x => x.attachListener(new QualCleanerListener()))
  }

  private def cleanupExistingFiles(id: Long): Unit = {
    val existingFiles = filesWritten(id)
    if (existingFiles.nonEmpty) {
      existingFiles.foreach { file =>
        val fs = FileSystem.get(file.toUri, hadoopConf)
        try {
          // delete recursive
          fs.delete(file, true)
        } catch {
          case _: AccessControlException =>
            logInfo(s"No permission to delete $file, ignoring.")
          case ioe: IOException =>
            logError(s"IOException in cleaning $file", ioe)
        }
      }
    }
  }

  private def getFileWriter(id: Long): Option[RunningQualOutputWriter] = {
    // get the running Hadoop Configuration so we pick up keys for accessing various
    // distributed filesystems
    val hadoopConf = SparkContext.getOrCreate(sparkConf).hadoopConfiguration
    if (outputFileFromConfig.nonEmpty) {
      cleanupExistingFiles(id)
      fileWriter.map(w => filesWritten(id) = w.getOutputFileNames)
      val writer = try {
        val runningWriter = new RunningQualOutputWriter(qualApp.appId, appName,
          outputFileFromConfig, Some(hadoopConf), s"_$id")
        runningWriter.getOutputFileNames
        Some(runningWriter)
      } catch {
        case NonFatal(e) =>
          logError("Error creating the RunningQualOutputWriter, output will not be" +
            s" saved to a file: ${e.getMessage}", e)
          None
      }
      writer.foreach(_.init())
      writer
    } else {
      None
    }
  }

  private def writeSQLDetails(sqlID: Long): Unit = {
    val (csvSQLInfo, textSQLInfo) = qualApp.getPerSqlTextAndCSVSummary(sqlID)
    if (currentSQLQueriesWritten >= maxNumFiles) {
      fileWriter = getFileWriter(sqlID)
    }
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
    // make sure we have attached the listener on the first job start
    if (!isInited.get()) {
      initListener()
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
