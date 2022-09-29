/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{CleanerListener, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

/**
 * This is a Spark Listener that can be used to determine if the SQL queries in
 * the running application are a good fit to try with the Rapids Accelerator for Spark.
 *
 * This only supports output on a per sql query basis. It supports writing to
 * local filesystem and most distribute filesystems and blob stores supported
 * by Hadoop.
 *
 * It can be run in your Spark application by installing it as a listener:
 * set spark.extraListeners to
 * org.apache.spark.sql.rapids.tool.qualification.RunningQualificationEventProcessor
 * and including the tools jar rapids-4-spark-tools_2.12-<version>.jar when you start
 * the Spark application.
 *
 * The user should specify the output directory if they want the output to go to separate
 * files, otherwise it will go to the Spark driver log:
 *  - spark.rapids.qualification.outputDir
 *
 * By default, this will output results for 10 SQL queries per file and will
 * keep 100 files. This behavior is because many blob stores don't show files until
 * they are full written so you wouldn't be able to see the results for a running
 * application until it finishes the number of SQL queries per file. This behavior
 * can be configured with the following configs.
 *  - spark.rapids.qualification.output.numSQLQueriesPerFile - default 10
 *  - spark.rapids.qualification.output.maxNumFiles - default 100
 *
 * @param sparkConf Spark Configuration used to get configs used by this listener
 */
class RunningQualificationEventProcessor(sparkConf: SparkConf) extends SparkListener with Logging {

  private val qualApp = new RunningQualificationApp(true)
  private val listener = qualApp.getEventListener
  private val isInited = new AtomicBoolean(false)
  private val maxSQLQueriesPerFile: Long =
    sparkConf.get("spark.rapids.qualification.output.numSQLQueriesPerFile", "10").toLong
  private val maxNumFiles: Int =
    sparkConf.get("spark.rapids.qualification.output.maxNumFiles", "100").toInt
  private val outputFileFromConfig = sparkConf.get("spark.rapids.qualification.outputDir", "")
  private lazy val appName = qualApp.appInfo.map(_.appName).getOrElse("")
  private var fileWriter: Option[RunningQualOutputWriter] = None
  private var currentFileNum = 0
  private var currentSQLQueriesWritten = 0
  private val filesWritten = Array.fill[Seq[Path]](maxNumFiles)(Seq.empty)
  private lazy val txtHeader = qualApp.getPerSqlTextHeader

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

  private def initListener(): Unit = {
    // install after startup when SparkContext is available
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.cleaner.foreach(x => x.attachListener(new QualCleanerListener()))
  }

  private def cleanupExistingFiles(id: Int, hadoopConf: Configuration): Unit = {
    filesWritten(id).foreach { file =>
      logWarning(s"Going to remove file: $file")
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

  private def updateFileWriter(): Unit = {
    // get the running Hadoop Configuration so we pick up keys for accessing various
    // distributed filesystems
    val hadoopConf = SparkContext.getOrCreate(sparkConf).hadoopConfiguration
    if (outputFileFromConfig.nonEmpty) {
      if (fileWriter.isDefined) {
        if (currentFileNum >= maxNumFiles - 1) {
          currentFileNum = 0
        } else {
          currentFileNum += 1
        }
        // if the current slot already had files written, remove those before
        // writing a new file
        cleanupExistingFiles(currentFileNum, hadoopConf)
      }
      val writer = try {
        logDebug(s"Creating new file output writer for id: $currentFileNum")
        val runningWriter = new RunningQualOutputWriter(qualApp.appId, appName,
          outputFileFromConfig, Some(hadoopConf), currentFileNum.toString)
        filesWritten(currentFileNum) = runningWriter.getOutputFileNames
        Some(runningWriter)
      } catch {
        case NonFatal(e) =>
          logError("Error creating the RunningQualOutputWriter, output will not be" +
            s" saved to a file, error: ${e.getMessage}", e)
          None
      }
      writer.foreach(_.init())
      fileWriter.foreach(_.close())
      fileWriter = writer
    }
  }

  private def writeSQLDetails(sqlID: Long): Unit = {
    val (csvSQLInfo, textSQLInfo) = qualApp.getPerSqlTextAndCSVSummary(sqlID)
    if (outputFileFromConfig.nonEmpty) {
      // once file has gotten enough SQL queries, switch it to new file
      if (currentSQLQueriesWritten >= maxSQLQueriesPerFile || !fileWriter.isDefined) {
        updateFileWriter()
        currentSQLQueriesWritten = 0
      }
      fileWriter.foreach { writer =>
        logDebug(s"Done with SQL query ${sqlID} summary:: \n $textSQLInfo")
        writer.writePerSqlCSVReport(csvSQLInfo)
        writer.writePerSqlTextReport(textSQLInfo)
        currentSQLQueriesWritten += 1
      }
    } else {
      // file writer isn't configured so just output to driver logs, us warning
      // level so it comes out when using the shell
      logWarning("\n" + txtHeader + textSQLInfo)
    }
    qualApp.cleanupSQL(sqlID)
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
    fileWriter.foreach(_.close())
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
        logDebug("Starting new SQL query")
      case e: SparkListenerSQLExecutionEnd =>
        writeSQLDetails(e.executionId)
      case _ =>
    }
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    listener.onResourceProfileAdded(event)
  }
}
