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

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo, EventLogPathProcessor}
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.util.{JsonProtocol, Utils}

class QualAppInfo(
    numOutputRows: Int,
    eventLogInfo: EventLogInfo,
    val index: Int) extends AppBase(numOutputRows, eventLogInfo) with Logging {

  var appId: String = ""
  var isPluginEnabled = false
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  // The duration of the SQL execution, in ms.
  val sqlDurationTime: HashMap[Long, Long] = HashMap.empty[Long, Long]

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoJobFailures: HashMap[Long, ArrayBuffer[Int]] = HashMap.empty[Long, ArrayBuffer[Int]]

  val problematicSQL: ArrayBuffer[ProblematicSQLCase] = ArrayBuffer[ProblematicSQLCase]()

  // SQL containing any Dataset operation
  // TODO - don't really need the DatasetSQLCase
  val sqlIDToDataSetCase: HashMap[Long, DatasetSQLCase] = HashMap[Long, DatasetSQLCase]()

  processEvents()

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    val estimatedResult =
      this.appEndTime match {
        case Some(t) => this.appEndTime
        case None =>
          if (lastSQLEndTime.isEmpty && lastJobEndTime.isEmpty) {
            None
          } else {
            logWarning("Application End Time is unknown, estimating based on" +
              " job and sql end times!")
            // estimate the app end with job or sql end times
            val sqlEndTime = if (this.lastSQLEndTime.isEmpty) 0L else this.lastSQLEndTime.get
            val jobEndTime = if (this.lastJobEndTime.isEmpty) 0L else lastJobEndTime.get
            val maxEndTime = math.max(sqlEndTime, jobEndTime)
            if (maxEndTime == 0) None else Some(maxEndTime)
          }
      }
    ProfileUtils.OptionLongMinusLong(estimatedResult, startTime)
  }

  private def calculatePercent(first: Long, total: Long): Double = {
    val firstDec = BigDecimal.decimal(first)
    val totalDec = BigDecimal.decimal(total)
    if (firstDec == 0 || totalDec == 0) {
      0.toDouble
    } else {
      val res = (firstDec * 100) / totalDec
      val resScale = res.setScale(2, BigDecimal.RoundingMode.HALF_UP)
      resScale.toDouble
    }
  }

  private def calculateScore(sqlDataframeDur: Long, appDuration: Long): Double = {
    calculatePercent(sqlDataframeDur, appDuration)
  }

  // if the sql contains a dataset, then duration for it is 0
  // for the sql dataframe duration
  private def calculateSqlDataframDuration: Long = {
    sqlDurationTime.filterNot { case (sqlID, dur) =>
        sqlIDToDataSetCase.contains(sqlID) || dur == -1
    }.values.sum
  }

  private def getPotentialProblems: String = {
    problematicSQL.map(_.reason).toSet.mkString(",")
  }

  private def getSQLDurationProblematic: Long = {
    problematicSQL.map { prob =>
      sqlDurationTime.getOrElse(prob.sqlID, 0L)
    }.sum
  }

  private def calculateCpuTimePercent: Double = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    val totalCpuTime = validSums.values.map { dur =>
      dur.executorCPUTime
    }.sum
    val totalRunTime = validSums.values.map { dur =>
      dur.executorRunTime
    }.sum
    calculatePercent(totalCpuTime, totalRunTime)
  }

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      // TODO - do we want 0 here or something else?
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframDuration
      val score = calculateScore(sqlDataframeDur, appDuration)
      val problems = getPotentialProblems
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      new QualificationSummaryInfo(info.appName, appId, score, problems,
        sqlDataframeDur, appDuration, executorCpuTimePercent, endDurationEstimated,
        sqlDurProblem, failedIds)
    }
  }

  /**
   * Functions to process all the events
   */
  // TODO - commonize
  def processEvents(): Unit = {
    val eventlog = eventLogInfo.eventLog

    logInfo("Parsing Event Log: " + eventlog.toString)

    // at this point all paths should be valid event logs or event log dirs
    // TODO - reuse Configuration
    val fs = eventlog.getFileSystem(new Configuration)
    var totalNumEvents = 0
    val eventsProcessor = new QualEventProcessor()
    val readerOpt = eventLogInfo match {
      case dblog: DatabricksEventLog =>
        Some(new DatabricksRollingEventLogFilesFileReader(fs, eventlog))
      case apachelog => EventLogFileReader(fs, eventlog)
    }

    if (readerOpt.isDefined) {
      val reader = readerOpt.get
      val logFiles = reader.listEventLogFiles
      logFiles.foreach { file =>
        Utils.tryWithResource(ToolUtils.openEventLogInternal(file.getPath, fs)) { in =>
          val lines = Source.fromInputStream(in)(Codec.UTF8).getLines().toList
          totalNumEvents += lines.size
          lines.foreach { line =>
            try {
              val event = JsonProtocol.sparkEventFromJson(parse(line))
              eventsProcessor.processAnyEvent(this, event)
            }
            catch {
              case e: ClassNotFoundException =>
                logWarning(s"ClassNotFoundException: ${e.getMessage}")
            }
          }
        }
      }
    } else {
      logError(s"Error getting reader for ${eventlog.getName}")
    }
    logInfo("Total number of events parsed: " + totalNumEvents)
  }

  def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      if (isDataSetPlan(node.desc)) {
        sqlIDToDataSetCase += (sqlID -> DatasetSQLCase(sqlID))
      }
      findPotentialIssues(node.desc).foreach { issues =>
        problematicSQL += ProblematicSQLCase(sqlID, issues)
      }
    }
  }

  // TODO - commonize
  def isDataSetPlan(desc: String): Boolean = {
    desc match {
      case l if l.matches(".*\\$Lambda\\$.*") => true
      case a if a.endsWith(".apply") => true
      case _ => false
    }
  }

  def findPotentialIssues(desc: String): Option[String] =  {
    desc match {
      case u if u.matches(".*UDF.*") => Some("UDF")
      case _ => None
    }
  }

}

case class QualApplicationInfo(
    appName: String,
    appId: Option[String],
    startTime: Long,
    sparkUser: String,
    endTime: Option[Long], // time in ms
    duration: Option[Long],
    endDurationEstimated: Boolean)

case class QualSQLExecutionInfo(
    sqlID: Long,
    startTime: Long,
    endTime: Option[Long],
    duration: Option[Long],
    durationStr: String,
    sqlQualDuration: Option[Long],
    hasDataset: Boolean,
    problematic: String = "")

case class QualificationSummaryInfo(
    appName: String,
    appId: String,
    score: Double,
    potentialProblems: String,
    sqlDataFrameDuration: Long,
    appDuration: Long,
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    sqlDurationForProblematic: Long,
    failedSQLIds: String) {

  private def stringIfempty(str: String): String = {
    if (str.isEmpty) {
      "\"\""
    } else {
      str
    }
  }

  def toCSV: String = {
    val probStr = stringIfempty(potentialProblems)
    val appIdStr = stringIfempty(appId)
    val appNameStr = stringIfempty(appName)
    val failedIds = stringIfempty(failedSQLIds)

    s"$appNameStr,$appIdStr,$score,$probStr,$sqlDataFrameDuration," +
      s"$appDuration,$executorCpuTimePercent," +
      s"$endDurationEstimated,$sqlDurationForProblematic,$failedIds"
  }
}

object QualAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      numRows: Int,
      startIndex: Int = 1): (Option[QualAppInfo], Int) = {
    var index: Int = startIndex
    var errorCode = 0
    val app = try {
        // This apps only contains 1 app in each loop.
        val app = new QualAppInfo(numRows, path, index)
        logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
        index += 1
        Some(app)
      } catch {
        case json: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON: $path")
          errorCode = 1
          None
        case il: IllegalArgumentException =>
          logWarning(s"Error parsing file: $path", il)
          errorCode = 2
          None
        case e: Exception =>
          // catch all exceptions and skip that file
          logWarning(s"Got unexpected exception processing file: $path", e)
          errorCode = 3
          None
      }
    (app, errorCode)
  }
}
