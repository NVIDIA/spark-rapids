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

import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}
import org.apache.spark.util.{JsonProtocol, Utils}
import org.json4s.jackson.JsonMethods.parse

class QualAppInfo(
    numOutputRows: Int,
    sparkSession: SparkSession,
    eventLogInfo: EventLogInfo,
    val index: Int) extends AppBase(numOutputRows, sparkSession, eventLogInfo) with Logging {

  var appId: String = ""
  var isPluginEnabled = false
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  // The duration of the SQL execution, in ms.
  // val sqlEndTime: HashMap[Long, Long] = HashMap.empty[Long, Long]
  val sqlDurationTime: HashMap[Long, Long] = HashMap.empty[Long, Long]


  // From SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate
  // sqlPlan stores HashMap (sqlID <-> SparkPlanInfo)
  // val sqlPlan: HashMap[Long, SparkPlanInfo] = HashMap.empty[Long, SparkPlanInfo]

  // TODO - do I need attempt id as well?
  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]]
  val sqlIdToStageId: HashMap[Long, ArrayBuffer[Int]] = HashMap.empty[Long, ArrayBuffer[Int]]
  val stageIdToAttempts: HashMap[Int, ArrayBuffer[Int]] = HashMap.empty[Int, ArrayBuffer[Int]]

  // this is used to aggregate metrics for qualification to speed up processing and
  // minimize memory usage
  var sqlIDToTaskQualificationEnd: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val problematicSQL: ArrayBuffer[ProblematicSQLCase] = ArrayBuffer[ProblematicSQLCase]()

  // SQL containing any Dataset operation
  val datasetSQL: ArrayBuffer[DatasetSQLCase] = ArrayBuffer[DatasetSQLCase]()

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

  private def calculatePercent(first: Long, total: Long): Float = {
    val firstDec = BigDecimal.decimal(first)
    val totalDec = BigDecimal.decimal(total)
    val res = (firstDec * 100) / totalDec
    val resScale = res.setScale(2, BigDecimal.RoundingMode.HALF_UP)
    resScale.toFloat
  }

  private def calculateScore(sqlDataframeDur: Long, appDuration: Long): Float = {
    calculatePercent(sqlDataframeDur, appDuration)
  }

  // if the sql contains a dataset, then duration for it is 0
  // for the sql dataframe duration
  private def calculateSqlDataframDuration: Long = {
    sqlStart.map { info =>
      val thisEndTime = sqlEndTime.get(info.sqlID)
      val durationResult = ProfileUtils.OptionLongMinusLong(thisEndTime, info.startTime)
      if (datasetSQL.exists(_.sqlID == info.sqlID)) {
        0L
      } else {
        durationResult.getOrElse(0)
      }
    }.sum
  }

  private def getPotentialProblems: String = {
    problematicSQL.map(_.reason).toSet.mkString(",")
  }

  private def calculateCpuTimePercent: Float = {
    val totalCpuTime = sqlIDToTaskQualificationEnd.values.map { dur =>
      dur.executorCPUTime
    }.sum
    val totalRunTime = sqlIDToTaskQualificationEnd.values.map { dur =>
      dur.executorRunTime
    }.sum
    calculatePercent(totalCpuTime, totalRunTime)
  }

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      // TODO - do we want 0 here or something else?
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0)
      val sqlDataframeDur = calculateSqlDataframDuration
      val score = calculateScore(sqlDataframeDur, appDuration)
      val problems = getPotentialProblems
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = 0
      new QualificationSummaryInfo(info.appName, appId, score, appDuration,
        sqlDataframeDur, problems, executorCpuTimePercent, endDurationEstimated, sqlDurProblem)
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
    val fs = eventlog.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
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
        datasetSQL += DatasetSQLCase(sqlID)
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
    score: Float,
    appDuration: Long,
    sqlDataFrameDuration: Long,
    problems: String,
    executorCpuTimePercent: Float,
    endDurationEstimated: Boolean,
    sqlDurationForProblematic: Long) {
  override def toString(): String = {

  }

  def toCSV: String = {

  }

  /*
  Application ID
  Total runtime in seconds
  Total time spent in SQL execution
  Total time spent in SQL execution for queries with “problematic” operators
  */
  def headerText: String = {
    "|App ID                 |SQL Dataframe Duration|App Duration|SQL with problematic operators Duration|"
  }

  def headerCSV: String = {
    "App Name,App ID,Score,Potential Problems,SQL Dataframe Duration," +
      "App Duration,Executor CPU Time Percent,App Duration Estimated"
  }
}

object QualAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      numRows: Int,
      sparkSession: SparkSession,
      startIndex: Int = 1): (Option[QualAppInfo], Int) = {
    var index: Int = startIndex
    var errorCode = 0
    val app = try {
        // This apps only contains 1 app in each loop.
        val app = new QualAppInfo(numRows, sparkSession, path, index)
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
