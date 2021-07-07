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

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}

class QualAppInfo(
    numOutputRows: Int,
    eventLogInfo: EventLogInfo,
    hadoopConf: Configuration,
    pluginTypeChecker: PluginTypeChecker,
    readScorePercent: Int)
  extends AppBase(numOutputRows, eventLogInfo, hadoopConf) with Logging {

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
  val sqlIDToDataSetCase: HashSet[Long] = HashSet[Long]()

  private lazy val eventProcessor =  new QualEventProcessor()

  processEvents()

  override def processEvent(event: SparkListenerEvent): Unit = {
    eventProcessor.processAnyEvent(this, event)
  }

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    val estimatedResult =
      this.appEndTime match {
        case Some(t) => this.appEndTime
        case None =>
          if (lastSQLEndTime.isEmpty && lastJobEndTime.isEmpty) {
            None
          } else {
            logWarning(s"Application End Time is unknown for $appId, estimating based on" +
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

  /**
   * The score starts out based on the over all time spent in SQL operations
   * and then can only decrease from there based on if it has operations not
   * supported by the plugin.
   *
   * @param sqlDataframeDur Time spent doing SQL dataframe operations.
   * @param appDuration Application duration
   * @param readFormatScore Score from read format and datatypes
   * @return the final score
   */
  private def calculateScore(sqlDataframeDur: Long, appDuration: Long,
      readFormatScore: Double): Double = {
    val durationScore = ToolUtils.calculateDurationPercent(sqlDataframeDur, appDuration)
    // the readScorePercent is an integer representation of percent, change to actual for
    // multiplication
    val percentForReadScoreDecimal = readScorePercent / 100.0
    val percentForRestOfScore = 1.0 - percentForReadScoreDecimal
    val partForReadScore = durationScore * percentForReadScoreDecimal
    val readScore = ToolUtils.calculatePercentRounded(partForReadScore, (readFormatScore / 100.0))
    val scoreRestPart = ToolUtils.calculatePercentRounded(durationScore, percentForRestOfScore)
    val finalScore = scoreRestPart + readScore
    logWarning(s"final score is $finalScore, before dur: $durationScore and " +
      s"$readScorePercent% and read: $readScorePercent")
    finalScore
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
    ToolUtils.calculateDurationPercent(totalCpuTime, totalRunTime)
  }

  case class SupportedTypesDS(format: String, direction: String,
      arraySup: String, binarySup: String,
      booleanSup: String, byteSup: String, calSup: String, dateSup: String,
      decimalSup: String, doubleSup: String,
      floatSup: String, intSup: String, longSup: String,
      mapSup: String, nullSup: String, shortSup: String,
      stringSup: String, structSup: String, timestampSup: String, udtSup:String)

  private def getAllReadFileFormats: String = {
    dataSourceInfo.map { ds =>
      val typesStr = getReadFileFormatTypes(ds)
      s"${ds.format.toLowerCase()}[$typesStr]"
    }.mkString(":")
  }

  private def getReadFileFormatTypes(ds: DataSourceCase): String = {
    val retTypes = ds.schema.toLowerCase.split(",").toSet.mkString(":")
    if (ds.schemaIncomplete) {
      s"$retTypes:INCOMPLETE"
    } else {
      retTypes
    }
  }

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframDuration
      val problems = getPotentialProblems
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val readFormatSum = dataSourceInfo.map { ds =>
        pluginTypeChecker.checkReadDataTypesSupported(ds.format, ds.schema)
      }.sum
      val readFormatScore = ToolUtils.calculateDurationPercent(readFormatSum, dataSourceInfo.size)
      val score = calculateScore(sqlDataframeDur, appDuration, readFormatScore)
      logWarning("read formats scores: " + readFormatScore)
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      new QualificationSummaryInfo(info.appName, appId, score, problems,
        sqlDataframeDur, appDuration, executorCpuTimePercent, endDurationEstimated,
        sqlDurProblem, failedIds, readFormatScore, getAllReadFileFormats)
    }
  }

  def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    checkMetadataForReadSchema(sqlID, planInfo)
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      checkGraphNodeForBatchScan(sqlID, node)
      if (isDataSetPlan(node.desc)) {
        sqlIDToDataSetCase += sqlID
      }
      findPotentialIssues(node.desc).foreach { issues =>
        problematicSQL += ProblematicSQLCase(sqlID, issues)
      }
    }
  }
}

class StageTaskQualificationSummary(
    val stageId: Int,
    val stageAttemptId: Int,
    var executorRunTime: Long,
    var executorCPUTime: Long)

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
    failedSQLIds: String,
    readFileFormatScore: Double,
    readFileFormats: String)

object QualAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration,
      pluginTypeChecker: PluginTypeChecker,
      readScorePercent: Int): Option[QualAppInfo] = {
    val app = try {
        val app = new QualAppInfo(numRows, path, hadoopConf, pluginTypeChecker, readScorePercent)
        logInfo(s"${path.eventLog.toString} has App: ${app.appId}")
        Some(app)
      } catch {
        case json: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON: ${path.eventLog.toString}")
          None
        case il: IllegalArgumentException =>
          logWarning(s"Error parsing file: ${path.eventLog.toString}", il)
          None
        case e: Exception =>
          // catch all exceptions and skip that file
          logWarning(s"Got unexpected exception processing file: ${path.eventLog.toString}", e)
          None
      }
    app
  }
}
