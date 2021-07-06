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

import java.io.File

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.io.Source

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}

class QualAppInfo(
    numOutputRows: Int,
    eventLogInfo: EventLogInfo,
    hadoopConf: Configuration)
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

  private def calculateScore(sqlDataframeDur: Long, appDuration: Long): Double = {
    ToolUtils.calculatePercent(sqlDataframeDur, appDuration)
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
    ToolUtils.calculatePercent(totalCpuTime, totalRunTime)
  }

  case class SupportedTypesDS(format: String, direction: String,
      arraySup: String, binarySup: String,
      booleanSup: String, byteSup: String, calSup: String, dateSup: String,
      decimalSup: String, doubleSup: String,
      floatSup: String, intSup: String, longSup: String,
      mapSup: String, nullSup: String, shortSup: String,
      stringSup: String, structSup: String, timestampSup: String, udtSup:String)

  private def checkDataTypesSupported: Boolean = {
    logWarning("checking datatypes supported!")
    val file = "supportedDataSource.csv"
    // val supportedSources = new File(getClass.getClassLoader.getResource(file).getFile)
    val source = Source.fromResource(file)
    val dotFileStr = source.getLines().toSeq
    val allSupportedsources = HashMap.empty[String, Map[String, String]]
    // Format,Direction,ARRAY,BINARY,BOOLEAN,BYTE,CALENDAR,DATE,DECIMAL,DOUBLE,FLOAT,
    // INT,LONG,MAP,NULL,SHORT,STRING,STRUCT,TIMESTAMP,UDT
    val headers = dotFileStr.head.split(",").map(_.toLowerCase)

    dotFileStr.tail.foreach { line =>
      val cols = line.split(",")
      if (headers.size != cols.size) {
        logError("somethign went wrong, header is not same size as cols")
      }
      val supportedType = cols(0).toLowerCase
      val direction = cols(1)
      val res = headers.drop(2).zip(cols.drop(2)).toMap
      allSupportedsources(supportedType) = res
    }



    source.close()
    if (dataSourceInfo.nonEmpty) {
      dataSourceInfo.foreach { ds =>
        logWarning("data source is: " + ds.format + " rest: "  + ds)
        if (allSupportedsources.contains(ds.format.toLowerCase)) {
          logWarning(s"data source format ${ds.format} is supported by plugin")
          val readSchema = ds.schema.split(",").map(_.toLowerCase)
          readSchema.foreach { typeRead =>
            // TODO - need to add array/map/etc
            val realType = typeRead match {
              case "bigint" => "long"
              case "smallint" => "short"
              case "integer" => "int"
              case "tinyint" => "byte"
              case "real" => "float"
              case "dec" | "numeric" => "decimal"
              case "interval" => "calendar"
              case other => other
            }
            if (allSupportedsources(ds.format.toLowerCase).contains(realType)) {
              val supString = allSupportedsources(ds.format.toLowerCase).getOrElse(realType, "")
              // S,S,S,S,S,S,S,S,S*,S,NS,NA,NS,NA,NA,NA,NA,NA
              logWarning(s"type is : $typeRead supported is: $supString")
              supString match {
                case "S" => logWarning("supported")
                case "S*" => logWarning("s*")
                case "PS" => logWarning("s*")
                case "PS*" => logWarning("s*")
                case "NS" => logWarning("ns")
                case "NA" => logWarning("na")
                case unknown => logWarning(s"unknown type $unknown for type: $typeRead")
              }
            } else {
              logWarning(s"type $realType not supported")
            }
          }
        } else {
          logWarning(s"data source ${ds.format} is not supported!")
        }
      }
    }
    true
  }

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframDuration
      val score = calculateScore(sqlDataframeDur, appDuration)
      val problems = getPotentialProblems
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val dataTypesSupported = checkDataTypesSupported
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      new QualificationSummaryInfo(info.appName, appId, score, problems,
        sqlDataframeDur, appDuration, executorCpuTimePercent, endDurationEstimated,
        sqlDurProblem, failedIds)
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
    failedSQLIds: String)

object QualAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      numRows: Int,
      hadoopConf: Configuration): Option[QualAppInfo] = {
    val app = try {
        val app = new QualAppInfo(numRows, path, hadoopConf)
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
