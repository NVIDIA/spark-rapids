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

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphNode}
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

  // The data source information
  var dataSourceInfo: ArrayBuffer[DataSourceCase] = ArrayBuffer[DataSourceCase]()

  private lazy val eventProcessor =  new QualEventProcessor()

  processEvents()

  override def processEvent(event: SparkListenerEvent): Unit = {
    eventProcessor.processAnyEvent(this, event)
  }

  def getPlanMetaWithSchema(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val childRes = planInfo.children.flatMap(getPlanMetaWithSchema(_))
    val keep = if (planInfo.metadata.contains("ReadSchema")) {
      childRes :+ planInfo
    } else {
      childRes
    }
    keep
  }

  private def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }

  // The ReadSchema metadata is only in the eventlog for DataSource V1 readers
  def checkMetadataForReadSchema(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = getPlanMetaWithSchema(planInfo)
    allMetaWithSchema.foreach { node =>
      val meta = node.metadata
      val readSchema = formatSchemaStr(meta.getOrElse("ReadSchema", ""))
      val allTypes = QualAppInfo.parseSchemaString(Some(readSchema)).values.toSet
      val schemaStr = allTypes.mkString(",")

      dataSourceInfo += DataSourceCase(sqlID,
        meta.getOrElse("Format", "unknown"),
        meta.getOrElse("Location", "unknown"),
        meta.getOrElse("PushedFilters", "unknown"),
        schemaStr,
        false
      )
    }
  }

  // This will find scans for DataSource V2
  def checkGraphNodeForBatchScan(sqlID: Long, node: SparkPlanGraphNode): Unit = {
    if (node.name.equals("BatchScan")) {
      val schemaTag = "ReadSchema: "
      val schema = if (node.desc.contains(schemaTag)) {
        val index = node.desc.indexOf(schemaTag)
        if (index != -1) {
          val subStr = node.desc.substring(index + schemaTag.size)
          val endIndex = subStr.indexOf(", ")
          if (endIndex != -1) {
            val schemaOnly = subStr.substring(0, endIndex)
            formatSchemaStr(schemaOnly)
          } else {
            ""
          }
        } else {
          ""
        }
      } else {
        ""
      }
      val locationTag = "Location:"
      val location = if (node.desc.contains(locationTag)) {
        val index = node.desc.indexOf(locationTag)
        val subStr = node.desc.substring(index)
        val endIndex = subStr.indexOf(", ")
        val location = subStr.substring(0, endIndex)
        location
      } else {
        "unknown"
      }
      val pushedFilterTag = "PushedFilters:"
      val pushedFilters = if (node.desc.contains(pushedFilterTag)) {
        val index = node.desc.indexOf(pushedFilterTag)
        val subStr = node.desc.substring(index)
        val endIndex = subStr.indexOf("]")
        val filters = subStr.substring(0, endIndex + 1)
        filters
      } else {
        "unknown"
      }
      val formatTag = "Format: "
      val fileFormat = if (node.desc.contains(formatTag)) {
        val index = node.desc.indexOf(formatTag)
        val subStr = node.desc.substring(index + formatTag.size)
        val endIndex = subStr.indexOf(", ")
        val format = subStr.substring(0, endIndex)
        format
      } else {
        "unknown"
      }
      val allTypes = QualAppInfo.parseSchemaString(Some(schema)).values.toSet
      val schemaStr = allTypes.mkString(",")

      dataSourceInfo += DataSourceCase(sqlID,
        fileFormat,
        location,
        pushedFilters,
        schemaStr,
        QualAppInfo.schemaIncomplete(schema)
      )
    }
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

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
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

  private def splitKeyValueSchema(schemaArr: Array[String]): Map[String, String] = {
    schemaArr.map { entry =>
      val keyValue = entry.split(":")
      if (keyValue.size == 2) {
        (keyValue(0) -> keyValue(1))
      } else {
        logWarning(s"Splitting key and value didn't result in key and value $entry")
        (entry -> "unknown")
      }
    }.toMap
  }

  def schemaIncomplete(schema: String): Boolean = {
    schema.endsWith("...")
  }

  def parseSchemaString(schemaOpt: Option[String]): Map[String, String] = {
    schemaOpt.map { schema =>
      val keyValues = schema.split(",")
      val validSchema = if (schemaIncomplete(keyValues.last)) {
        // the last schema element will be cutoff because it has the ...
        keyValues.dropRight(1)
      } else {
        keyValues
      }
      splitKeyValueSchema(validSchema)
    }.getOrElse(Map.empty[String, String])
  }
}
