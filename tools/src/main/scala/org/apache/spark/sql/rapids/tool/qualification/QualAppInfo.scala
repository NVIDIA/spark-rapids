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
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}

class QualAppInfo(
    eventLogInfo: Option[EventLogInfo],
    hadoopConf: Option[Configuration] = None,
    pluginTypeChecker: Option[PluginTypeChecker],
    readScorePercent: Int)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  var appId: String = ""
  var isPluginEnabled = false
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  // The duration of the SQL execution, in ms.
  val sqlDurationTime: HashMap[Long, Long] = HashMap.empty[Long, Long]

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoJobFailures: HashMap[Long, ArrayBuffer[Int]] = HashMap.empty[Long, ArrayBuffer[Int]]

  val sqlIDtoProblematic: HashMap[Long, Set[String]] = HashMap[Long, Set[String]]()

  // SQL containing any Dataset operation
  val sqlIDToDataSetCase: HashSet[Long] = HashSet[Long]()

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()

  private lazy val eventProcessor =  new QualEventProcessor(this)

  def getEventListener: SparkListener = {
    eventProcessor
  }

  processEvents()

  override def processEvent(event: SparkListenerEvent): Boolean = {
    eventProcessor.processAnyEvent(event)
    false
  }

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    if (startTime > 0) {
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
    } else {
      None
    }
  }

  /**
   * The score starts out based on the over all task time spent in SQL dataframe
   * operations and then can only decrease from there based on if it has operations not
   * supported by the plugin.
   */
  private def calculateScore(readScoreRatio: Double, sqlDataframeTaskDuration: Long): Double = {
    // the readScorePercent is an integer representation of percent
    val ratioForReadScore = readScorePercent / 100.0
    val ratioForRestOfScore = 1.0 - ratioForReadScore
    // get the part of the duration that will apply to the read score
    val partForReadScore = sqlDataframeTaskDuration * ratioForReadScore
    // calculate the score for the read part based on the read format score
    val readScore = partForReadScore * readScoreRatio
    // get the rest of the duration that doesn't apply to the read score
    val scoreRestPart = sqlDataframeTaskDuration * ratioForRestOfScore
    scoreRestPart + readScore
  }

  // if the SQL contains a dataset, then duration for it is 0
  // for the SQL dataframe duration
  private def calculateSqlDataframeDuration: Long = {
    sqlDurationTime.filterNot { case (sqlID, dur) =>
        sqlIDToDataSetCase.contains(sqlID) || dur == -1
    }.values.sum
  }

  private def probNotDataset: HashMap[Long, Set[String]] = {
    sqlIDtoProblematic.filterNot { case (sqlID, _) => sqlIDToDataSetCase.contains(sqlID) }
  }

  // The total task time for all tasks that ran during SQL dataframe
  // operations.  if the SQL contains a dataset, it isn't counted.
  private def calculateTaskDataframeDuration: Long = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    validSums.values.map(dur => dur.totalTaskDuration).sum
  }

  private def getPotentialProblemsForDf: String = {
    probNotDataset.values.flatten.toSet.mkString(":")
  }

  // This is to append potential issues such as UDF, decimal type determined from
  // SparkGraphPlan Node description and nested complex type determined from reading the
  // event logs. If there are any complex nested types, then `NESTED COMPLEX TYPE` is mentioned
  // in the `Potential Problems` section in the csv file. Section `Unsupported Nested Complex
  // Types` has information on the exact nested complex types which are not supported for a
  // particular application.
  private def getAllPotentialProblems(dFPotentialProb: String, nestedComplex: String): String = {
    val nestedComplexType = if (nestedComplex.nonEmpty) "NESTED COMPLEX TYPE" else ""
    val result = if (dFPotentialProb.nonEmpty) {
      if (nestedComplex.nonEmpty) {
        s"$dFPotentialProb:$nestedComplexType"
      } else {
        dFPotentialProb
      }
    } else {
      nestedComplexType
    }
    result
  }

  private def getSQLDurationProblematic: Long = {
    probNotDataset.keys.map { sqlId =>
      sqlDurationTime.getOrElse(sqlId, 0L)
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

  private def getAllReadFileFormats: String = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }.mkString(":")
  }
  
  // For the read score we look at all the read formats and datatypes for each
  // format and for each read give it a value 0.0 - 1.0 depending on whether
  // the format is supported and if the data types are supported. We then sum
  // those together and divide by the total number.  So if none of the data types
  // are supported, the score would be 0.0 and if all formats and datatypes are
  // supported the score would be 1.0.
  private def calculateReadScoreRatio(): Double = {
    pluginTypeChecker.map { checker =>
      if (dataSourceInfo.size == 0) {
        1.0
      } else {
        val readFormatSum = dataSourceInfo.map { ds =>
          val (readScore, nsTypes) = checker.scoreReadDataTypes(ds.format, ds.schema)
          if (nsTypes.nonEmpty) {
            val currentFormat = notSupportFormatAndTypes.get(ds.format).getOrElse(Set.empty[String])
            notSupportFormatAndTypes(ds.format) = (currentFormat ++ nsTypes)
          }
          readScore
        }.sum
        readFormatSum / dataSourceInfo.size
      }
    }.getOrElse(1.0)
  }

  def reportComplexTypes: (String, String) = {
    if (dataSourceInfo.size != 0) {
      val schema = dataSourceInfo.map { ds => ds.schema }
      QualAppInfo.parseReadSchemaForNestedTypes(schema)
    } else {
      ("", "")
    }
  }

  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframeDuration
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val readScoreRatio = calculateReadScoreRatio
      val sqlDataframeTaskDuration = calculateTaskDataframeDuration
      val readScoreHumanPercent = 100 * readScoreRatio
      val readScoreHumanPercentRounded = f"${readScoreHumanPercent}%1.2f".toDouble
      val score = calculateScore(readScoreRatio, sqlDataframeTaskDuration)
      val scoreRounded = f"${score}%1.2f".toDouble
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case(format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.mkString(";")
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      new QualificationSummaryInfo(info.appName, appId, scoreRounded, problems,
        sqlDataframeDur, sqlDataframeTaskDuration, appDuration, executorCpuTimePercent,
        endDurationEstimated, sqlDurProblem, failedIds, readScorePercent,
        readScoreHumanPercentRounded, notSupportFormatAndTypesString,
        getAllReadFileFormats, writeFormat, allComplexTypes, nestedComplexTypes)
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
      val issues = findPotentialIssues(node.desc)
      if (issues.nonEmpty) {
        val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
        sqlIDtoProblematic(sqlID) = existingIssues ++ issues
      }
      // Get the write data format
      if (node.name.contains("InsertIntoHadoopFsRelationCommand")) {
        val writeFormat = node.desc.split(",")(2)
        writeDataFormat += writeFormat
      }
    }
  }

  def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): String = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.map { checker =>
      checker.isWriteFormatsupported(writeFormat)
    }.getOrElse(ArrayBuffer[String]())

    unSupportedWriteFormat.distinct.mkString(";").toUpperCase
  }
}

class StageTaskQualificationSummary(
    val stageId: Int,
    val stageAttemptId: Int,
    var executorRunTime: Long,
    var executorCPUTime: Long,
    var totalTaskDuration: Long)

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
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    sqlDurationForProblematic: Long,
    failedSQLIds: String,
    readScorePercent: Int,
    readFileFormatScore: Double,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String)

object QualAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: Option[PluginTypeChecker],
      readScorePercent: Int): Option[QualAppInfo] = {
    val app = try {
        val app = new QualAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker,
          readScorePercent)
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

  def parseReadSchemaForNestedTypes(schema: ArrayBuffer[String]): (String, String) = {
    val tempStringBuilder = new StringBuilder()
    val individualSchema: ArrayBuffer[String] = new ArrayBuffer()
    var angleBracketsCount = 0
    var parenthesesCount = 0
    val distinctSchema = schema.distinct.filter(_.nonEmpty).mkString(",")

    // Get the nested types i.e everything between < >
    for (char <- distinctSchema) {
      char match {
        case '<' => angleBracketsCount += 1
        case '>' => angleBracketsCount -= 1
        // If the schema has decimals, Example decimal(6,2) then we have to make sure it has both
        // opening and closing parentheses(unless the string is incomplete due to V2 reader).
        case '(' => parenthesesCount += 1
        case ')' => parenthesesCount -= 1
        case _ =>
      }
      if (angleBracketsCount == 0 && parenthesesCount == 0 && char.equals(',')) {
        individualSchema += tempStringBuilder.toString
        tempStringBuilder.setLength(0)
      } else {
        tempStringBuilder.append(char);
      }
    }
    if (!tempStringBuilder.isEmpty) {
      individualSchema += tempStringBuilder.toString
    }

    // If DataSource V2 is used, then Schema may be incomplete with ... appended at the end.
    // We determine complex types and nested complex types until ...
    val incompleteSchema = individualSchema.filter(x => x.contains("..."))
    val completeSchema = individualSchema.filterNot(x => x.contains("..."))

    // Check if it has types
    val incompleteTypes = incompleteSchema.map { x =>
      if (x.contains("...") && x.contains(":")) {
        val schemaTypes = x.split(":", 2)
        if (schemaTypes.size == 2) {
          val partialSchema = schemaTypes(1).split("\\.\\.\\.")
          if (partialSchema.size == 1) {
            partialSchema(0)
          } else {
            ""
          }
        } else {
          ""
        }
      } else {
        ""
      }
    }
    // Omit columnName and get only schemas
    val completeTypes = completeSchema.map { x =>
      val schemaTypes = x.split(":", 2)
      if (schemaTypes.size == 2) {
        schemaTypes(1)
      } else {
        ""
      }
    }
    val schemaTypes = completeTypes ++ incompleteTypes

    // Filter only complex types.
    // Example: array<string>, array<struct<string, string>>
    val complexTypes = schemaTypes.filter(x =>
      x.startsWith("array<") || x.startsWith("map<") || x.startsWith("struct<"))

    // Determine nested complex types from complex types
    // Example: array<struct<string, string>> is nested complex type.
    val nestedComplexTypes = complexTypes.filter(complexType => {
      val startIndex = complexType.indexOf('<')
      val closedBracket = complexType.lastIndexOf('>')
      // If String is incomplete due to dsv2, then '>' may not be present. In that case traverse
      // until length of the incomplete string
      val lastIndex = if (closedBracket == -1) {
        complexType.length - 1
      } else {
        closedBracket
      }
      val string = complexType.substring(startIndex, lastIndex + 1)
      string.contains("array<") || string.contains("struct<") || string.contains("map<")
    })

    // Since it is saved as csv, replace commas with ;
    val complexTypesResult = complexTypes.filter(_.nonEmpty).mkString(";").replace(",", ";")
    val nestedComplexTypesResult = nestedComplexTypes.filter(
      _.nonEmpty).mkString(";").replace(",", ";")

    (complexTypesResult, nestedComplexTypesResult)
  }
}
