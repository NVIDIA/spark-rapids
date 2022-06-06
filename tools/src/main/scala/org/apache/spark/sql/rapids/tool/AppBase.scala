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

package org.apache.spark.sql.rapids.tool

import java.io.InputStream
import java.util.zip.GZIPInputStream

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import com.nvidia.spark.rapids.tool.planparser.ReadParser
import com.nvidia.spark.rapids.tool.profiling.{DataSourceCase, DriverAccumCase, JobInfoClass, SQLExecutionInfoClass, StageInfoClass, TaskStageAccumCase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerEvent, StageInfo}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.util.{JsonProtocol, Utils}

abstract class AppBase(
    val eventLogInfo: Option[EventLogInfo],
    val hadoopConf: Option[Configuration]) extends Logging {

  var sparkVersion: String = ""
  var appEndTime: Option[Long] = None
  // The data source information
  val dataSourceInfo: ArrayBuffer[DataSourceCase] = ArrayBuffer[DataSourceCase]()

  // jobId to job info
  val jobIdToInfo = new HashMap[Int, JobInfoClass]()
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]

  // SQL containing any Dataset operation or RDD to DataSet/DataFrame operation
  val sqlIDToDataSetOrRDDCase: HashSet[Long] = HashSet[Long]()
  val sqlIDtoProblematic: HashMap[Long, Set[String]] = HashMap[Long, Set[String]]()
  // sqlId to sql info
  val sqlIdToInfo = new HashMap[Long, SQLExecutionInfoClass]()

  // accum id to task stage accum info
  var taskStageAccumMap: HashMap[Long, ArrayBuffer[TaskStageAccumCase]] =
    HashMap[Long, ArrayBuffer[TaskStageAccumCase]]()

  val stageIdToInfo: HashMap[(Int, Int), StageInfoClass] = new HashMap[(Int, Int), StageInfoClass]()
  val stageAccumulators: HashMap[Int, Seq[Long]] = new HashMap[Int, Seq[Long]]()

  var driverAccumMap: HashMap[Long, ArrayBuffer[DriverAccumCase]] =
    HashMap[Long, ArrayBuffer[DriverAccumCase]]()

  var gpuMode = false

  def getOrCreateStage(info: StageInfo): StageInfoClass = {
    val stage = stageIdToInfo.getOrElseUpdate((info.stageId, info.attemptNumber()),
      new StageInfoClass(info))
    stage
  }

  def processEvent(event: SparkListenerEvent): Boolean

  private def openEventLogInternal(log: Path, fs: FileSystem): InputStream = {
    EventLogFileWriter.codecName(log) match {
      case c if (c.isDefined && c.get.equals("gz")) =>
        val in = fs.open(log)
        try {
          new GZIPInputStream(in)
        } catch {
          case e: Throwable =>
            in.close()
            throw e
        }
      case _ => EventLogFileReader.openEventLog(log, fs)
    }
  }

  /**
   * Functions to process all the events
   */
  protected def processEvents(): Unit = {
    eventLogInfo match {
      case Some(eventLog) =>
        val eventLogPath = eventLog.eventLog
        logInfo("Parsing Event Log: " + eventLogPath.toString)

        // at this point all paths should be valid event logs or event log dirs
        val hconf = hadoopConf.getOrElse(new Configuration())
        val fs = eventLogPath.getFileSystem(hconf)
        var totalNumEvents = 0
        val readerOpt = eventLog match {
          case dblog: DatabricksEventLog =>
            Some(new DatabricksRollingEventLogFilesFileReader(fs, eventLogPath))
          case apachelog => EventLogFileReader(fs, eventLogPath)
        }

        if (readerOpt.isDefined) {
          val reader = readerOpt.get
          val logFiles = reader.listEventLogFiles
          logFiles.foreach { file =>
            Utils.tryWithResource(openEventLogInternal(file.getPath, fs)) { in =>
              val lines = Source.fromInputStream(in)(Codec.UTF8).getLines().toList
              // Using find as foreach with conditional to exit early if we are done.
              // Do NOT use a while loop as it is much much slower.
              lines.find { line =>
                val isDone = try {
                  totalNumEvents += 1
                  val event = JsonProtocol.sparkEventFromJson(parse(line))
                  processEvent(event)
                }
                catch {
                  case e: ClassNotFoundException =>
                    // swallow any messages about this class since likely using spark version
                    // before 3.1
                    if (!e.getMessage.contains("SparkListenerResourceProfileAdded")) {
                      logWarning(s"ClassNotFoundException: ${e.getMessage}")
                    }
                    false
                }
                isDone
              }
            }
          }
        } else {
          logError(s"Error getting reader for ${eventLogPath.getName}")
        }
        logInfo(s"Total number of events parsed: $totalNumEvents for ${eventLogPath.toString}")
      case None => logInfo("Streaming events to application")
    }
  }

  def isDataSetOrRDDPlan(desc: String): Boolean = {
    desc match {
      case l if l.matches(".*\\$Lambda\\$.*") => true
      case a if a.endsWith(".apply") => true
      case r if r.matches(".*SerializeFromObject.*") => true
      case _ => false
    }
  }

  private val UDFRegex = ".*UDF.*"
  private val UDFKeywords = Map(UDFRegex -> "UDF")

  def containsUDF(desc: String): Boolean = {
    desc.matches(UDFRegex)
  }

  protected def findPotentialIssues(desc: String): Set[String] =  {
    val potentialIssuesRegexs = UDFKeywords
    val issues = potentialIssuesRegexs.filterKeys(desc.matches(_))
    issues.values.toSet
  }

  def getPlanMetaWithSchema(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val childRes = planInfo.children.flatMap(getPlanMetaWithSchema(_))
    if (planInfo.metadata != null && planInfo.metadata.contains("ReadSchema")) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }

  // The ReadSchema metadata is only in the eventlog for DataSource V1 readers
  protected def checkMetadataForReadSchema(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = getPlanMetaWithSchema(planInfo)
    allMetaWithSchema.foreach { node =>
      val meta = node.metadata
      val readSchema = ReadParser.formatSchemaStr(meta.getOrElse("ReadSchema", ""))

      dataSourceInfo += DataSourceCase(sqlID,
        meta.getOrElse("Format", "unknown"),
        meta.getOrElse("Location", "unknown"),
        meta.getOrElse("PushedFilters", "unknown"),
        readSchema
      )
    }
  }

  // This will find scans for DataSource V2, if the schema is very large it
  // will likely be incomplete and have ... at the end.
  protected def checkGraphNodeForReads(sqlID: Long, node: SparkPlanGraphNode): Unit = {
    if (node.name.equals("BatchScan") ||
        node.name.contains("GpuScan") ||
        node.name.contains("GpuBatchScan") ||
        node.name.contains("JDBCRelation")) {
      val res = ReadParser.parseReadNode(node)

      dataSourceInfo += DataSourceCase(sqlID,
        res.format,
        res.location,
        res.filters,
        res.schema
      )
    }
  }

  protected def reportComplexTypes: (String, String) = {
    if (dataSourceInfo.size != 0) {
      val schema = dataSourceInfo.map { ds => ds.schema }
      AppBase.parseReadSchemaForNestedTypes(schema)
    } else {
      ("", "")
    }
  }

  protected def probNotDataset: HashMap[Long, Set[String]] = {
    sqlIDtoProblematic.filterNot { case (sqlID, _) => sqlIDToDataSetOrRDDCase.contains(sqlID) }
  }

  protected def getPotentialProblemsForDf: String = {
    probNotDataset.values.flatten.toSet.mkString(":")
  }

  // This is to append potential issues such as UDF, decimal type determined from
  // SparkGraphPlan Node description and nested complex type determined from reading the
  // event logs. If there are any complex nested types, then `NESTED COMPLEX TYPE` is mentioned
  // in the `Potential Problems` section in the csv file. Section `Unsupported Nested Complex
  // Types` has information on the exact nested complex types which are not supported for a
  // particular application.
  protected def getAllPotentialProblems(dFPotentialProb: String, nestedComplex: String): String = {
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
}

object AppBase {

  def parseReadSchemaForNestedTypes(
      schema: ArrayBuffer[String]): (String, String) = {
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
