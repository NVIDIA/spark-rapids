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

package org.apache.spark.sql.rapids.tool

import java.io.InputStream
import java.util.zip.GZIPInputStream

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import com.nvidia.spark.rapids.tool.profiling.DataSourceCase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.util.{JsonProtocol, Utils}

abstract class AppBase(
    val numOutputRows: Int,
    val eventLogInfo: EventLogInfo,
    val hadoopConf: Configuration) extends Logging {

  var sparkVersion: String = ""
  var appEndTime: Option[Long] = None
  // The data source information
  val dataSourceInfo: ArrayBuffer[DataSourceCase] = ArrayBuffer[DataSourceCase]()

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
    val eventlog = eventLogInfo.eventLog

    logInfo("Parsing Event Log: " + eventlog.toString)

    // at this point all paths should be valid event logs or event log dirs
    val fs = eventlog.getFileSystem(hadoopConf)
    var totalNumEvents = 0
    val readerOpt = eventLogInfo match {
      case dblog: DatabricksEventLog =>
        Some(new DatabricksRollingEventLogFilesFileReader(fs, eventlog))
      case apachelog => EventLogFileReader(fs, eventlog)
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
      logError(s"Error getting reader for ${eventlog.getName}")
    }
    logInfo(s"Total number of events parsed: $totalNumEvents for ${eventlog.toString}")
  }

  protected def isDataSetPlan(desc: String): Boolean = {
    desc match {
      case l if l.matches(".*\\$Lambda\\$.*") => true
      case a if a.endsWith(".apply") => true
      case _ => false
    }
  }

  // Decimal support on the GPU is limited to less than 18 digits and decimals
  // are configured off by default for now. It would be nice to have this
  // based off of what plugin supports at some point.
  private val decimalKeyWords = Map(".*promote_precision\\(.*" -> "DECIMAL",
    ".*decimal\\([0-9]+,[0-9]+\\).*" -> "DECIMAL",
    ".*DecimalType\\([0-9]+,[0-9]+\\).*" -> "DECIMAL")

  private val UDFKeywords = Map(".*UDF.*" -> "UDF")

  protected def findPotentialIssues(desc: String): Set[String] =  {
    val potentialIssuesRegexs = UDFKeywords ++ decimalKeyWords
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

  // strip off the struct<> part that Spark adds to the ReadSchema
  private def formatSchemaStr(schema: String): String = {
    schema.stripPrefix("struct<").stripSuffix(">")
  }

  // The ReadSchema metadata is only in the eventlog for DataSource V1 readers
  protected def checkMetadataForReadSchema(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = getPlanMetaWithSchema(planInfo)
    allMetaWithSchema.foreach { node =>
      val meta = node.metadata
      val readSchema = formatSchemaStr(meta.getOrElse("ReadSchema", ""))

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
  protected def checkGraphNodeForBatchScan(sqlID: Long, node: SparkPlanGraphNode): Unit = {
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

      dataSourceInfo += DataSourceCase(sqlID,
        fileFormat,
        location,
        pushedFilters,
        schema
      )
    }
  }
}
