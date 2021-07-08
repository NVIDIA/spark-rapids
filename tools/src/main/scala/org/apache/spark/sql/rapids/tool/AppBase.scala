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

import scala.collection.Map
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

  def processEvent(event: SparkListenerEvent): Unit

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
          totalNumEvents += lines.size
          lines.foreach { line =>
            try {
              val event = JsonProtocol.sparkEventFromJson(parse(line))
              processEvent(event)
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
    logInfo(s"Total number of events parsed: $totalNumEvents for ${eventlog.toString}")
  }

  protected def isDataSetPlan(desc: String): Boolean = {
    desc match {
      case l if l.matches(".*\\$Lambda\\$.*") => true
      case a if a.endsWith(".apply") => true
      case _ => false
    }
  }

  protected def findPotentialIssues(desc: String): Option[String] =  {
    desc match {
      case u if u.matches(".*UDF.*") => Some("UDF")
      case _ => None
    }
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
  protected def checkMetadataForReadSchema(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    // check if planInfo has ReadSchema
    val allMetaWithSchema = getPlanMetaWithSchema(planInfo)
    allMetaWithSchema.foreach { node =>
      val meta = node.metadata
      val readSchema = formatSchemaStr(meta.getOrElse("ReadSchema", ""))
      // val allTypes = AppBase.parseSchemaString(Some(readSchema)).values.toSet
      // val schemaStr = allTypes.mkString(",")

      // TODO - qualification may just check right here instead of recording!
      dataSourceInfo += DataSourceCase(sqlID,
        meta.getOrElse("Format", "unknown"),
        meta.getOrElse("Location", "unknown"),
        meta.getOrElse("PushedFilters", "unknown"),
        readSchema,
        false
      )
    }
  }

  // This will find scans for DataSource V2
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
      // val allTypes = AppBase.parseSchemaString(Some(schema)).values.toSet
      // val schemaStr = allTypes.mkString(",")

      dataSourceInfo += DataSourceCase(sqlID,
        fileFormat,
        location,
        pushedFilters,
        schema,
        AppBase.schemaIncomplete(schema)
      )
    }
  }
}

object AppBase extends Logging {

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
      // struct<name:string,age:int,salary:double,array:array<int>>
      // map<string,string>
      // array<string>

      // ReadSchema: struct<key:string,location:struct<lat:double,long:double>>
      // ReadSchema: struct<name:struct<firstname:string,middlename:array<string>,lastname:string>,
      // address:struct<current:struct<state:string,city:string>,previous:
      // struct<state:map<string,string>,city:string>>>
      val complextTypes = Seq("struct", "map")
      val containsComplex = complextTypes.exists(schema.contains)

      val keyValues = if (containsComplex) {
        // can't just split on commas because struct can have commas in it

        schema.split(",")
      } else {
        // just split on commas
        schema.split(",")
      }
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
