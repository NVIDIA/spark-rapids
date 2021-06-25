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

import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.{JsonProtocol, Utils}

abstract class AppBase(
    val numOutputRows: Int,
    val eventLogInfo: EventLogInfo,
    val hadoopConf: Configuration) extends Logging {

  var sparkVersion: String = ""
  var appEndTime: Option[Long] = None

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
    logInfo("Total number of events parsed: " + totalNumEvents)
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
}
