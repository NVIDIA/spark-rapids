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

package com.nvidia.spark.rapids.tool

import java.io.FileNotFoundException
import java.time.LocalDateTime
import java.util.zip.ZipOutputStream

import scala.collection.mutable.LinkedHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

sealed trait EventLogInfo {
  def eventLog: Path
}

case class ApacheSparkEventLog(override val eventLog: Path) extends EventLogInfo
case class DatabricksEventLog(override val eventLog: Path) extends EventLogInfo


object EventLogPathProcessor extends Logging {
  // Apache Spark event log prefixes
  val EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  val EVENT_LOG_FILE_NAME_PREFIX = "events_"
  val DB_EVENT_LOG_FILE_NAME_PREFIX = "eventlog"

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && isEventLogDir(status.getPath.getName)
  }

  // This only checks the name of the path
  def isEventLogDir(path: String): Boolean = {
    path.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  def isDBEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(DB_EVENT_LOG_FILE_NAME_PREFIX)
  }

  def isDBEventLogFile(status: FileStatus): Boolean = {
    status.isFile && isDBEventLogFile(status.getPath.getName)
  }

  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/
  // core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L67
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES = Set("lz4", "lzf", "snappy", "zstd")
  // Apache Spark ones plus gzip
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER =
    SPARK_SHORT_COMPRESSION_CODEC_NAMES ++ Set("gz")

  def eventLogNameFilter(logFile: Path): Boolean = {
    EventLogFileWriter.codecName(logFile)
      .forall(suffix => SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.contains(suffix))
  }

  // Databricks has the latest events in file named eventlog and then any rolled in format
  // eventlog-2021-06-14--20-00.gz, here we assume that is any files start with eventlog
  // then the directory is a Databricks event log directory.
  def isDatabricksEventLogDir(dir: FileStatus, fs: FileSystem): Boolean = {
    val dbLogFiles = fs.listStatus(dir.getPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        isDBEventLogFile(path.getName)
      }
    })
    (dbLogFiles.size > 1)
  }

  def getEventLogInfo(pathString: String, hadoopConf: Configuration): Map[EventLogInfo, Long] = {
    val inputPath = new Path(pathString)
    val fs = inputPath.getFileSystem(hadoopConf)
    try {
      val fileStatus = fs.getFileStatus(inputPath)
      val filePath = fileStatus.getPath()
      val fileName = filePath.getName()

      if (fileStatus.isFile() && !eventLogNameFilter(filePath)) {
        logWarning(s"File: $fileName it not a supported file type. " +
          "Supported compression types are: " +
          s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.mkString(", ")}. " +
          "Skipping this file.")
        Map.empty[EventLogInfo, Long]
      } else if (fileStatus.isDirectory && isEventLogDir(fileStatus)) {
        // either event logDir v2 directory or regular event log
        val info = ApacheSparkEventLog(fileStatus.getPath).asInstanceOf[EventLogInfo]
        Map(info -> fileStatus.getModificationTime)
      } else if (fileStatus.isDirectory &&
        isDatabricksEventLogDir(fileStatus, fs)) {
        val dbinfo = DatabricksEventLog(fileStatus.getPath).asInstanceOf[EventLogInfo]
        Map(dbinfo -> fileStatus.getModificationTime)
      } else {
        // assume either single event log or directory with event logs in it, we don't
        // support nested dirs, so if event log dir within another one we skip it
        val (validLogs, invalidLogs) = fs.listStatus(inputPath).partition(s => {
            val name = s.getPath().getName()
            (s.isFile ||
              (s.isDirectory && (isEventLogDir(name) || isDatabricksEventLogDir(s, fs))))
          })
        if (invalidLogs.nonEmpty) {
          logWarning("Skipping the following directories: " +
            s"${invalidLogs.map(_.getPath().getName()).mkString(", ")}")
        }
        val (logsSupported, unsupport) = validLogs.partition { l =>
          (l.isFile && eventLogNameFilter(l.getPath())) || l.isDirectory
        }
        if (unsupport.nonEmpty) {
          logWarning(s"Files: ${unsupport.map(_.getPath.getName).mkString(", ")} " +
            s"have unsupported file types. Supported compression types are: " +
            s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.mkString(", ")}. " +
            "Skipping these files.")
        }
        logsSupported.map { s =>
          if (s.isFile || (s.isDirectory && isEventLogDir(s.getPath().getName()))) {
            (ApacheSparkEventLog(s.getPath).asInstanceOf[EventLogInfo] -> s.getModificationTime)
          } else {
            (DatabricksEventLog(s.getPath).asInstanceOf[EventLogInfo] -> s.getModificationTime)
          }
        }.toMap
      }
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"$pathString not found, skipping!")
        Map.empty[EventLogInfo, Long]
    }
  }

  /**
   * Function to process event log paths passed in and evaluate which ones are really event
   * logs and filter based on user options.
   *
   * @param filterNLogs    number of event logs to be selected
   * @param matchlogs      keyword to match file names in the directory
   * @param eventLogsPaths Array of event log paths
   * @param hadoopConf     Hadoop Configuration
   * @return EventLogInfo indicating type and location of event log
   */
  def processAllPaths(
      filterNLogs: Option[String],
      matchlogs: Option[String],
      eventLogsPaths: List[String],
      hadoopConf: Configuration): Seq[EventLogInfo] = {

    val logsWithTimestamp = eventLogsPaths.flatMap(getEventLogInfo(_, hadoopConf)).toMap

    logDebug("Paths after stringToPath: " + logsWithTimestamp)
    // Filter the event logs to be processed based on the criteria. If it is not provided in the
    // command line, then return all the event logs processed above.
    val matchedLogs = matchlogs.map { strMatch =>
      logsWithTimestamp.filterKeys(_.eventLog.getName.contains(strMatch))
    }.getOrElse(logsWithTimestamp)

    val filteredLogs = filterNLogs.map { filter =>
      val filteredInfo = filterNLogs.get.split("-")
      val numberofEventLogs = filteredInfo(0).toInt
      val criteria = filteredInfo(1)
      val matched = if (criteria.equals("newest")) {
        LinkedHashMap(matchedLogs.toSeq.sortWith(_._2 > _._2): _*)
      } else if (criteria.equals("oldest")) {
        LinkedHashMap(matchedLogs.toSeq.sortWith(_._2 < _._2): _*)
      } else {
        logError("Criteria should be either newest or oldest")
        Map.empty[EventLogInfo, Long]
      }
      matched.take(numberofEventLogs)
    }.getOrElse(matchedLogs)

    filteredLogs.keys.toSeq
  }

  def logApplicationInfo(app: ApplicationInfo) = {
    logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }

  def getDBEventLogFileDate(eventLogFileName: String): LocalDateTime = {
    if (!isDBEventLogFile(eventLogFileName)) {
      logError(s"$eventLogFileName Not an event log file!")
    }
    val fileParts = eventLogFileName.split("--")
    if (fileParts.size < 2) {
      // assume this is the current log and we want that one to be read last
      LocalDateTime.now()
    } else {
      val date = fileParts(0).split("-")
      val day = Integer.parseInt(date(3))
      val month = Integer.parseInt(date(2))
      val year = Integer.parseInt(date(1))
      val time = fileParts(1).split("-")
      val minParse = time(1).split('.')
      val hour = Integer.parseInt(time(0))
      val min = Integer.parseInt(minParse(0))
      LocalDateTime.of(year, month, day, hour, min)
    }
  }
}

/**
 * The reader which will read the information of Databricks rolled multiple event log files.
 */
class DatabricksRollingEventLogFilesFileReader(
    fs: FileSystem,
    path: Path) extends EventLogFileReader(fs, path) with Logging {

  private lazy val files: Seq[FileStatus] = {
    val ret = fs.listStatus(rootPath).toSeq
    if (!ret.exists(EventLogPathProcessor.isDBEventLogFile)) {
      Seq.empty[FileStatus]
    } else {
      ret
    }
  }

  private lazy val eventLogFiles: Seq[FileStatus] = {
    files.filter(EventLogPathProcessor.isDBEventLogFile).sortWith { (status1, status2) =>
      val dateTime = EventLogPathProcessor.getDBEventLogFileDate(status1.getPath.getName)
      val dateTime2 = EventLogPathProcessor.getDBEventLogFileDate(status2.getPath.getName)
      dateTime.isBefore(dateTime2)
    }
  }

  override def completed: Boolean = true
  override def modificationTime: Long = lastEventLogFile.getModificationTime
  private def lastEventLogFile: FileStatus = eventLogFiles.last
  override def listEventLogFiles: Seq[FileStatus] = eventLogFiles

  // unused functions
  override def compressionCodec: Option[String] = None
  override def totalSize: Long = 0
  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {}
  override def fileSizeForLastIndexForDFS: Option[Long] = None
  override def fileSizeForLastIndex: Long = lastEventLogFile.getLen
  override def lastIndex: Option[Long] = None
}
