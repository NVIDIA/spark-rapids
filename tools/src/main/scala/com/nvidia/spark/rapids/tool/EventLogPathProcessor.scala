package com.nvidia.spark.rapids.tool

import java.io.FileNotFoundException
import java.time.LocalDateTime
import java.util.zip.ZipOutputStream

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Map}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.deploy.history.{EventLogFileReader, EventLogFileWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

trait EventLogInfo {
  def eventLog: Path
}

case class ApacheSparkEventLog(eventLog: Path) extends EventLogInfo
case class DatabricksEventLog(eventLog: Path) extends EventLogInfo


object EventLogPathProcessor extends Logging {
  val EVENT_LOG_DIR_NAME_PREFIX = "eventlog_v2_"
  val EVENT_LOG_FILE_NAME_PREFIX = "events_"

  def isEventLogDir(status: FileStatus): Boolean = {
    status.isDirectory && status.getPath.getName.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  // This only checks the name of the path
  def isEventLogDir(path: String): Boolean = {
    path.startsWith(EVENT_LOG_DIR_NAME_PREFIX)
  }

  // https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/
  // core/src/main/scala/org/apache/spark/io/CompressionCodec.scala#L67
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES = Set("lz4", "lzf", "snappy", "zstd")
  val SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER =
    SPARK_SHORT_COMPRESSION_CODEC_NAMES ++ Set("gz")


  def eventLogNameFilter(logFile: Path): Boolean = {
    EventLogFileWriter.codecName(logFile)
      .forall(suffix => SPARK_SHORT_COMPRESSION_CODEC_NAMES_FOR_FILTER.contains(suffix))
  }

  def dbFileFormat(logfile: String): Unit = {
    // eventlog for active
    // eventlog-2021-06-14--20-00.gz for rolled and compressed
    if (logfile.startsWith("eventlog-")) {
      // then look for date time and compression
    }
  }

  // Databricks tends to have latest eventlog and then any rolled in format
  // eventlog-2021-06-14--20-00.gz
  def isDatabricksEventLogDir(dir: FileStatus,
      fs: FileSystem, databricksLogs: Option[Boolean]): Boolean = {
    logWarning(s"checking is databricks log ${dir.getPath.getName}")
    databricksLogs match {
      case Some(true) =>
        logWarning("true is databricks log")
        true
      case Some(false) =>
        logWarning("false not databricks log")
        false
      case _ =>
        // try to determine if dir structure looks right
        logWarning(s"listing status ${dir.getPath.getName}")
        val dirList = fs.listStatus(dir.getPath)
        if (dirList.size > 0) {
          logWarning(s"listing status 2 ${dir.getPath.getName}")

          if (dirList.exists(_.getPath.getName.equals("eventlog"))) {
            if (dirList.size > 1) {
              logWarning(s"listing status 3 ${dirList.map(_.getPath.getName).mkString(", ")}")

              val res = dirList.exists(_.getPath.getName
                .matches("eventlog-([0-9]){4}-([0-9]){2}-([0-9]){2}--([0-9]){2}-([0-9]){2}.*"))
              logWarning(s"checking exists res is: $res")
              res
            } else {
              true
            }
          } else {
            false
          }
        } else {
          false
        }
    }
  }

  // Return an Array(Path) and Timestamp Map based on input path string
  def stringToPath(pathString: String,
      databricksLogs: Option[Boolean] = None): Map[EventLogInfo, Long] = {
    val inputPath = new Path(pathString)
    val uri = inputPath.toUri
    val fs = FileSystem.get(uri, new Configuration())
    val pathsWithTimestamp: Map[EventLogInfo, Long] = Map.empty[EventLogInfo, Long]
    try {
      val fileStatus = fs.getFileStatus(inputPath)
      val filePath = fileStatus.getPath()
      val fileName = filePath.getName()
      if (!eventLogNameFilter(filePath)) {
        logWarning(s"File: $fileName it not a supported file type. " +
          "Supported compression types are: " +
          s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES.mkString(", ")}. " +
          "Skipping this file.")
      } else if (fileStatus.isDirectory && isEventLogDir(fileStatus)) {
        // either event logDir v2 directory or regular event log
        pathsWithTimestamp +=
          (ApacheSparkEventLog(fileStatus.getPath) -> fileStatus.getModificationTime)
      } else if (fileStatus.isDirectory &&
        isDatabricksEventLogDir(fileStatus, fs, databricksLogs)) {
        logWarning("is databricks event log dir")
        pathsWithTimestamp +=
          (DatabricksEventLog(fileStatus.getPath) -> fileStatus.getModificationTime)
      } else {
        // assume either single event log or directory with event logs in it, we don't
        // supported nested dirs, so if event log dir within another one we skip it
        val (validLogs, invalidLogs) = fs.listStatus(inputPath)
          .partition(s => {
            val name = s.getPath().getName()
            logWarning(s"s is: $name dir ${s.isDirectory} file: ${s.isFile}")
            (s.isFile || (s.isDirectory && isEventLogDir(name)))
          })
        val (logsSupported, unsupportLogs) =
          validLogs.partition(l => eventLogNameFilter(l.getPath()))
        logWarning("file status is are: " + logsSupported.map(_.getPath).mkString(", "))
        if (logsSupported != null) {
          logsSupported.map(a => pathsWithTimestamp +=
            (ApacheSparkEventLog(a.getPath) -> a.getModificationTime))
        }
        if (invalidLogs.nonEmpty) {
          logWarning("Skipping the following directories: " +
            s"${invalidLogs.map(_.getPath().getName()).mkString(", ")}")
        }
        if (unsupportLogs.nonEmpty) {
          logWarning(s"Files: ${unsupportLogs.map(_.getPath.getName).mkString(", ")} " +
            s"have unsupported file types. Supported compression types are: " +
            s"${SPARK_SHORT_COMPRESSION_CODEC_NAMES.mkString(", ")}. " +
            "Skipping these files.")
        }
      }
    } catch {
      case e: FileNotFoundException => logWarning(s"$pathString not found, skipping!")
    }
    pathsWithTimestamp
  }

  /**
   * Function to evaluate the event logs to be processed.
   *
   * @param filterNLogs    number of event logs to be selected
   * @param matchlogs      keyword to match file names in the directory
   * @param eventLogsPaths Array of event log paths
   * @return event logs to be processed
   */
  def processAllPaths(
      filterNLogs: Option[String],
      matchlogs: Option[String],
      eventLogsPaths: List[String],
      databricksLogs: Option[Boolean] = None): ArrayBuffer[EventLogInfo] = {

    var allPathsWithTimestamp: Map[EventLogInfo, Long] = Map.empty[EventLogInfo, Long]
    for (pathString <- eventLogsPaths) {
      val paths = stringToPath(pathString, databricksLogs)
      if (paths.nonEmpty) {
        allPathsWithTimestamp ++= paths
      }
    }

    // Filter the event logs to be processed based on the criteria. If it is not provided in the
    // command line, then return all the event logs processed above.
    val paths = if (matchlogs.isDefined || filterNLogs.isDefined) {
      if (matchlogs.isDefined) {
        allPathsWithTimestamp = allPathsWithTimestamp.filter { case (logInfo, _) =>
          logInfo.eventLog.getName.contains(matchlogs.get)
        }
      }
      if (filterNLogs.isDefined) {
        val numberofEventLogs = filterNLogs.get.split("-")(0).toInt
        val criteria = filterNLogs.get.split("-")(1)
        if (criteria.equals("newest")) {
          allPathsWithTimestamp = LinkedHashMap(
            allPathsWithTimestamp.toSeq.sortWith(_._2 > _._2): _*)
        } else if (criteria.equals("oldest")) {
          allPathsWithTimestamp = LinkedHashMap(
            allPathsWithTimestamp.toSeq.sortWith(_._2 < _._2): _*)
        } else {
          logError("Criteria should be either newest or oldest")
          System.exit(1)
        }
        ArrayBuffer(allPathsWithTimestamp.keys.toSeq.take(numberofEventLogs): _*)
      } else {
        // return event logs which contains the keyword.
        ArrayBuffer(allPathsWithTimestamp.keys.toSeq: _*)
      }
    } else { // send all event logs for processing
      ArrayBuffer(allPathsWithTimestamp.keys.toSeq: _*)
    }
    logWarning("paths is; " + paths)
    //val finalPaths = paths.filter(eventLogNameFilter)
    // logWarning("paths is; " + finalPaths)
    paths
  }

  def logApplicationInfo(app: ApplicationInfo) = {
    logInfo(s"==============  ${app.appId} (index=${app.index})  ==============")
  }

  val DB_EVENT_LOG_FILE_NAME_PREFIX = "eventlog"

  def isDBEventLogFile(fileName: String): Boolean = {
    fileName.startsWith(DB_EVENT_LOG_FILE_NAME_PREFIX)
  }

  def isDBEventLogFile(status: FileStatus): Boolean = {
    logWarning(s"is db event log ${status.isFile} and ${isDBEventLogFile(status.getPath.getName)}")
    status.isFile && isDBEventLogFile(status.getPath.getName)
  }

  val dbFileFormat = "eventlog-([0-9]){4}-([0-9]){2}-([0-9]){2}--([0-9]){2}-([0-9]){2}.*"

  def getDBEventLogFileDate(eventLogFileName: String): LocalDateTime = {
    if (!isDBEventLogFile(eventLogFileName)) {
      logError(s"$eventLogFileName Not an event log file!")
    }
    logWarning(s"get event log date for $eventLogFileName")
    val fileParts = eventLogFileName.split("--")
    if (fileParts.size < 2) {
      // assume this is the current log
      logWarning("returning now")
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

  /**
   * The reader which will read the information of rolled multiple event log files.
   *
   * This reader lists the files only once; if caller would like to play with updated list,
   * it needs to create another reader instance.
   */
  class DatabricksRollingEventLogFilesFileReader(
      fs: FileSystem,
      path: Path) extends EventLogFileReader(fs, path) {

    private lazy val files: Seq[FileStatus] = {
      val ret = fs.listStatus(rootPath).toSeq
      logWarning(s"files is: ${ret.map(_.getPath.getName).mkString(", ")}")
      if (!ret.exists(isDBEventLogFile)) {
        logWarning("Log directory must contain at least one event log file!")
        Seq.empty[FileStatus]
      } else {
        ret
      }
    }

    private lazy val eventLogFiles: Seq[FileStatus] = {
      files.filter(isDBEventLogFile).sortWith { (status1, status2) =>
        logWarning(s"comparing ${status1.getPath.getName} and ${status2.getPath.getName}")
        val dateTime = getDBEventLogFileDate(status1.getPath.getName)
        val dateTime2 = getDBEventLogFileDate(status2.getPath.getName)
        val res = dateTime.isBefore(dateTime2)
        logWarning(s"comparing ${status1.getPath.getName} and ${status2.getPath.getName} res: $res")
        res
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
}
