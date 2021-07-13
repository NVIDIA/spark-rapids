package org.apache.spark.sql.rapids.tool

import com.nvidia.spark.rapids.tool.{DatabricksEventLog, DatabricksRollingEventLogFilesFileReader, EventLogInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.scheduler.{SparkListenerApplicationStart, SparkListenerEvent}
import org.apache.spark.sql.rapids.tool.qualification.{QualApplicationInfo, QualEventProcessor}
import org.apache.spark.util.{JsonProtocol, Utils}
import org.json4s.jackson.JsonMethods.parse

import scala.io.{Codec, Source}

class AppFilter(
    numOutputRows: Int,
    eventLogInfo: EventLogInfo,
    hadoopConf: Configuration) extends AppBase(numOutputRows, eventLogInfo, hadoopConf) {

  private lazy val eventProcessor = new QualEventProcessor()

  var appInfo: Option[QualApplicationInfo] = None

  override def processEvent(event: SparkListenerEvent): Unit = {
    println("INSIDE APP FILTER ProcessEvent")
    eventProcessor.doSparkListenerApplicationStart(
      this, event.asInstanceOf[SparkListenerApplicationStart])
  }

  override def processEvents(): Unit = {
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
          //println(s"TOTAL NUM EVENTS $totalNumEvents")
          //println("LISTING EVENTS")
          val appStartClass = "org.apache.spark.scheduler.SparkListenerApplicationStart"
          lines.foreach { line =>
            try {
              val event = JsonProtocol.sparkEventFromJson(parse(line))

              if (event.getClass.getName.equals(appStartClass)) {
                println("INSIDE IF")
                println(event)
                processEvent(event)
                return
              }
              println(event.getClass)
              //println(processEvent(event))
              //processEvent(event)
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

  processEvents()
}
