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

package org.apache.spark.sql.rapids.tool.profiling

import java.io.FileWriter
import java.net.URI

import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.jackson.JsonMethods.parse
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.{Codec, Source}

import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ui.UIUtils
import org.apache.spark.util._

/**
 * ApplicationInfo class saves all parsed events for future use.
 */

class ApplicationInfo(
    val args: ProfileArgs,
    val sparkSession: SparkSession,
    val fileWriter: FileWriter,
    val eventlog: String,
    val index: Int) extends Logging {

  // From SparkListenerLogStart
  var sparkVersion: String = ""

  // allDataFrames is to store all the DataFrames
  // after event log parsing has completed.
  // Possible DataFrames include:
  // 1. resourceProfilesDF (Optional)
  // 2. blockManagersDF (Optional)
  // 3. appDF (Must exist, otherwise fail!)
  // 4. executorsDF (Must exist, otherwise fail!)
  // 5. propertiesDF (Must exist, otherwise fail!)
  // 6. blockManagersRemoved (Optional)
  val allDataFrames: HashMap[String, DataFrame] = HashMap.empty[String, DataFrame]

  // From SparkListenerResourceProfileAdded
  var resourceProfiles: ArrayBuffer[ResourceProfileCase] = ArrayBuffer[ResourceProfileCase]()

  // From SparkListenerBlockManagerAdded and SparkListenerBlockManagerRemoved
  var blockManagers: ArrayBuffer[BlockManagerCase] =
    ArrayBuffer[BlockManagerCase]()
  var blockManagersRemoved: ArrayBuffer[BlockManagerRemovedCase] =
    ArrayBuffer[BlockManagerRemovedCase]()

  // From SparkListenerEnvironmentUpdate
  var sparkProperties = Map.empty[String, String]
  var hadoopProperties = Map.empty[String, String]
  var systemProperties = Map.empty[String, String]
  var jvmInfo = Map.empty[String, String]
  var classpathEntries = Map.empty[String, String]
  var gpuMode = false
  var allProperties: ArrayBuffer[PropertiesCase] = ArrayBuffer[PropertiesCase]()

  // From SparkListenerApplicationStart and SparkListenerApplicationEnd
  var appStart: ArrayBuffer[ApplicationCase] = ArrayBuffer[ApplicationCase]()
  var appEndTime: Option[Long] = None
  var appId: String = ""

  // From SparkListenerExecutorAdded and SparkListenerExecutorRemoved
  var executors: ArrayBuffer[ExecutorCase] = ArrayBuffer[ExecutorCase]()
  var executorsRemoved: ArrayBuffer[ExecutorRemovedCase] = ArrayBuffer[ExecutorRemovedCase]()

  // From all other events
  var otherEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer[SparkListenerEvent]()

  // Generated warnings by predefined checks for this Application
  var warnings: ArrayBuffer[String] = ArrayBuffer[String]()

  // Process all events
  processEvents()
  // Process all properties after all events are processed
  processAllProperties()
  // Create Spark DataFrame(s) based on ArrayBuffer(s)
  arraybufferToDF()

  /**
   * Functions to process all the events
   */
  def processEvents(): Unit = {
    logInfo("Parsing Event Log File: " + eventlog)
    // Convert a String to org.apache.hadoop.fs.Path
    val uri = URI.create(eventlog)
    val config = new Configuration()
    val fs = FileSystem.get(uri, config)
    val path = new Path(eventlog)
    var totalNumEvents = 0

    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      val lines = Source.fromInputStream(in)(Codec.UTF8).getLines().toList
      totalNumEvents = lines.size
      lines.foreach { line =>
        try {
          val event = JsonProtocol.sparkEventFromJson(parse(line))
          EventsProcessor.processAnyEvent(this, event)
          logDebug(line)
        }
        catch {
          case e: ClassNotFoundException =>
            logWarning(s"ClassNotFoundException: ${e.getMessage}")
        }
      }
    }
    logInfo("Total number of events parsed: " + totalNumEvents)
  }

  /**
   * Functions to process all properties after all events are processed
   */
  def processAllProperties(): Unit = {
    for ((k, v) <- sparkProperties) {
      val thisProperty = PropertiesCase("spark", k, v)
      allProperties += thisProperty
    }
    for ((k, v) <- hadoopProperties) {
      val thisProperty = PropertiesCase("hadoop", k, v)
      allProperties += thisProperty
    }
    for ((k, v) <- systemProperties) {
      val thisProperty = PropertiesCase("system", k, v)
      allProperties += thisProperty
    }
    for ((k, v) <- jvmInfo) {
      val thisProperty = PropertiesCase("jvm", k, v)
      allProperties += thisProperty
    }
    for ((k, v) <- classpathEntries) {
      val thisProperty = PropertiesCase("classpath", k, v)
      allProperties += thisProperty
    }
  }

  /**
   * Functions to convert ArrayBuffer to DataFrame
   * and then create a view for each of them
   */
  def arraybufferToDF(): Unit = {
    import sparkSession.implicits._

    // For resourceProfilesDF
    if (this.resourceProfiles.nonEmpty) {
      this.allDataFrames += (s"resourceProfilesDF_$index" -> this.resourceProfiles.toDF)
      this.resourceProfiles.clear()
    } else {
      logWarning("resourceProfiles is empty!")
    }

    // For blockManagersDF
    if (this.blockManagers.nonEmpty) {
      this.allDataFrames += (s"blockManagersDF_$index" -> this.blockManagers.toDF)
      this.blockManagers.clear()
    } else {
      logWarning("blockManagers is empty!")
    }

    // For blockManagersRemovedDF
    if (this.blockManagersRemoved.nonEmpty) {
      this.allDataFrames += (s"blockManagersRemovedDF_$index" -> this.blockManagersRemoved.toDF)
      this.blockManagersRemoved.clear()
    } else {
      logDebug("blockManagersRemoved is empty!")
    }

    // For propertiesDF
    if (this.allProperties.nonEmpty) {
      this.allDataFrames += (s"propertiesDF_$index" -> this.allProperties.toDF)
      this.allProperties.clear()
    } else {
      logError("propertiesDF is empty! Existing...")
      System.exit(1)
    }

    // For appDF
    if (this.appStart.nonEmpty) {
      val appStartNew: ArrayBuffer[ApplicationCase] = ArrayBuffer[ApplicationCase]()
      for (res <- this.appStart) {
        val durationResult = ProfileUtils.OptionLongMinusLong(this.appEndTime, res.startTime)
        val durationString = durationResult match {
          case Some(i) => UIUtils.formatDuration(i.toLong)
          case None => ""
        }

        val newApp = res.copy(endTime = this.appEndTime, duration = durationResult,
          durationStr = durationString, sparkVersion = this.sparkVersion,
          gpuMode = this.gpuMode)
        appStartNew += newApp
      }
      this.allDataFrames += (s"appDF_$index" -> appStartNew.toDF)
      this.appStart.clear()
    } else {
      logError("Application is empty! Exiting...")
      System.exit(1)
    }

    // For executorsDF
    if (this.executors.nonEmpty) {
      this.allDataFrames += (s"executorsDF_$index" -> this.executors.toDF)
      this.executors.clear()
    } else {
      logError("executors is empty! Exiting...")
      System.exit(1)
    }

    // For executorsRemovedDF
    if (this.executorsRemoved.nonEmpty) {
      this.allDataFrames += (s"executorsRemovedDF_$index" -> this.executorsRemoved.toDF)
      this.executorsRemoved.clear()
    } else {
      logDebug("executorsRemoved is empty!")
    }

    for ((name, df) <- this.allDataFrames) {
      df.createOrReplaceTempView(name)
      sparkSession.table(name).cache
    }
  }

  // Function to run a query and show the result -- limit 1000 rows.
  def runQuery(query: String, vertical: Boolean = false): DataFrame = {
    logDebug("Running:" + query)
    val df = sparkSession.sql(query)
    logInfo("\n" + df.showString(1000, 0, vertical))
    fileWriter.write(df.showString(1000, 0, vertical))
    df
  }

  // Function to return a DataFrame based on query text
  def queryToDF(query: String): DataFrame = {
    logDebug("Creating a DataFrame based on query : \n" + query)
    sparkSession.sql(query)
  }

  // Function to generate a query for printing Application information
  def generateAppInfo: String =
    s"""select $index as appIndex, appId, startTime, endTime, duration,
       |durationStr, sparkVersion, gpuMode
       |from appDF_$index
       |""".stripMargin

  // Function to generate a query for printing Executors information
  def generateExecutorInfo: String = {
    // If both blockManagersDF and resourceProfilesDF exist:
    if (allDataFrames.contains(s"blockManagersDF_$index") &&
        allDataFrames.contains(s"resourceProfilesDF_$index")) {

      s"""select $index as appIndex, e.executorID, e.totalCores,
         |b.maxMem, b.maxOnHeapMem,b.maxOffHeapMem,
         |r.exec_cpu, r.exec_mem, r.exec_gpu, r.exec_offheap, r.task_cpu, r.task_gpu
         |from executorsDF_$index e, blockManagersDF_$index b, resourceProfilesDF_$index r
         |where e.executorID=b.executorID
         |and e.resourceProfileId=r.id
         |""".stripMargin
    } else if (allDataFrames.contains(s"blockManagersDF_$index") &&
        !allDataFrames.contains(s"resourceProfilesDF_$index")) {

      s"""select $index as appIndex,e.executorID, e.totalCores,
         |b.maxMem, b.maxOnHeapMem,b.maxOffHeapMem,
         |null as exec_cpu, null as exec_mem, null as exec_gpu,
         |null as exec_offheap, null as task_cpu, null as task_gpu
         |from executorsDF_$index e, blockManagersDF_$index b
         |where e.executorID=b.executorID
         |""".stripMargin
    } else if (!allDataFrames.contains(s"blockManagersDF_$index") &&
        allDataFrames.contains(s"resourceProfilesDF_$index")) {
      s"""select $index as appIndex,e.executorID, e.totalCores,
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |r.exec_cpu, r.exec_mem, r.exec_gpu, r.exec_offheap, r.task_cpu, r.task_gpu
         |from executorsDF_$index e, resourceProfilesDF_$index r
         |where e.resourceProfileId=r.id
         |""".stripMargin
    } else {
      s"""select $index as appIndex,executorID, totalCores
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |null as exec_cpu, null as exec_mem, null as exec_gpu,
         |null as exec_offheap, null as task_cpu, null as task_gpu
         |from executorsDF_$index
         |""".stripMargin
    }
  }

  // Function to generate a query for printing Rapids related Spark properties
  def generateRapidsProperties: String =
    s"""select key,value as value_app$index
       |from propertiesDF_$index
       |where source ='spark'
       |and key like 'spark.rapids%'
       |""".stripMargin
}