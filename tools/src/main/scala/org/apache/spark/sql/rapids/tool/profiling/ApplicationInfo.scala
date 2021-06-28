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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.{immutable, mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

import com.nvidia.spark.rapids.tool.{EventLogInfo, EventLogPathProcessor, ToolTextFileWriter}
import com.nvidia.spark.rapids.tool.profiling._

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.ui.UIUtils

class SparkPlanInfoWithStage(
    nodeName: String,
    simpleString: String,
    override val children: Seq[SparkPlanInfoWithStage],
    metadata: scala.Predef.Map[String, String],
    metrics: Seq[SQLMetricInfo],
    val stageId: Option[Int]) extends SparkPlanInfo(nodeName, simpleString, children,
  metadata, metrics) {

  import SparkPlanInfoWithStage._

  def debugEquals(other: Any, depth: Int = 0): Boolean = {
    System.err.println(s"${" " * depth}DOES $this == $other?")
    other match {
      case o: SparkPlanInfo =>
        if (nodeName != o.nodeName) {
          System.err.println(s"${" " * depth}" +
              s"$this != $o (names different) ${this.nodeName} != ${o.nodeName}")
          return false
        }
        if (simpleString != o.simpleString) {
          System.err.println(s"${" " * depth}$this != $o (simpleString different) " +
              s"${this.simpleString} != ${o.simpleString}")
          return false
        }
        if (children.length != o.children.length) {
          System.err.println(s"${" " * depth}$this != $o (different num children) " +
              s"${this.children.length} ${o.children.length}")
          return false
        }
        children.zip(o.children).zipWithIndex.forall {
          case ((t, o), index) =>
            System.err.println(s"${" " * depth}COMPARING CHILD $index")
            t.debugEquals(o, depth = depth + 1)
        }
      case _ =>
        System.err.println(s"${" " * depth}NOT EQUAL WRONG TYPE")
        false
    }
  }

  override def toString: String = {
    s"PLAN NODE: '$nodeName' '$simpleString' $stageId"
  }

  /**
   * Create a new SparkPlanInfoWithStage that is normalized in a way so that CPU and GPU
   * plans look the same. This should really only be used when matching stages/jobs between
   * different plans.
   */
  def normalizeForStageComparison: SparkPlanInfoWithStage = {
    nodeName match {
      case "GpuColumnarToRow" | "GpuRowToColumnar" | "ColumnarToRow" | "RowToColumnar" |
           "AdaptiveSparkPlan" | "AvoidAdaptiveTransitionToRow" |
           "HostColumnarToGpu" | "GpuBringBackToHost" |
           "GpuCoalesceBatches" | "GpuShuffleCoalesce" |
           "InputAdapter" | "Subquery" | "ReusedSubquery" |
           "CustomShuffleReader" | "GpuCustomShuffleReader" | "ShuffleQueryStage" |
           "BroadcastQueryStage" |
           "Sort" | "GpuSort" =>
        // Remove data format, grouping changes because they don't impact the computation
        // Remove CodeGen stages (not important to actual query execution)
        // Remove AQE fix-up parts.
        // Remove sort because in a lot of cases (like sort merge join) it is not part of the
        // actual computation and it is hard to tell which sort is important to the query result
        // and which is not
        children.head.normalizeForStageComparison
      case name if name.startsWith("WholeStageCodegen") =>
        // Remove whole stage codegen (It includes an ID afterwards that we should ignore too)
        children.head.normalizeForStageComparison
      case name if name.contains("Exchange") =>
        // Drop all exchanges, a broadcast could become a shuffle/etc
        children.head.normalizeForStageComparison
      case "GpuTopN" if isShuffledTopN(this) =>
        val coalesce = this.children.head
        val shuffle = coalesce.children.head
        val firstTopN = shuffle.children.head
        firstTopN.normalizeForStageComparison
      case name =>
        val newName = normalizedNameRemapping.getOrElse(name,
          if (name.startsWith("Gpu")) {
            name.substring(3)
          } else {
            name
          })

        val normalizedChildren = children.map(_.normalizeForStageComparison)
        // We are going to ignore the simple string because it can contain things in it that
        // are specific to a given run
        new SparkPlanInfoWithStage(newName, newName, normalizedChildren, metadata,
          metrics, stageId)
    }
  }

  /**
   * Walk the tree depth first and get all of the stages for each node in the tree
   */
  def depthFirstStages: Seq[Option[Int]] =
    Seq(stageId) ++ children.flatMap(_.depthFirstStages)
}

object SparkPlanInfoWithStage {
  def apply(plan: SparkPlanInfo, accumIdToStageId: Map[Long, Int]): SparkPlanInfoWithStage = {
    // In some cases Spark will do a shuffle in the middle of an operation,
    // like TakeOrderedAndProject. In those cases the node is associated with the
    // min stage ID, just to remove ambiguity

    val newChildren = plan.children.map(SparkPlanInfoWithStage(_, accumIdToStageId))

    val stageId = plan.metrics.flatMap { m =>
      accumIdToStageId.get(m.accumulatorId)
    }.reduceOption((l, r) => Math.min(l, r))

    new SparkPlanInfoWithStage(plan.nodeName, plan.simpleString, newChildren, plan.metadata,
      plan.metrics, stageId)
  }

  private val normalizedNameRemapping: Map[String, String] = Map(
    "Execute GpuInsertIntoHadoopFsRelationCommand" -> "Execute InsertIntoHadoopFsRelationCommand",
    "GpuTopN" -> "TakeOrderedAndProject",
    "SortMergeJoin" -> "Join",
    "ShuffledHashJoin" -> "Join",
    "GpuShuffledHashJoin" -> "Join",
    "BroadcastHashJoin" -> "Join",
    "GpuBroadcastHashJoin" -> "Join",
    "HashAggregate" -> "Aggregate",
    "SortAggregate" -> "Aggregate",
    "GpuHashAggregate" -> "Aggregate",
    "RunningWindow" -> "Window", //GpuWindow and Window are already covered
    "GpuRunningWindow" -> "Window")

  private def isShuffledTopN(info: SparkPlanInfoWithStage): Boolean = {
    if (info.children.length == 1 && // shuffle coalesce
        info.children.head.children.length == 1 && // shuffle
        info.children.head.children.head.children.length == 1) { // first top n
      val coalesce = info.children.head
      val shuffle = coalesce.children.head
      val firstTopN = shuffle.children.head
      info.nodeName == "GpuTopN" &&
          (coalesce.nodeName == "GpuShuffleCoalesce" ||
              coalesce.nodeName == "GpuCoalesceBatches") &&
          shuffle.nodeName == "GpuColumnarExchange" && firstTopN.nodeName == "GpuTopN"
    } else {
      false
    }
  }
}

/**
 * ApplicationInfo class saves all parsed events for future use.
 * Used only for Profiling.
 */
class ApplicationInfo(
    numRows: Int,
    val sparkSession: SparkSession,
    eLogInfo: EventLogInfo,
    val index: Int)
  extends AppBase(numRows, eLogInfo, sparkSession.sparkContext.hadoopConfiguration) with Logging {

  // allDataFrames is to store all the DataFrames
  // after event log parsing has completed.
  // Possible DataFrames include:
  // 1. resourceProfilesDF (Optional)
  // 2. blockManagersDF (Optional)
  // 3. appDF (Must exist, otherwise fail!)
  // 4. executorsDF (Must exist, otherwise fail!)
  // 5. propertiesDF (Must exist, otherwise fail!)
  // 6. blockManagersRemoved (Optional)
  // 7. sqlDF (Could be missing)
  // 8. jobDF (Must exist, otherwise fail!)
  // 9. stageDF (Must exist, otherwise fail!)
  // 10. taskDF (Must exist, otherwise fail!)
  val allDataFrames: mutable.HashMap[String, DataFrame] = mutable.HashMap.empty[String, DataFrame]

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
  var appId: String = ""

  // From SparkListenerExecutorAdded and SparkListenerExecutorRemoved
  var executors: ArrayBuffer[ExecutorCase] = ArrayBuffer[ExecutorCase]()
  var executorsRemoved: ArrayBuffer[ExecutorRemovedCase] = ArrayBuffer[ExecutorRemovedCase]()

  // From SparkListenerSQLExecutionStart and SparkListenerSQLExecutionEnd
  var sqlStart: ArrayBuffer[SQLExecutionCase] = ArrayBuffer[SQLExecutionCase]()
  val sqlEndTime: mutable.HashMap[Long, Long] = mutable.HashMap.empty[Long, Long]

  // From SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate
  // sqlPlan stores HashMap (sqlID <-> SparkPlanInfo)
  var sqlPlan: mutable.HashMap[Long, SparkPlanInfo] = mutable.HashMap.empty[Long, SparkPlanInfo]

  // physicalPlanDescription stores HashMap (sqlID <-> physicalPlanDescription)
  var physicalPlanDescription: mutable.HashMap[Long, String] = mutable.HashMap.empty[Long, String]

  // From SparkListenerSQLExecutionStart and SparkListenerSQLAdaptiveExecutionUpdate
  var sqlPlanMetrics: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()
  var planNodeAccum: ArrayBuffer[PlanNodeAccumCase] = ArrayBuffer[PlanNodeAccumCase]()
  // From SparkListenerSQLAdaptiveSQLMetricUpdates
  var sqlPlanMetricsAdaptive: ArrayBuffer[SQLPlanMetricsCase] = ArrayBuffer[SQLPlanMetricsCase]()

  // From SparkListenerDriverAccumUpdates
  var driverAccum: ArrayBuffer[DriverAccumCase] = ArrayBuffer[DriverAccumCase]()
  // From SparkListenerTaskEnd and SparkListenerTaskEnd
  var taskStageAccum: ArrayBuffer[TaskStageAccumCase] = ArrayBuffer[TaskStageAccumCase]()

  lazy val accumIdToStageId: immutable.Map[Long, Int] =
    taskStageAccum.map(accum => (accum.accumulatorId, accum.stageId)).toMap

  // From SparkListenerJobStart and SparkListenerJobEnd
  // JobStart contains mapping relationship for JobID -> StageID(s)
  var jobStart: ArrayBuffer[JobCase] = ArrayBuffer[JobCase]()
  val jobEndTime: mutable.HashMap[Int, Long] = mutable.HashMap.empty[Int, Long]
  val jobEndResult: mutable.HashMap[Int, String] = mutable.HashMap.empty[Int, String]
  val jobFailedReason: mutable.HashMap[Int, String] = mutable.HashMap.empty[Int, String]

  // From SparkListenerStageSubmitted and SparkListenerStageCompleted
  // stageSubmitted contains mapping relationship for Stage -> RDD(s)
  var stageSubmitted: ArrayBuffer[StageCase] = ArrayBuffer[StageCase]()
  val stageCompletionTime: mutable.HashMap[Int, Option[Long]] =
    mutable.HashMap.empty[Int, Option[Long]]
  val stageFailureReason: mutable.HashMap[Int, Option[String]] =
    mutable.HashMap.empty[Int, Option[String]]

  // A cleaned up version of stageSubmitted
  lazy val enhancedStage: ArrayBuffer[StageCase] = {
    val ret: ArrayBuffer[StageCase] = ArrayBuffer[StageCase]()
    for (res <- stageSubmitted) {
      val thisEndTime = stageCompletionTime.getOrElse(res.stageId, None)
      val thisFailureReason = stageFailureReason.getOrElse(res.stageId, None)

      val durationResult =
        ProfileUtils.optionLongMinusOptionLong(thisEndTime, res.submissionTime)
      val durationString = durationResult match {
        case Some(i) => UIUtils.formatDuration(i)
        case None => ""
      }

      val stageNew = res.copy(completionTime = thisEndTime,
        failureReason = thisFailureReason,
        duration = durationResult,
        durationStr = durationString)
      ret += stageNew
    }
    ret
  }

  lazy val enhancedJob: ArrayBuffer[JobCase] = {
    val ret = ArrayBuffer[JobCase]()
    for (res <- jobStart) {
      val thisEndTime = jobEndTime.get(res.jobID)
      val durationResult = ProfileUtils.OptionLongMinusLong(thisEndTime, res.startTime)
      val durationString = durationResult match {
        case Some(i) => UIUtils.formatDuration(i)
        case None => ""
      }

      val jobNew = res.copy(endTime = thisEndTime,
        duration = durationResult,
        durationStr = durationString,
        jobResult = jobEndResult.get(res.jobID),
        failedReason = jobFailedReason.get(res.jobID)
      )
      ret += jobNew
    }
    ret
  }

  lazy val enhancedSql: ArrayBuffer[SQLExecutionCase] = {
    val ret: ArrayBuffer[SQLExecutionCase] = ArrayBuffer[SQLExecutionCase]()
    for (res <- sqlStart) {
      val thisEndTime = sqlEndTime.get(res.sqlID)
      val durationResult = ProfileUtils.OptionLongMinusLong(thisEndTime, res.startTime)
      val durationString = durationResult match {
        case Some(i) => UIUtils.formatDuration(i)
        case None => ""
      }
      val (containsDataset, sqlQDuration) = if (datasetSQL.exists(_.sqlID == res.sqlID)) {
        (true, Some(0L))
      } else {
        (false, durationResult)
      }
      val potProbs = problematicSQL.filter { p =>
        p.sqlID == res.sqlID && p.reason.nonEmpty
      }.map(_.reason).mkString(",")
      val finalPotProbs = if (potProbs.isEmpty) {
        null
      } else {
        potProbs
      }
      val sqlExecutionNew = res.copy(endTime = thisEndTime,
        duration = durationResult,
        durationStr = durationString,
        sqlQualDuration = sqlQDuration,
        hasDataset = containsDataset,
        problematic = finalPotProbs
      )
      ret += sqlExecutionNew
    }
    ret
  }

  // From SparkListenerTaskStart & SparkListenerTaskEnd
  // taskStart was not used so comment out for now
  // var taskStart: ArrayBuffer[SparkListenerTaskStart] = ArrayBuffer[SparkListenerTaskStart]()
  // taskEnd contains task level metrics - only used for profiling
  var taskEnd: ArrayBuffer[TaskCase] = ArrayBuffer[TaskCase]()

  // From SparkListenerTaskGettingResult
  var taskGettingResult: ArrayBuffer[SparkListenerTaskGettingResult] =
    ArrayBuffer[SparkListenerTaskGettingResult]()

  // Unsupported SQL plan
  var unsupportedSQLplan: ArrayBuffer[UnsupportedSQLPlan] = ArrayBuffer[UnsupportedSQLPlan]()

  // From all other events
  var otherEvents: ArrayBuffer[SparkListenerEvent] = ArrayBuffer[SparkListenerEvent]()

  // Generated warnings by predefined checks for this Application
  var warnings: ArrayBuffer[String] = ArrayBuffer[String]()

  // All the metrics column names in Task Metrics with the aggregation type
  val taskMetricsColumns: scala.collection.mutable.SortedMap[String, String]
  = scala.collection.mutable.SortedMap(
    "duration" -> "all",
    "gettingResultTime" -> "sum",
    "executorDeserializeTime" -> "sum",
    "executorDeserializeCPUTime" -> "sum",
    "executorRunTime" -> "sum",
    "executorCPUTime" -> "sum",
    "peakExecutionMemory" -> "max",
    "resultSize" -> "max",
    "jvmGCTime" -> "sum",
    "resultSerializationTime" -> "sum",
    "memoryBytesSpilled" -> "sum",
    "diskBytesSpilled" -> "sum",
    "sr_remoteBlocksFetched" -> "sum",
    "sr_localBlocksFetched" -> "sum",
    "sr_fetchWaitTime" -> "sum",
    "sr_remoteBytesRead" -> "sum",
    "sr_remoteBytesReadToDisk" -> "sum",
    "sr_localBytesRead" -> "sum",
    "sr_totalBytesRead" -> "sum",
    "sw_bytesWritten" -> "sum",
    "sw_writeTime" -> "sum",
    "sw_recordsWritten" -> "sum",
    "input_bytesRead" -> "sum",
    "input_recordsRead" -> "sum",
    "output_bytesWritten" -> "sum",
    "output_recordsWritten" -> "sum"
  )

  // By looping through SQL Plan nodes to find out the problematic SQLs. Currently we define
  // problematic SQL's as those which have RowToColumnar, ColumnarToRow transitions and Lambda's in
  // the Spark plan.
  var problematicSQL: ArrayBuffer[ProblematicSQLCase] = ArrayBuffer[ProblematicSQLCase]()

  // SQL containing any Dataset operation
  var datasetSQL: ArrayBuffer[DatasetSQLCase] = ArrayBuffer[DatasetSQLCase]()

  private lazy val eventProcessor =  new EventsProcessor()

  // Process all events
  processEvents()
  // Process all properties after all events are processed
  processAllProperties()
  // Process SQL Plan Metrics after all events are processed
  processSQLPlanMetrics()
  // Create Spark DataFrame(s) based on ArrayBuffer(s)
  arraybufferToDF()

  private val codecMap = new ConcurrentHashMap[String, CompressionCodec]()

  override def processEvent(event: SparkListenerEvent) = {
    eventProcessor.processAnyEvent(this, event)
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
   * Function to process SQL Plan Metrics after all events are processed
   */
  def processSQLPlanMetrics(): Unit ={
    for ((sqlID, planInfo) <- sqlPlan){
      val planGraph = SparkPlanGraph(planInfo)
      // SQLPlanMetric is a case Class of
      // (name: String,accumulatorId: Long,metricType: String)
      val allnodes = planGraph.allNodes
      for (node <- allnodes) {
        if (isDataSetPlan(node.desc)) {
          datasetSQL += DatasetSQLCase(sqlID)
          if (gpuMode) {
            val thisPlan = UnsupportedSQLPlan(sqlID, node.id, node.name, node.desc)
            unsupportedSQLplan += thisPlan
          }
        }
        // Then process SQL plan metric type
        for (metric <- node.metrics){
          val thisMetric = SQLPlanMetricsCase(sqlID,metric.name,
            metric.accumulatorId,metric.metricType)
          sqlPlanMetrics += thisMetric
          val thisNode = PlanNodeAccumCase(sqlID, node.id,
            node.name, node.desc, metric.accumulatorId)
          planNodeAccum += thisNode
        }
      }
    }
    if (this.sqlPlanMetricsAdaptive.nonEmpty){
      logInfo(s"Merging ${sqlPlanMetricsAdaptive.size} SQL Metrics(Adaptive) for appID=$appId")
      sqlPlanMetrics = sqlPlanMetrics.union(sqlPlanMetricsAdaptive).distinct
    }
  }

  /**
   * Functions to convert ArrayBuffer to DataFrame
   * and then create a view for each of them
   */
  def arraybufferToDF(): Unit = {
    import sparkSession.implicits._

    // For appDF
    if (this.appStart.nonEmpty) {
      val appStartNew: ArrayBuffer[ApplicationCase] = ArrayBuffer[ApplicationCase]()
      for (res <- this.appStart) {

        val estimatedResult =
          this.appEndTime match {
            case Some(t) => this.appEndTime
            case None =>
              if (this.sqlEndTime.isEmpty && this.jobEndTime.isEmpty) {
                None
              } else {
                logWarning("Application End Time is unknown, estimating based on" +
                  " job and sql end times!")
                // estimate the app end with job or sql end times
                val sqlEndTime = if (this.sqlEndTime.isEmpty) 0L else this.sqlEndTime.values.max
                val jobEndTime = if (this.jobEndTime.isEmpty) 0L else this.jobEndTime.values.max
                val maxEndTime = math.max(sqlEndTime, jobEndTime)
                if (maxEndTime == 0) None else Some(maxEndTime)
              }
          }

        val durationResult = ProfileUtils.OptionLongMinusLong(estimatedResult, res.startTime)
        val durationString = durationResult match {
          case Some(i) => UIUtils.formatDuration(i.toLong)
          case None => ""
        }

        val newApp = res.copy(endTime = this.appEndTime, duration = durationResult,
          durationStr = durationString, sparkVersion = this.sparkVersion,
          gpuMode = this.gpuMode, endDurationEstimated = this.appEndTime.isEmpty)
        appStartNew += newApp
      }
      this.allDataFrames += (s"appDF_$index" -> appStartNew.toDF)
    }

    // For sqlDF
    if (sqlStart.nonEmpty) {
      allDataFrames += (s"sqlDF_$index" -> enhancedSql.toDF)
    } else {
      logInfo("No SQL Execution Found. Skipping generating SQL Execution DataFrame.")
    }

    // For jobDF
    if (jobStart.nonEmpty) {
      allDataFrames += (s"jobDF_$index" -> enhancedJob.toDF)
    }

    // For stageDF
    if (stageSubmitted.nonEmpty) {
      allDataFrames += (s"stageDF_$index" -> enhancedStage.toDF)
    }

    // For taskDF
    if (taskEnd.nonEmpty) {
      allDataFrames += (s"taskDF_$index" -> taskEnd.toDF)
    }

    // For sqlMetricsDF
    if (sqlPlanMetrics.nonEmpty) {
      logInfo(s"Total ${sqlPlanMetrics.size} SQL Metrics for appID=$appId")
      allDataFrames += (s"sqlMetricsDF_$index" -> sqlPlanMetrics.toDF)
    } else {
      logInfo("No SQL Metrics Found. Skipping generating SQL Metrics DataFrame.")
    }

    // For resourceProfilesDF
    if (this.resourceProfiles.nonEmpty) {
      this.allDataFrames += (s"resourceProfilesDF_$index" -> this.resourceProfiles.toDF)
    } else {
      logWarning("resourceProfiles is empty!")
    }

    // For blockManagersDF
    if (this.blockManagers.nonEmpty) {
      this.allDataFrames += (s"blockManagersDF_$index" -> this.blockManagers.toDF)
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
    }

    // For executorsDF
    if (this.executors.nonEmpty) {
      this.allDataFrames += (s"executorsDF_$index" -> this.executors.toDF)
    }

    // For executorsRemovedDF
    if (this.executorsRemoved.nonEmpty) {
      this.allDataFrames += (s"executorsRemovedDF_$index" -> this.executorsRemoved.toDF)
    } else {
      logDebug("executorsRemoved is empty!")
    }

    // For driverAccumDF
    allDataFrames += (s"driverAccumDF_$index" -> driverAccum.toDF)
    if (driverAccum.nonEmpty) {
      logInfo(s"Total ${driverAccum.size} driver accums for appID=$appId")
    } else {
      logInfo("No Driver accum Found. Create an empty driver accum DataFrame.")
    }

    // For taskStageAccumDF
    allDataFrames += (s"taskStageAccumDF_$index" -> taskStageAccum.toDF)
    if (taskStageAccum.nonEmpty) {
      logInfo(s"Total ${taskStageAccum.size} task&stage accums for appID=$appId")
    } else {
      logInfo("No task&stage accums Found.Create an empty task&stage accum DataFrame.")
    }

    // For planNodeAccumDF
    allDataFrames += (s"planNodeAccumDF_$index" -> planNodeAccum.toDF)
    if (planNodeAccum.nonEmpty) {
      logInfo(s"Total ${planNodeAccum.size} Plan node accums for appID=$appId")
    } else {
      logInfo("No Plan node accums Found. Create an empty Plan node accums DataFrame.")
    }

    // For unsupportedSQLPlanDF
    allDataFrames += (s"unsupportedSQLplan_$index" -> unsupportedSQLplan.toDF)
    if (unsupportedSQLplan.nonEmpty) {
      logInfo(s"Total ${unsupportedSQLplan.size} Unsupported ops for appID=$appId")
    } else {
      logInfo("No unSupportedSQLPlan node accums Found. " +
        "Create an empty node accums DataFrame.")
    }

    for ((name, df) <- this.allDataFrames) {
      df.createOrReplaceTempView(name)
    }
  }

  // Function to drop all temp views of this application.
  def dropAllTempViews(): Unit ={
    for ((name,_) <- this.allDataFrames) {
      sparkSession.catalog.dropTempView(name)
    }
  }

  // Function to run a query and optionally print the result to the file.
  def runQuery(
      query: String,
      vertical: Boolean = false,
      fileWriter: Option[ToolTextFileWriter] = None,
      messageHeader: String = ""): DataFrame = {
    logDebug("Running:" + query)
    val df = sparkSession.sql(query)
    writeDF(df, messageHeader, fileWriter, vertical=vertical)
    df
  }

  def writeDF(df: DataFrame,
      messageHeader: String,
      writer: Option[ToolTextFileWriter],
      vertical: Boolean = false): Unit = {
    writer.foreach { writer =>
      writer.write(messageHeader)
      writer.write(df.showString(numOutputRows, 0, vertical))
    }
  }

  def writeAsDF(data: java.util.List[Row],
      schema: StructType,
      messageHeader: String,
      writer: Option[ToolTextFileWriter],
      vertical: Boolean = false): Unit = {
    val df = sparkSession.createDataFrame(data, schema)
    writeDF(df, messageHeader, writer, vertical = vertical)
  }

  // Function to return a DataFrame based on query text
  def queryToDF(query: String): DataFrame = {
    logDebug("Creating a DataFrame based on query : \n" + query)
    sparkSession.sql(query)
  }

  // Function to generate a query for printing Application information
  def generateAppInfo: String =
    s"""select $index as appIndex, appName, appId, startTime, endTime, duration,
       |durationStr, sparkVersion, gpuMode as pluginEnabled
       |from appDF_$index
       |""".stripMargin

  // Function to generate a query for printing Executors information
  def generateExecutorInfo: String = {
    // If both blockManagersDF and resourceProfilesDF exist:
    if (allDataFrames.contains(s"blockManagersDF_$index") &&
        allDataFrames.contains(s"resourceProfilesDF_$index")) {
      s"""select $index as appIndex,
         |t.resourceProfileId, t.numExecutors, t.totalCores as executorCores,
         |bm.maxMem, bm.maxOnHeapMem, bm.maxOffHeapMem,
         |rp.executorMemory, rp.numGpusPerExecutor,
         |rp.executorOffHeap, rp.taskCpu, rp.taskGpu
         |from (select resourceProfileId, totalCores ,
         |count(executorId) as numExecutors,
         |max(executorId) as maxId
         |from executorsDF_$index
         |group by resourceProfileId, totalCores) t
         |inner join resourceProfilesDF_$index rp
         |on t.resourceProfileId = rp.id
         |inner join blockManagersDF_$index bm
         |on t.maxId = bm.executorId""".stripMargin
    } else if (allDataFrames.contains(s"blockManagersDF_$index") &&
        !allDataFrames.contains(s"resourceProfilesDF_$index")) {

      s"""select $index as appIndex,
         |t.numExecutors, t.totalCores as executorCores,
         |bm.maxMem, bm.maxOnHeapMem, bm.maxOffHeapMem,
         |null as executorMemory, null as numGpusPerExecutor,
         |null as executorOffHeap, null as taskCpu, null as taskGpu
         |from (select resourceProfileId, totalCores,
         |count(executorId) as numExecutors,
         |max(executorId) as maxId
         |from executorsDF_$index
         |group by resourceProfileId, totalCores) t
         |inner join blockManagersDF_$index bm
         |on t.maxId = bm.executorId""".stripMargin

    } else if (!allDataFrames.contains(s"blockManagersDF_$index") &&
        allDataFrames.contains(s"resourceProfilesDF_$index")) {
      s"""select $index as appIndex,
         |t.resourceProfileId,
         |t.numExecutors,
         |t.totalCores as executorCores,
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |rp.executorMemory, rp.numGpusPerExecutor,
         |rp.executorOffHeap, rp.taskCpu, rp.taskGpu
         |from (select resourceProfileId, totalCores,
         |count(executorId) as numExecutors,
         |max(executorId) as maxId
         |from executorsDF_$index
         |group by resourceProfileId, totalCores) t
         |inner join resourceProfilesDF_$index rp
         |on t.resourceProfileId = rp.id
         |""".stripMargin
    } else {
      s"""select $index as appIndex,
         |count(executorID) as numExecutors,
         |first(totalCores) as executorCores,
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |null as maxMem, null as maxOnHeapMem, null as maxOffHeapMem,
         |null as executorMemory, null as numGpusPerExecutor,
         |null as executorOffHeap, null as taskCpu, null as taskGpu
         |from executorsDF_$index
         |group by appIndex
         |""".stripMargin
    }
  }

  // Function to generate a query for printing Rapids related Spark properties
  def generateRapidsProperties: String = {
    s"""select key as propertyName,value as appIndex_$index
       |from propertiesDF_$index
       |where source ='spark'
       |and key like 'spark.rapids%'
       |""".stripMargin
  }

  // Function to generate the SQL string for aggregating task metrics columns.
  def generateAggSQLString: String = {
    var resultString = ""

    // Function to concat the Aggregation column string
    // eg: ",\n round(sum(column),1) as column_sum"
    def concatAggCol(col: String, aggType: String): Unit = {
      val colString = "round(" + aggType + "(t." + col + ")" + ",1)"
      resultString += ",\n" + colString + " as " + col + "_" + aggType
    }

    for ((col, aggType) <- this.taskMetricsColumns) {
      // If aggType=all, it means all 4 aggregation: sum, max, min, avg.
      if (aggType == "all") {
        concatAggCol(col, "sum")
        concatAggCol(col, "max")
        concatAggCol(col, "min")
        concatAggCol(col, "avg")
      }
      else {
        concatAggCol(col, aggType)
      }
    }
    resultString
  }

  // Function to generate a query for job level Task Metrics aggregation
  def jobtoStagesSQL: String = {
    s"""select $index as appIndex, j.jobID,
       |j.stageIds, j.sqlID
       |from jobDF_$index j
       |""".stripMargin
  }

  // Function to generate a query for job level Task Metrics aggregation
  def jobMetricsAggregationSQL: String = {
    s"""select $index as appIndex, concat('job_',j.jobID) as ID,
       |count(*) as numTasks, max(j.duration) as Duration
       |$generateAggSQLString
       |from taskDF_$index t, stageDF_$index s, jobDF_$index j
       |where t.stageId=s.stageId
       |and array_contains(j.stageIds, s.stageId)
       |group by j.jobID
       |""".stripMargin
  }

  // Function to generate a query for stage level Task Metrics aggregation
  def stageMetricsAggregationSQL: String = {
    s"""select $index as appIndex, concat('stage_',s.stageId) as ID,
       |count(*) as numTasks, max(s.duration) as Duration
       |$generateAggSQLString
       |from taskDF_$index t, stageDF_$index s
       |where t.stageId=s.stageId
       |group by s.stageId
       |""".stripMargin
  }

  // Function to generate a query for job+stage level Task Metrics aggregation
  def jobAndStageMetricsAggregationSQL: String = {
    jobMetricsAggregationSQL + " union " + stageMetricsAggregationSQL
  }

  // Function to generate a query for SQL level Task Metrics aggregation
  def sqlMetricsAggregationSQL: String = {
    s"""select $index as appIndex, '$appId' as appID,
       |sq.sqlID, sq.description,
       |count(*) as numTasks, max(sq.duration) as Duration,
       |sum(executorCPUTime) as executorCPUTime,
       |sum(executorRunTime) as executorRunTime,
       |round(sum(executorCPUTime)/sum(executorRunTime)*100,2) executorCPURatio
       |$generateAggSQLString
       |from taskDF_$index t, stageDF_$index s,
       |jobDF_$index j, sqlDF_$index sq
       |where t.stageId=s.stageId
       |and array_contains(j.stageIds, s.stageId)
       |and sq.sqlID=j.sqlID
       |group by sq.sqlID,sq.description
       |""".stripMargin
  }

  // Function to generate a query for getting the executor CPU time and run time
  // specifically for how we aggregate for qualification
  def sqlMetricsAggregationSQLQual: String = {
    s"""select $index as appIndex, '$appId' as appID,
       |sq.sqlID, sq.description,
       |sum(executorCPUTimeSum) as executorCPUTime,
       |sum(executorRunTimeSum) as executorRunTime
       |from stageDF_$index s,
       |jobDF_$index j, sqlDF_$index sq
       |where array_contains(j.stageIds, s.stageId)
       |and sq.sqlID=j.sqlID and sq.sqlID not in ($sqlIdsForUnsuccessfulJobs)
       |group by sq.sqlID,sq.description
       |""".stripMargin
  }

  // Function to generate a query for printing SQL metrics(accumulables)
  def generateSQLAccums: String = {
    s"""with allaccums as
       |(
       |select s.sqlID, p.nodeID, p.nodeName,
       |s.accumulatorId, s.name, d.value, s.metricType
       |from sqlMetricsDF_$index s, driverAccumDF_$index d,
       |planNodeAccumDF_$index p
       |where s.sqlID = d.sqlID and s.accumulatorId=d.accumulatorId
       |and s.sqlID=p.sqlID and s.accumulatorId=p.accumulatorId
       |union
       |select s.sqlID, p.nodeID, p.nodeName,
       |s.accumulatorId, s.name, t.value, s.metricType
       |from jobDF_$index j, sqlDF_$index sq ,
       |taskStageAccumDF_$index t, sqlMetricsDF_$index s,
       |planNodeAccumDF_$index p
       |where array_contains(j.stageIds, t.stageId)
       |and sq.sqlID=j.sqlID
       |and s.sqlID = sq.sqlID
       |and s.accumulatorId=t.accumulatorId
       |and s.sqlID=p.sqlID and s.accumulatorId=p.accumulatorId
       |)
       |select sqlID, nodeID, nodeName,
       |accumulatorId, name, max(value) as max_value, metricType
       |from allaccums
       |group by sqlID, nodeID, nodeName, accumulatorId, name, metricType
       |order by sqlID, nodeID, nodeName, accumulatorId, name, metricType
       |""".stripMargin
  }

  def getFailedTasks: String = {
    s"""select $index as appIndex, stageId, stageAttemptId, taskId, attempt,
       |substr(endReason, 1, 100) as failureReason
       |from taskDF_$index
       |where successful = false
       |order by appIndex, stageId, stageAttemptId, taskId, attempt
       |""".stripMargin
  }

  def getFailedStages: String = {
    s"""select $index as appIndex, stageId, attemptId, name, numTasks,
       |substr(failureReason, 1, 100) as failureReason
       |from stageDF_$index
       |where failureReason is not null
       |order by stageId, attemptId
       |""".stripMargin
  }

  def getFailedJobs: String = {
    s"""select $index as appIndex, jobID, jobResult,
       |substr(failedReason, 1, 100) as failureReason
       |from jobDF_$index
       |where jobResult <> 'JobSucceeded'
       |order by jobID
       |""".stripMargin
  }

  def getblockManagersRemoved: String = {
    s"""select executorID, time
       |from blockManagersRemovedDF_$index
       |order by cast(executorID as long)
       |""".stripMargin
  }

  def getExecutorsRemoved: String = {
    s"""select executorID, time,
       |substr(reason, 1, 100) reason_first100char
       |from executorsRemovedDF_$index
       |order by cast(executorID as long)
       |""".stripMargin
  }

  def unsupportedSQLPlan: String = {
    s"""select $index as appIndex, sqlID, nodeID, nodeName,
       |substr(nodeDesc, 1, 100) nodeDescription
       |from unsupportedSQLplan_$index""".stripMargin
  }

  def sqlIdsForUnsuccessfulJobs: String = {
    s"""select
       |sqlID
       |from jobDF_$index j
       |where j.jobResult != "JobSucceeded" or j.jobResult is null
       |""".stripMargin
  }

  def qualificationDurationNoMetricsSQL: String = {
    s"""select
       |first(appName) as `App Name`,
       |'$appId' as `App ID`,
       |ROUND((sum(sqlQualDuration) * 100) / first(app.duration), 2) as Score,
       |concat_ws(",", collect_set(problematic)) as `Potential Problems`,
       |sum(sqlQualDuration) as `SQL Dataframe Duration`,
       |first(app.duration) as `App Duration`,
       |first(app.endDurationEstimated) as `App Duration Estimated`
       |from sqlDF_$index sq, appdf_$index app
       |where sq.sqlID not in ($sqlIdsForUnsuccessfulJobs)
       |""".stripMargin
  }

  // only include jobs that are marked as succeeded
  def qualificationDurationSQL: String = {
    s"""select
       |$index as appIndex,
       |'$appId' as appID,
       |app.appName,
       |sq.sqlID, sq.description,
       |sq.sqlQualDuration as dfDuration,
       |app.duration as appDuration,
       |app.endDurationEstimated as appEndDurationEstimated,
       |problematic as potentialProblems,
       |m.executorCPUTime,
       |m.executorRunTime
       |from sqlDF_$index sq, appdf_$index app
       |left join sqlAggMetricsDF m on $index = m.appIndex and sq.sqlID = m.sqlID
       |where sq.sqlID not in ($sqlIdsForUnsuccessfulJobs)
       |""".stripMargin
  }

  def qualificationDurationSumSQL: String = {
    s"""select first(appName) as `App Name`,
       |'$appId' as `App ID`,
       |ROUND((sum(dfDuration) * 100) / first(appDuration), 2) as Score,
       |concat_ws(",", collect_set(potentialProblems)) as `Potential Problems`,
       |sum(dfDuration) as `SQL Dataframe Duration`,
       |first(appDuration) as `App Duration`,
       |round(sum(executorCPUTime)/sum(executorRunTime)*100,2) as `Executor CPU Time Percent`,
       |first(appEndDurationEstimated) as `App Duration Estimated`
       |from (${qualificationDurationSQL.stripLineEnd})
       |""".stripMargin
  }

  def profilingDurationSQL: String = {
    s"""select
       |$index as appIndex,
       |'$appId' as `App ID`,
       |sq.sqlID,
       |sq.duration as `SQL Duration`,
       |sq.hasDataset as `Contains Dataset Op`,
       |app.duration as `App Duration`,
       |problematic as `Potential Problems`,
       |round(executorCPUTime/executorRunTime*100,2) as `Executor CPU Time Percent`
       |from sqlDF_$index sq, appdf_$index app
       |left join sqlAggMetricsDF m on $index = m.appIndex and sq.sqlID = m.sqlID
       |""".stripMargin
  }
}

object ApplicationInfo extends Logging {
  def createApps(
      allPaths: Seq[EventLogInfo],
      numRows: Int,
      sparkSession: SparkSession,
      startIndex: Int = 1): (Seq[ApplicationInfo], Int) = {
    var index: Int = startIndex
    var errorCode = 0
    val apps = allPaths.flatMap { path =>
      try {
        // This apps only contains 1 app in each loop.
        val app = new ApplicationInfo(numRows, sparkSession, path, index)
        EventLogPathProcessor.logApplicationInfo(app)
        index += 1
        Some(app)
      } catch {
        case json: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON: $path")
          errorCode = 1
          None
        case il: IllegalArgumentException =>
          logWarning(s"Error parsing file: $path", il)
          errorCode = 2
          None
        case e: Exception =>
          // catch all exceptions and skip that file
          logWarning(s"Got unexpected exception processing file: $path", e)
          errorCode = 3
          None
      }
    }
    (apps, errorCode)
  }
}
