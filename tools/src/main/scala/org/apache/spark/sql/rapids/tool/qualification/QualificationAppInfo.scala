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

package org.apache.spark.sql.rapids.tool.qualification

import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo, SQLPlanParser}
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AppBase, GpuEventLogException, ToolUtils}

class QualificationAppInfo(
    eventLogInfo: Option[EventLogInfo],
    hadoopConf: Option[Configuration] = None,
    pluginTypeChecker: PluginTypeChecker)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  var appId: String = ""
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  var longestSQLDuration: Long = 0
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]
  val stageIdToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoFailures: HashMap[Long, ArrayBuffer[String]] = HashMap.empty[Long, ArrayBuffer[String]]

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()
  var sqlPlans: HashMap[Long, SparkPlanInfo] = HashMap.empty[Long, SparkPlanInfo]

  private lazy val eventProcessor =  new QualificationEventProcessor(this)

  /**
   * Get the event listener the qualification tool uses to process Spark events.
   * Install this listener in Spark.
   *
   * {{{
   *   spark.sparkContext.addSparkListener(listener)
   * }}}
   * @return SparkListener
   */
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

  // Assume that overhead is the all time windows that do not overlap with a running job.
  private def calculateJobOverHeadTime(startTime: Long): Long = {
    // Simple algorithm:
    // 1- sort all jobs by start/endtime.
    // 2- Initialize Time(p) = app.StartTime
    // 3- loop on the sorted seq. if the job.startTime is larger than the current Time(p):
    //    then this must be considered a gap
    // 4- Update Time(p) at the end of each iteration: Time(p+1) = Max(Time(p), job.endTime)
    val sortedJobs = jobIdToInfo.values.toSeq.sortBy(_.startTime)
    var pivot = startTime
    var overhead : Long = 0

    sortedJobs.foreach(job => {
      val timeDiff = job.startTime - pivot
      if (timeDiff > 0) {
        overhead += timeDiff
      }
      // if jobEndTime is not set, use job.startTime
      pivot = Math max(pivot, job.endTime.getOrElse(job.startTime))
    })
    overhead
  }

  // Look at the total task times for all jobs/stages that aren't SQL or
  // SQL but dataset or rdd
  private def calculateNonSQLTaskDataframeDuration(taskDFDuration: Long): Long = {
    val allTaskTime = stageIdToTaskEndSum.values.map(_.totalTaskDuration).sum
    val res = allTaskTime - taskDFDuration
    assert(res >= 0)
    res
  }

  private def calculateCpuTimePercent(perSqlStageSummary: Seq[SQLStageSummary]): Double = {
    val totalCpuTime = perSqlStageSummary.map(_.execCPUTime).sum
    val totalRunTime = perSqlStageSummary.map(_.execRunTime).sum
    ToolUtils.calculateDurationPercent(totalCpuTime, totalRunTime)
  }

  private def calculateSQLSupportedTaskDuration(all: Seq[Set[StageQualSummaryInfo]]): Long = {
    all.flatMap(_.map(s => s.stageTaskTime - s.unsupportedTaskDur)).sum
  }

  private def calculateSQLUnsupportedTaskDuration(all: Seq[Set[StageQualSummaryInfo]]): Long = {
    all.flatMap(_.map(_.unsupportedTaskDur)).sum
  }

  private def calculateSpeedupFactor(all: Seq[Set[StageQualSummaryInfo]]): Double = {
    val allSpeedupFactors = all.flatMap(_.map(_.averageSpeedup))
    val res = SQLPlanParser.averageSpeedup(allSpeedupFactors)
    res
  }

  private def getAllReadFileFormats: String = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }.mkString(":")
  }

  private def getStageToExec(execInfos: Seq[ExecInfo]): Map[Int, Seq[ExecInfo]] = {
    execInfos.flatMap { execInfo =>
      if (execInfo.stages.size > 1) {
        execInfo.stages.map((_, execInfo))
      } else if (execInfo.stages.size < 1) {
        // we don't know what stage its in our its duration
        logDebug(s"No stage associated with ${execInfo.exec} " +
          s"so speedup factor isn't applied anywhere.")
        Seq.empty
      } else {
        Seq((execInfo.stages.head, execInfo))
      }
    }.groupBy(_._1).map { case (k, v) =>
      (k, v.map(_._2))
    }
  }

  def summarizeStageLevel(allStagesToExecs: Map[Int, Seq[ExecInfo]]): Set[StageQualSummaryInfo] = {
    // if it doesn't have a stage id associated we can't calculate the time spent in that
    // SQL so we just drop it
    val stageIds = allStagesToExecs.keys.toSet
    stageIds.map { stageId =>
      val stageTaskTime = stageIdToTaskEndSum.get(stageId)
        .map(_.totalTaskDuration).getOrElse(0L)
      val execsForStage = allStagesToExecs.getOrElse(stageId, Seq.empty)
      val speedupFactors = execsForStage.map(_.speedupFactor)
      val averageSpeedupFactor = SQLPlanParser.averageSpeedup(speedupFactors)
      // need to remove the WholeStageCodegen wrappers since they aren't actual
      // execs that we want to get timings of
      val allFlattenedExecs = execsForStage.flatMap { e =>
        if (e.exec.contains("WholeStageCodegen")) {
          e.children.getOrElse(Seq.empty)
        } else {
          e.children.getOrElse(Seq.empty) :+ e
        }
      }
      val numUnsupported = allFlattenedExecs.filterNot(_.isSupported)
      // if we have unsupported try to guess at how much time.  For now divide
      // time by number of execs and give each one equal weight
      val eachExecTime = stageTaskTime / allFlattenedExecs.size
      val unsupportedDur = eachExecTime * numUnsupported.size

      StageQualSummaryInfo(stageId, averageSpeedupFactor, stageTaskTime, unsupportedDur)
    }
  }

  def summarizeSQLStageInfo(planInfos: Seq[PlanInfo]): Seq[SQLStageSummary] = {
    planInfos.flatMap { pInfo =>
      val perSQLId = pInfo.execInfo.groupBy(_.sqlID)
      perSQLId.map { case (sqlID, execInfos) =>
        val sqlWallClockDuration = sqlIdToInfo.get(sqlID).flatMap(_.duration).getOrElse(0L)
        // there are issues with duration in whole stage code gen where duration of multiple
        // execs is more than entire stage time, for now ignore the exec duration and just
        // calculate based on average applied to total task time of each stage
        val allStagesToExecs = getStageToExec(execInfos)
        // if it doesn't have a stage id associated we can't calculate the time spent in that
        // SQL so we just drop it
        val stageSum = summarizeStageLevel(allStagesToExecs)
        val numUnsupportedExecs = execInfos.filterNot(_.isSupported).size
        // This is a guestimate at how much wall clock was supported
        val numExecs = execInfos.size.toDouble
        val numSupportedExecs = (numExecs - numUnsupportedExecs).toDouble
        val ratio = numSupportedExecs / numExecs
        val hackEstimateWallclockSupported = (sqlWallClockDuration * ratio).toInt
        if (hackEstimateWallclockSupported > longestSQLDuration) {
          longestSQLDuration = hackEstimateWallclockSupported
        }
        // TODO - do we need to estimate based on supported execs?
        // for now just take the time as is
        val execRunTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorRunTime).getOrElse(0L)
        val execCPUTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorCPUTime).getOrElse(0L)

        SQLStageSummary(stageSum, sqlID, hackEstimateWallclockSupported,
          execCPUTime, execRunTime)
      }
    }
  }

  def removeExecsShouldRemove(origPlanInfos: Seq[PlanInfo]): Seq[PlanInfo] = {
    origPlanInfos.map { p =>
      val execFilteredChildren = p.execInfo.map { e =>
        val filteredChildren = e.children.map { c =>
          c.filterNot(_.shouldRemove)
        }
        e.copy(children = filteredChildren)
      }
      val filteredPlanInfos = execFilteredChildren.filterNot(_.shouldRemove)
      p.copy(execInfo = filteredPlanInfos)
    }
  }

  /**
   * Aggregate and process the application after reading the events.
   * @return Option of QualificationSummaryInfo, Some if we were able to process the application
   *         otherwise None.
   */
  def aggregateStats(): Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)

      // if either job or stage failures then we mark as N/A
      val sqlIdsWithFailures = sqlIDtoFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")

      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case (format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.mkString(";")
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      val origPlanInfos = sqlPlans.map { case (id, plan) =>
        SQLPlanParser.parseSQLPlan(appId, plan, id, pluginTypeChecker, this)
      }.toSeq

      // filter out any execs that should be removed
      val planInfos = removeExecsShouldRemove(origPlanInfos)
      // get a summary of each SQL Query
      val perSqlStageSummary = summarizeSQLStageInfo(planInfos)
      // wall clock time
      val executorCpuTimePercent = calculateCpuTimePercent(perSqlStageSummary)

      val sqlDFWallClockDuration =
        perSqlStageSummary.map(s => s.hackEstimateWallclockSupported).sum
      val allStagesSummary = perSqlStageSummary.map(_.stageSum)
      val sqlDataframeTaskDuration = allStagesSummary.flatMap(_.map(s => s.stageTaskTime)).sum
      val unsupportedSQLTaskDuration = calculateSQLUnsupportedTaskDuration(allStagesSummary)
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val jobOverheadTime = calculateJobOverHeadTime(info.startTime)
      val nonSQLDataframeTaskDuration =
        calculateNonSQLTaskDataframeDuration(sqlDataframeTaskDuration)
      val nonSQLTaskDuration = nonSQLDataframeTaskDuration + jobOverheadTime
      val supportedSQLTaskDuration = calculateSQLSupportedTaskDuration(allStagesSummary)
      val taskSpeedupFactor = calculateSpeedupFactor(allStagesSummary)

      // get the ratio based on the Task durations that we will use for wall clock durations
      val estimatedGPURatio = if (sqlDataframeTaskDuration > 0) {
        supportedSQLTaskDuration.toDouble / sqlDataframeTaskDuration.toDouble
      } else {
        1
      }

      val appName = appInfo.map(_.appName).getOrElse("")
      val estimatedInfo = QualificationAppInfo.calculateEstimatedInfoSummary(estimatedGPURatio,
        sqlDFWallClockDuration, appDuration, taskSpeedupFactor, appName, appId,
        sqlIdsWithFailures.nonEmpty)

      QualificationSummaryInfo(info.appName, appId, problems,
        executorCpuTimePercent, endDurationEstimated, sqlIdsWithFailures,
        notSupportFormatAndTypesString, getAllReadFileFormats, writeFormat,
        allComplexTypes, nestedComplexTypes, longestSQLDuration, nonSQLTaskDuration,
        sqlDataframeTaskDuration, unsupportedSQLTaskDuration, supportedSQLTaskDuration,
        taskSpeedupFactor, info.sparkUser, info.startTime, origPlanInfos,
        perSqlStageSummary.map(_.stageSum).flatten, estimatedInfo)
    }
  }
  // case class TaskTimeSummaryInfo(sqlDataframeTaskDuration: Long, nonSQLTaskDuration: Long)

  private[qualification] def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    checkMetadataForReadSchema(sqlID, planInfo)
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      checkGraphNodeForReads(sqlID, node)
      // Get the write data format
      if (node.name.contains("InsertIntoHadoopFsRelationCommand")) {
        val writeFormat = node.desc.split(",")(2)
        writeDataFormat += writeFormat
      }
    }
  }

  private def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): String = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.isWriteFormatsupported(writeFormat)
    unSupportedWriteFormat.distinct.mkString(";").toUpperCase
  }
}

// Estimate based on wall clock times
case class EstimatedSummaryInfo(
    appName: String,
    appId: String,
    appDur: Long,
    sqlDfDuration: Long,
    gpuOpportunity: Long, // Projected opportunity for acceleration on GPU in ms
    estimatedGpuDur: Double, // Predicted runtime for the app if it was run on the GPU
    estimatedGpuSpeedup: Double, // app_duration / estimated_gpu_duration
    estimatedGpuTimeSaved: Double, // app_duration - estimated_gpu_duration
    recommendation: Recommendation.Recommendation)

case class SQLStageSummary(
    stageSum: Set[StageQualSummaryInfo],
    sqlID: Long,
    hackEstimateWallclockSupported: Long,
    execCPUTime: Long,
    execRunTime: Long)

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
    potentialProblems: String,
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    longestSqlDuration: Long,
    sqlDataframeTaskDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    user: String,
    startTime: Long,
    planInfo: Seq[PlanInfo],
    stageInfo: Seq[StageQualSummaryInfo],
    estimatedInfo: EstimatedSummaryInfo)

case class StageQualSummaryInfo(
    stageId: Int,
    averageSpeedup: Double,
    stageTaskTime: Long,
    unsupportedTaskDur: Long)

object Recommendation extends Enumeration {
  type Recommendation = Value

  val Strongly_Recommended = Value(3, "Strongly_Recommended")
  val Recommended = Value(2, "Recommended")
  val Not_Recommended = Value(1, "Not_Recommended")
  val Not_Applicable = Value(0, "Not_Applicable")
}

object QualificationAppInfo extends Logging {

  def getRecommendation(totalSpeedup: Double,
      hasFailures: Boolean): Recommendation.Recommendation = {
    if (hasFailures) {
      Recommendation.Not_Applicable
    } else if (totalSpeedup >= 2.5) {
      Recommendation.Strongly_Recommended
    } else if (totalSpeedup >= 1.25) {
      Recommendation.Recommended
    } else {
      Recommendation.Not_Recommended
    }
  }

  // Summarize and estimate based on wall clock times
  def calculateEstimatedInfoSummary(estimatedRatio: Double, sqlDataFrameDuration: Long,
      appDuration: Long, speedupFactor: Double, appName: String,
      appId: String, hasFailures: Boolean): EstimatedSummaryInfo = {
    val speedupOpportunityWallClock = sqlDataFrameDuration * estimatedRatio
    val estimated_wall_clock_dur_not_on_gpu = appDuration - speedupOpportunityWallClock
    val estimated_gpu_duration =
      (speedupOpportunityWallClock / speedupFactor) + estimated_wall_clock_dur_not_on_gpu
    val estimated_gpu_speedup = appDuration / estimated_gpu_duration
    val estimated_gpu_timesaved = appDuration - estimated_gpu_duration
    val recommendation = getRecommendation(estimated_gpu_speedup, hasFailures)

    EstimatedSummaryInfo(appName, appId, appDuration,
      sqlDataFrameDuration, speedupOpportunityWallClock.toLong,
      estimated_gpu_duration, estimated_gpu_speedup,
      estimated_gpu_timesaved, recommendation)
  }

  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: PluginTypeChecker): Option[QualificationAppInfo] = {
    val app = try {
        val app = new QualificationAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker)
        logInfo(s"${path.eventLog.toString} has App: ${app.appId}")
        Some(app)
      } catch {
        case gpuLog: GpuEventLogException =>
          logWarning(gpuLog.message)
          None
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
}
