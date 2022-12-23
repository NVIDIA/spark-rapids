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
    pluginTypeChecker: PluginTypeChecker,
    reportSqlLevel: Boolean,
    perSqlOnly: Boolean = false)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  var appId: String = ""
  var lastJobEndTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  var appInfo: Option[QualApplicationInfo] = None

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]
  val stageIdToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoFailures: HashMap[Long, ArrayBuffer[String]] = HashMap.empty[Long, ArrayBuffer[String]]

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()

  var clusterTags: String = ""
  private lazy val eventProcessor =  new QualificationEventProcessor(this, perSqlOnly)

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

  override def cleanupStages(stageIds: Set[Int]): Unit = {
    stageIds.foreach { stageId =>
      stageIdToTaskEndSum.remove(stageId)
      stageIdToSqlID.remove(stageId)
    }
    super.cleanupStages(stageIds)
  }

  override def cleanupSQL(sqlID: Long): Unit = {
    sqlIDToTaskEndSum.remove(sqlID)
    sqlIDtoFailures.remove(sqlID)
    super.cleanupSQL(sqlID)
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

    sortedJobs.foreach { job =>
      val timeDiff = job.startTime - pivot
      if (timeDiff > 0) {
        overhead += timeDiff
      }
      // if jobEndTime is not set, use job.startTime
      pivot = Math.max(pivot, job.endTime.getOrElse(job.startTime))
    }
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

  private def calculateSQLSupportedTaskDuration(all: Seq[StageQualSummaryInfo]): Long = {
    all.map(s => s.stageTaskTime - s.unsupportedTaskDur).sum
  }

  private def calculateSQLUnsupportedTaskDuration(all: Seq[StageQualSummaryInfo]): Long = {
    all.map(_.unsupportedTaskDur).sum
  }

  private def calculateSpeedupFactor(all: Seq[StageQualSummaryInfo]): Double = {
    val allSpeedupFactors = all.filter(_.stageTaskTime > 0).map(_.averageSpeedup)
    val res = SQLPlanParser.averageSpeedup(allSpeedupFactors)
    res
  }

  private def getAllReadFileFormats: Seq[String] = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }
  }

  protected def checkUnsupportedReadFormats(): Unit = {
    if (dataSourceInfo.size > 0) {
      dataSourceInfo.map { ds =>
        val (_, nsTypes) = pluginTypeChecker.scoreReadDataTypes(ds.format, ds.schema)
        if (nsTypes.nonEmpty) {
          val currentFormat = notSupportFormatAndTypes.get(ds.format).getOrElse(Set.empty[String])
          notSupportFormatAndTypes(ds.format) = (currentFormat ++ nsTypes)
        }
      }
    }
  }

  private def getStageToExec(execInfos: Seq[ExecInfo]): (Map[Int, Seq[ExecInfo]], Seq[ExecInfo]) = {
    val execsWithoutStages = new ArrayBuffer[ExecInfo]()
    val perStageSum = execInfos.flatMap { execInfo =>
      if (execInfo.stages.size > 1) {
        execInfo.stages.map((_, execInfo))
      } else if (execInfo.stages.size < 1) {
        // we don't know what stage its in or its duration
        logDebug(s"No stage associated with ${execInfo.exec} " +
          s"so speedup factor isn't applied anywhere.")
        execsWithoutStages += execInfo
        Seq.empty
      } else {
        Seq((execInfo.stages.head, execInfo))
      }
    }.groupBy(_._1).map { case (stage, execInfos) =>
      (stage, execInfos.map(_._2))
    }
    (perStageSum, execsWithoutStages.toSeq)
  }

  private def flattenedExecs(execs: Seq[ExecInfo]): Seq[ExecInfo] = {
    // need to remove the WholeStageCodegen wrappers since they aren't actual
    // execs that we want to get timings of
    execs.flatMap { e =>
      if (e.exec.contains("WholeStageCodegen")) {
        e.children.getOrElse(Seq.empty)
      } else {
        e.children.getOrElse(Seq.empty) :+ e
      }
    }
  }

  private def stagesSummary(execInfos: Seq[ExecInfo],
      stages: Seq[Int], estimated: Boolean): Set[StageQualSummaryInfo] = {
    val allStageTaskTime = stages.map { stageId =>
      stageIdToTaskEndSum.get(stageId).map(_.totalTaskDuration).getOrElse(0L)
    }.sum
    val allSpeedupFactorAvg = SQLPlanParser.averageSpeedup(execInfos.map(_.speedupFactor))
    val allFlattenedExecs = flattenedExecs(execInfos)
    val numUnsupported = allFlattenedExecs.filterNot(_.isSupported)
    // if we have unsupported try to guess at how much time.  For now divide
    // time by number of execs and give each one equal weight
    val eachExecTime = allStageTaskTime / allFlattenedExecs.size
    val unsupportedDur = eachExecTime * numUnsupported.size
    // split unsupported per stage
    val eachStageUnsupported = unsupportedDur / stages.size
    stages.map { stageId =>
      val stageTaskTime = stageIdToTaskEndSum.get(stageId)
        .map(_.totalTaskDuration).getOrElse(0L)
      StageQualSummaryInfo(stageId, allSpeedupFactorAvg, stageTaskTime,
        eachStageUnsupported, estimated)
    }.toSet
  }

  def summarizeStageLevel(execInfos: Seq[ExecInfo], sqlID: Long): Set[StageQualSummaryInfo] = {
    val (allStagesToExecs, execsNoStage) = getStageToExec(execInfos)
    if (allStagesToExecs.isEmpty) {
      // use job level
      // also get the job ids associated with the SQLId
      val allStagesBasedOnJobs = getAllStagesForJobsInSqlQuery(sqlID)
      if (allStagesBasedOnJobs.isEmpty) {
        Set.empty
      } else {
        // we don't know which execs map to each stage so we are going to cheat somewhat and
        // apply equally and then just split apart for per stage reporting
        stagesSummary(execInfos, allStagesBasedOnJobs, true)
      }
    } else {
      val stageIdsWithExecs = allStagesToExecs.keys.toSet
      val allStagesBasedOnJobs = getAllStagesForJobsInSqlQuery(sqlID)
      val stagesNotAccounted = allStagesBasedOnJobs.toSet -- stageIdsWithExecs
      val stageSummaryNotMapped = if (stagesNotAccounted.nonEmpty) {
        if (execsNoStage.nonEmpty) {
          stagesSummary(execsNoStage, stagesNotAccounted.toSeq, true)
        } else {
          // weird case, stages but not associated execs, not sure what to do with this so
          // just drop for now
          Set.empty
        }
      } else {
        Set.empty
      }
      // if it doesn't have a stage id associated we can't calculate the time spent in that
      // SQL so we just drop it
      val stagesFromExecs = stageIdsWithExecs.flatMap { stageId =>
        val execsForStage = allStagesToExecs.getOrElse(stageId, Seq.empty)
        stagesSummary(execsForStage, Seq(stageId), false)
      }
      stagesFromExecs ++ stageSummaryNotMapped
    }
  }

  def summarizeSQLStageInfo(planInfos: Seq[PlanInfo]): Seq[SQLStageSummary] = {
    planInfos.flatMap { pInfo =>
      val perSQLId = pInfo.execInfo.groupBy(_.sqlID)
      perSQLId.map { case (sqlID, execInfos) =>
        val sqlWallClockDuration = sqlIdToInfo.get(sqlID).flatMap(_.duration).getOrElse(0L)
        // There are issues with duration in whole stage code gen where duration of multiple
        // execs is more than entire stage time, for now ignore the exec duration and just
        // calculate based on average applied to total task time of each stage.
        // Also note that the below can map multiple stages to the same exec for things like
        // shuffle.

        // if it doesn't have a stage id associated we can't calculate the time spent in that
        // SQL so we just drop it
        val stageSum = summarizeStageLevel(execInfos, sqlID)

        val numUnsupportedExecs = execInfos.filterNot(_.isSupported).size
        // This is a guestimate at how much wall clock was supported
        val numExecs = execInfos.size.toDouble
        val numSupportedExecs = (numExecs - numUnsupportedExecs).toDouble
        val ratio = numSupportedExecs / numExecs
        val estimateWallclockSupported = (sqlWallClockDuration * ratio).toInt
        // don't worry about supported execs for these are these are mostly indicator of I/O
        val execRunTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorRunTime).getOrElse(0L)
        val execCPUTime = sqlIDToTaskEndSum.get(sqlID).map(_.executorCPUTime).getOrElse(0L)
        SQLStageSummary(stageSum, sqlID, estimateWallclockSupported,
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
        new ExecInfo(e.sqlID, e.exec, e.expr, e.speedupFactor, e.duration,
          e.nodeId, e.isSupported, filteredChildren, e.stages, e.shouldRemove)
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
      // TODO - what about incomplete, do we want to change those?
      val sqlIdsWithFailures = sqlIDtoFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.map(_.toString).toSeq

      // a bit odd but force filling in notSupportFormatAndTypes
      // TODO - make this better
      checkUnsupportedReadFormats()
      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case (format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.toSeq
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      val origPlanInfos = sqlPlans.map { case (id, plan) =>
        val sqlDesc = sqlIdToInfo(id).description
        SQLPlanParser.parseSQLPlan(appId, plan, id, sqlDesc, pluginTypeChecker, this)
      }.toSeq

      // filter out any execs that should be removed
      val planInfos = removeExecsShouldRemove(origPlanInfos)
      // get a summary of each SQL Query
      val perSqlStageSummary = summarizeSQLStageInfo(planInfos)
      // wall clock time
      val executorCpuTimePercent = calculateCpuTimePercent(perSqlStageSummary)

      // Using the spark SQL reported duration, this could be a bit off from the
      // task times because it relies on the stage times and we might not have
      // a stage for every exec
      val allSQLDurations = sqlIdToInfo.map { case (_, info) =>
        info.duration.getOrElse(0L)
      }

      val appName = appInfo.map(_.appName).getOrElse("")

      val allClusterTagsMap = if (clusterTags.nonEmpty) {
        ToolUtils.parseClusterTags(clusterTags)
      } else {
        Map.empty[String, String]
      }
      val perSqlInfos = if (reportSqlLevel) {
        Some(planInfos.flatMap { pInfo =>
          sqlIdToInfo.get(pInfo.sqlID).map { info =>
            val wallClockDur = info.duration.getOrElse(0L)
            // get task duration ratio
            val sqlStageSums = perSqlStageSummary.filter(_.sqlID == pInfo.sqlID)
            val estimatedInfo = getPerSQLWallClockSummary(sqlStageSums, wallClockDur,
              sqlIDtoFailures.get(pInfo.sqlID).nonEmpty, appName)
            EstimatedPerSQLSummaryInfo(pInfo.sqlID, pInfo.sqlDesc, estimatedInfo)
          }
        })
      } else {
        None
      }

      val sparkSQLDFWallClockDuration = allSQLDurations.sum
      val longestSQLDuration = if (allSQLDurations.size > 0) {
        allSQLDurations.max
      } else {
        0L
      }

      // the same stage might be referenced from multiple sql queries, we have to dedup them
      // with the assumption the stage was reused so time only counts once
      val allStagesSummary = perSqlStageSummary.flatMap(_.stageSum)
        .map(sum => sum.stageId -> sum).toMap.values.toSeq
      val sqlDataframeTaskDuration = allStagesSummary.map(s => s.stageTaskTime).sum
      val unsupportedSQLTaskDuration = calculateSQLUnsupportedTaskDuration(allStagesSummary)
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val jobOverheadTime = calculateJobOverHeadTime(info.startTime)
      val nonSQLDataframeTaskDuration =
        calculateNonSQLTaskDataframeDuration(sqlDataframeTaskDuration)
      val nonSQLTaskDuration = nonSQLDataframeTaskDuration + jobOverheadTime
      // note that these ratios are based off the stage times which may be missing some stage
      // overhead or execs that didn't have associated stages
      val supportedSQLTaskDuration = calculateSQLSupportedTaskDuration(allStagesSummary)
      val taskSpeedupFactor = calculateSpeedupFactor(allStagesSummary)
      // Get all the unsupported Execs from the plan
      val unSupportedExecs = origPlanInfos.flatMap { p =>
        // WholeStageCodeGen is excluded from the result.
        val topLevelExecs = p.execInfo.filterNot(_.isSupported).filterNot(
          x => x.exec.startsWith("WholeStage"))
        val childrenExecs = p.execInfo.flatMap { e =>
          e.children.map(x => x.filterNot(_.isSupported))
        }.flatten
        topLevelExecs ++ childrenExecs
      }.map(_.exec).toSet.mkString(";").trim
      // Get all the unsupported Expressions from the plan
      val unSupportedExprs = origPlanInfos.map(_.execInfo.flatMap(
        _.unsupportedExprs)).flatten.filter(_.nonEmpty).toSet.mkString(";").trim

      // get the ratio based on the Task durations that we will use for wall clock durations
      val estimatedGPURatio = if (sqlDataframeTaskDuration > 0) {
        supportedSQLTaskDuration.toDouble / sqlDataframeTaskDuration.toDouble
      } else {
        1
      }

      val estimatedInfo = QualificationAppInfo.calculateEstimatedInfoSummary(estimatedGPURatio,
        sparkSQLDFWallClockDuration, appDuration, taskSpeedupFactor, appName, appId,
        sqlIdsWithFailures.nonEmpty, unSupportedExecs, unSupportedExprs, allClusterTagsMap)

      QualificationSummaryInfo(info.appName, appId, problems,
        executorCpuTimePercent, endDurationEstimated, sqlIdsWithFailures,
        notSupportFormatAndTypesString, getAllReadFileFormats, writeFormat,
        allComplexTypes, nestedComplexTypes, longestSQLDuration, sqlDataframeTaskDuration,
        nonSQLTaskDuration, unsupportedSQLTaskDuration, supportedSQLTaskDuration,
        taskSpeedupFactor, info.sparkUser, info.startTime, origPlanInfos,
        perSqlStageSummary.map(_.stageSum).flatten, estimatedInfo, perSqlInfos,
        unSupportedExecs, unSupportedExprs, clusterTags, allClusterTagsMap)
    }
  }

  def getPerSQLWallClockSummary(sqlStageSums: Seq[SQLStageSummary], sqlDataFrameDuration: Long,
      hasFailures: Boolean, appName: String): EstimatedSummaryInfo = {
    val allStagesSummary = sqlStageSums.flatMap(_.stageSum)
    val sqlDataframeTaskDuration = allStagesSummary.map(_.stageTaskTime).sum
    val supportedSQLTaskDuration = calculateSQLSupportedTaskDuration(allStagesSummary)
    val taskSpeedupFactor = calculateSpeedupFactor(allStagesSummary)

    // get the ratio based on the Task durations that we will use for wall clock durations
    val estimatedGPURatio = if (sqlDataframeTaskDuration > 0) {
      supportedSQLTaskDuration.toDouble / sqlDataframeTaskDuration.toDouble
    } else {
      1
    }
    // reusing the same function here (calculateEstimatedInfoSummary) as the app level,
    // there is no app duration so just set it to sqlDataFrameDuration
    QualificationAppInfo.calculateEstimatedInfoSummary(estimatedGPURatio,
      sqlDataFrameDuration, sqlDataFrameDuration, taskSpeedupFactor, appName,
      appId, hasFailures)
  }

  private[qualification] def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    checkMetadataForReadSchema(sqlID, planInfo)
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      checkGraphNodeForReads(sqlID, node)
      val issues = findPotentialIssues(node.desc)
      if (issues.nonEmpty) {
        val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
        sqlIDtoProblematic(sqlID) = existingIssues ++ issues
      }
      // Get the write data format
      if (!perSqlOnly && node.name.contains("InsertIntoHadoopFsRelationCommand")) {
        writeDataFormat += pluginTypeChecker.getWriteFormatString(node.desc)
      }
    }
  }

  private def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): Seq[String] = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.isWriteFormatsupported(writeFormat)
    unSupportedWriteFormat.distinct
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
    recommendation: String,
    unsupportedExecs: String,
    unsupportedExprs: String,
    allTagsMap: Map[String, String])

// Estimate based on wall clock times for each SQL query
case class EstimatedPerSQLSummaryInfo(
    sqlID: Long,
    sqlDesc: String,
    info: EstimatedSummaryInfo)

case class SQLStageSummary(
    stageSum: Set[StageQualSummaryInfo],
    sqlID: Long,
    estimateWallClockSupported: Long,
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
    potentialProblems: Seq[String],
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    failedSQLIds: Seq[String],
    readFileFormatAndTypesNotSupported: Seq[String],
    readFileFormats: Seq[String],
    writeDataFormat: Seq[String],
    complexTypes: Seq[String],
    nestedComplexTypes: Seq[String],
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
    estimatedInfo: EstimatedSummaryInfo,
    perSQLEstimatedInfo: Option[Seq[EstimatedPerSQLSummaryInfo]],
    unSupportedExecs: String,
    unSupportedExprs: String,
    clusterTags: String,
    allClusterTagsMap: Map[String, String])

case class StageQualSummaryInfo(
    stageId: Int,
    averageSpeedup: Double,
    stageTaskTime: Long,
    unsupportedTaskDur: Long,
    estimated: Boolean = false)

object QualificationAppInfo extends Logging {
  // define recommendation constants
  val RECOMMENDED = "Recommended"
  val NOT_RECOMMENDED = "Not Recommended"
  val STRONGLY_RECOMMENDED = "Strongly Recommended"
  val NOT_APPLICABLE = "Not Applicable"
  val LOWER_BOUND_RECOMMENDED = 1.3
  val LOWER_BOUND_STRONGLY_RECOMMENDED = 2.5

  def getRecommendation(totalSpeedup: Double,
      hasFailures: Boolean): String = {
    if (hasFailures) {
      NOT_APPLICABLE
    } else if (totalSpeedup >= LOWER_BOUND_STRONGLY_RECOMMENDED) {
      STRONGLY_RECOMMENDED
    } else if (totalSpeedup >= LOWER_BOUND_RECOMMENDED) {
      RECOMMENDED
    } else {
      NOT_RECOMMENDED
    }
  }

  // Summarize and estimate based on wall clock times
  def calculateEstimatedInfoSummary(estimatedRatio: Double, sqlDataFrameDuration: Long,
      appDuration: Long, speedupFactor: Double, appName: String,
      appId: String, hasFailures: Boolean, unsupportedExecs: String = "",
      unsupportedExprs: String = "",
      allClusterTagsMap: Map[String, String] = Map.empty[String, String]): EstimatedSummaryInfo = {
    val sqlDataFrameDurationToUse = if (sqlDataFrameDuration > appDuration) {
      // our app duration is shorter then our sql duration, estimate the sql duration down
      // to app duration
      appDuration
    } else {
      sqlDataFrameDuration
    }
    val speedupOpportunityWallClock = sqlDataFrameDurationToUse * estimatedRatio
    val estimated_wall_clock_dur_not_on_gpu = appDuration - speedupOpportunityWallClock
    val estimated_gpu_duration =
      (speedupOpportunityWallClock / speedupFactor) + estimated_wall_clock_dur_not_on_gpu
    val estimated_gpu_speedup = if (appDuration == 0 || estimated_gpu_duration == 0) {
      0
    } else {
      appDuration / estimated_gpu_duration
    }
    val estimated_gpu_timesaved = appDuration - estimated_gpu_duration
    val recommendation = getRecommendation(estimated_gpu_speedup, hasFailures)

    // truncate the double fields to double precision to ensure that unit-tests do not explicitly
    // set the format to match the output. Removing the truncation from here requires modifying
    // TestQualificationSummary to truncate the same fields to match the CSV static samples.
    EstimatedSummaryInfo(appName, appId, appDuration,
      sqlDataFrameDurationToUse,
      speedupOpportunityWallClock.toLong,
      estimated_gpu_duration,
      estimated_gpu_speedup,
      estimated_gpu_timesaved,
      recommendation,
      unsupportedExecs,
      unsupportedExprs,
      allClusterTagsMap)
  }

  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: PluginTypeChecker,
      reportSqlLevel: Boolean): Option[QualificationAppInfo] = {
    val app = try {
        val app = new QualificationAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker,
          reportSqlLevel, false)
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
