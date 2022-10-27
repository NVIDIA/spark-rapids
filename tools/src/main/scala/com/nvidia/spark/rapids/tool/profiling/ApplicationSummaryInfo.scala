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

package com.nvidia.spark.rapids.tool.profiling

case class ApplicationSummaryInfo(
    val appInfo: Seq[AppInfoProfileResults],
    val dsInfo: Seq[DataSourceProfileResult],
    val execInfo: Seq[ExecutorInfoProfileResult],
    val jobInfo: Seq[JobInfoProfileResult],
    val rapidsProps: Seq[RapidsPropertyProfileResult],
    val rapidsJar: Seq[RapidsJarProfileResult],
    val sqlMetrics: Seq[SQLAccumProfileResults],
    val jsMetAgg: Seq[JobStageAggTaskMetricsProfileResult],
    val sqlTaskAggMetrics: Seq[SQLTaskAggMetricsProfileResult],
    val durAndCpuMet: Seq[SQLDurationExecutorTimeProfileResult],
    val skewInfo: Seq[ShuffleSkewProfileResult],
    val failedTasks: Seq[FailedTaskProfileResults],
    val failedStages: Seq[FailedStagesProfileResults],
    val failedJobs: Seq[FailedJobsProfileResults],
    val removedBMs: Seq[BlockManagerRemovedProfileResult],
    val removedExecutors: Seq[ExecutorsRemovedProfileResult],
    val unsupportedOps: Seq[UnsupportedOpsProfileResult],
    val sparkProps: Seq[RapidsPropertyProfileResult],
    val sqlStageInfo: Seq[SQLStageInfoProfileResult],
    val wholeStage: Seq[WholeStageCodeGenResults],
    val maxTaskInputBytesRead: Seq[SQLMaxTaskInputSizes],
    val appLogPath: Seq[AppLogPathProfileResults])

trait AppInfoPropertyGetter {
  def getSparkProperty(propKey: String): Option[String]
  def getRapidsProperty(propKey: String): Option[String]
  def getProperty(propKey: String): Option[String]
  def getSparkVersion: Option[String]
}

trait AppInfoSqlTaskAggMetricsVisitor {
  def getJvmGCFractions: Seq[Double]
  def getSpilledMetrics: Seq[Long]
}

trait AppInfoSQLMaxTaskInputSizes {
  def getMaxInput: Double
}

/**
 * A base class definition that provides an empty implementation of the profile results embedded in
 * [[ApplicationSummaryInfo]].
 */
class AppSummaryInfoBaseProvider extends AppInfoPropertyGetter
  with AppInfoSqlTaskAggMetricsVisitor
  with AppInfoSQLMaxTaskInputSizes {
  override def getSparkProperty(propKey: String): Option[String] = None
  override def getRapidsProperty(propKey: String): Option[String] = None
  override def getProperty(propKey: String): Option[String] = None
  override def getSparkVersion: Option[String] = None
  override def getMaxInput: Double = 0.0
  override def getJvmGCFractions: Seq[Double] = Seq()
  override def getSpilledMetrics: Seq[Long] = Seq()
}

/**
 * A wrapper class to process the information embedded in a valid instance of
 * [[ApplicationSummaryInfo]].
 * Note that this class does not handle combined mode and assume that the profiling results belong
 * to a single app.
 * @param app the object resulting from profiling a single app.
 */
class SingleAppSummaryInfoProvider(val app: ApplicationSummaryInfo)
  extends AppSummaryInfoBaseProvider {
  private def findPropertyInProfPropertyResults(
      key: String,
      props: Seq[RapidsPropertyProfileResult]): Option[String] = {
    props.collectFirst {
      case entry: RapidsPropertyProfileResult
        if entry.key == key && entry.rows(1) != "null" => entry.rows(1)
    }
  }

  override def getSparkProperty(propKey: String): Option[String] = {
    findPropertyInProfPropertyResults(propKey, app.sparkProps)
  }

  override def getRapidsProperty(propKey: String): Option[String] = {
    findPropertyInProfPropertyResults(propKey, app.rapidsProps)
  }

  override def getProperty(propKey: String): Option[String] = {
    if (propKey.startsWith("spark.rapids")) {
      getRapidsProperty(propKey)
    } else {
      getSparkProperty(propKey)
    }
  }

  override def getSparkVersion: Option[String] = {
    Option(app.appInfo.head.sparkVersion)
  }

  override def getJvmGCFractions: Seq[Double] = {
    app.sqlTaskAggMetrics.map {
      taskMetrics => taskMetrics.jvmGCTimeSum * 1.0 / taskMetrics.executorCpuTime
    }
  }

  override def getSpilledMetrics: Seq[Long] = {
    app.sqlTaskAggMetrics.map { task =>
      task.diskBytesSpilledSum + task.memoryBytesSpilledSum
    }
  }

  override def getMaxInput: Double = {
    if (app.maxTaskInputBytesRead.nonEmpty) {
      app.maxTaskInputBytesRead.head.maxTaskInputBytesRead
    } else {
      0.0
    }
  }
}