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
    val sparkProps: Seq[RapidsPropertyProfileResult])
