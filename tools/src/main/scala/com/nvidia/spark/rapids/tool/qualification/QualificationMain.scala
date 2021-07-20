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

package com.nvidia.spark.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.EventLogPathProcessor
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.AppFilterImpl
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo

/**
 * A tool to analyze Spark event logs and determine if 
 * they might be a good fit for running on the GPU.
 */
object QualificationMain extends Logging {

  def main(args: Array[String]) {
    val (exitCode, _) = mainInternal(new QualificationArgs(args), printStdout = true)
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(appArgs: QualificationArgs,
      writeOutput: Boolean = true,
      dropTempViews: Boolean = false,
      printStdout:Boolean = false): (Int, Seq[QualificationSummaryInfo]) = {

    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")
    val numOutputRows = appArgs.numOutputRows.getOrElse(1000)

    val nThreads = appArgs.numThreads.getOrElse(
      Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
    val timeout = appArgs.timeout.toOption
    val readScorePercent = appArgs.readScorePercent.getOrElse(20)
    val reportReadSchema = appArgs.reportReadSchema.getOrElse(false)
    val order = appArgs.order.getOrElse("desc")

    val hadoopConf = new Configuration()

    val pluginTypeChecker = try {
      if (readScorePercent > 0 || reportReadSchema) {
        Some(new PluginTypeChecker())
      } else {
        None
      }
    } catch {
      case ie: IllegalStateException =>
        logError("Error creating the plugin type checker!", ie)
        return (1, Seq[QualificationSummaryInfo]())
    }

    val eventLogInfos = EventLogPathProcessor.processAllPaths(filterN.toOption,
      matchEventLogs.toOption, eventlogPaths, hadoopConf)

    val filteredLogs = if (argsContainsAppFilters(appArgs)) {
      val appFilter = new AppFilterImpl(numOutputRows, hadoopConf, timeout, nThreads)
      val finaleventlogs = appFilter.filterEventLogs(eventLogInfos, appArgs)
      finaleventlogs
    } else {
      eventLogInfos
    }

    if (filteredLogs.isEmpty) {
      logWarning("No event logs to process after checking paths, exiting!")
      return (0, Seq[QualificationSummaryInfo]())
    }

    val qual = new Qualification(outputDirectory, numOutputRows, hadoopConf, timeout,
      nThreads, order, pluginTypeChecker, readScorePercent, reportReadSchema, printStdout)
    val res = qual.qualifyApps(filteredLogs)
    (0, res)
  }

  def argsContainsAppFilters(appArgs: QualificationArgs): Boolean = {
    val filterCriteria = appArgs.filterCriteria.toOption
    appArgs.applicationName.isSupplied || appArgs.startAppTime.isSupplied ||
        (filterCriteria.isDefined && (filterCriteria.get.endsWith("-newest") ||
            filterCriteria.get.endsWith("-oldest") || filterCriteria.get.endsWith("-per-app-name")))
  }
}
