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
    val (exitCode, _) =
      mainInternal(new QualificationArgs(args), printStdout = true, enablePB = true)
    if (exitCode != 0) {
      System.exit(exitCode)
    }
  }

  /**
   * Entry point for tests
   */
  def mainInternal(appArgs: QualificationArgs,
      printStdout: Boolean = false,
      enablePB: Boolean = false): (Int, Seq[QualificationSummaryInfo]) = {

    val eventlogPaths = appArgs.eventlog()
    val filterN = appArgs.filterCriteria
    val matchEventLogs = appArgs.matchEventLogs
    val outputDirectory = appArgs.outputDirectory().stripSuffix("/")
    val numOutputRows = appArgs.numOutputRows.getOrElse(1000)
    val maxSQLDescLength = appArgs.maxSqlDescLength.getOrElse(100)

    val nThreads = appArgs.numThreads.getOrElse(
      Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)
    val timeout = appArgs.timeout.toOption
    val reportReadSchema = appArgs.reportReadSchema.getOrElse(false)
    val order = appArgs.order.getOrElse("desc")
    val uiEnabled = appArgs.htmlReport.getOrElse(false)
    val reportSqlLevel = appArgs.perSql.getOrElse(false)

    val hadoopConf = new Configuration()

    val pluginTypeChecker = try {
      new PluginTypeChecker()
    } catch {
      case ie: IllegalStateException =>
        logError("Error creating the plugin type checker!", ie)
        return (1, Seq[QualificationSummaryInfo]())
    }

    val (eventLogFsFiltered, allEventLogs) = EventLogPathProcessor.processAllPaths(
      filterN.toOption, matchEventLogs.toOption, eventlogPaths, hadoopConf)

    val filteredLogs = if (argsContainsAppFilters(appArgs)) {
      val appFilter = new AppFilterImpl(numOutputRows, hadoopConf, timeout, nThreads)
      val finaleventlogs = if (appArgs.any() && argsContainsFSFilters(appArgs)) {
        (appFilter.filterEventLogs(allEventLogs, appArgs) ++ eventLogFsFiltered).toSet.toSeq
      } else {
        appFilter.filterEventLogs(eventLogFsFiltered, appArgs)
      }
      finaleventlogs
    } else {
      eventLogFsFiltered
    }

    if (filteredLogs.isEmpty) {
      logWarning("No event logs to process after checking paths, exiting!")
      return (0, Seq[QualificationSummaryInfo]())
    }

    val qual = new Qualification(outputDirectory, numOutputRows, hadoopConf, timeout,
      nThreads, order, pluginTypeChecker, reportReadSchema, printStdout, uiEnabled,
      enablePB, reportSqlLevel, maxSQLDescLength)
    val res = qual.qualifyApps(filteredLogs)
    (0, res)
  }

  def argsContainsFSFilters(appArgs: QualificationArgs): Boolean = {
    val filterCriteria = appArgs.filterCriteria.toOption
    appArgs.matchEventLogs.isSupplied ||
        (filterCriteria.isDefined && filterCriteria.get.endsWith("-filesystem"))
  }

  def argsContainsAppFilters(appArgs: QualificationArgs): Boolean = {
    val filterCriteria = appArgs.filterCriteria.toOption
    appArgs.applicationName.isSupplied || appArgs.startAppTime.isSupplied ||
        appArgs.userName.isSupplied || appArgs.sparkProperty.isSupplied ||
        (filterCriteria.isDefined && (filterCriteria.get.endsWith("-newest") ||
            filterCriteria.get.endsWith("-oldest") || filterCriteria.get.endsWith("-per-app-name")))
  }
}
