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

import org.rogach.scallop.{ScallopConf, ScallopOption}

import org.apache.spark.sql.rapids.tool.AppFilterImpl

class ProfileArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""
Profiling Tool for the RAPIDS Accelerator and Apache Spark

Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.profiling.ProfileMain [options]
       <eventlogs | eventlog directories ...>
    """)

  val outputDirectory: ScallopOption[String] =
    opt[String](required = false,
      descr = "Base output directory. Default is current directory for the default filesystem." +
        " The final output will go into a subdirectory called" +
        " rapids_4_spark_profile. It will overwrite any existing files" +
        " with the same name.",
      default = Some("."))
  val eventlog: ScallopOption[List[String]] =
    trailArg[List[String]](required = true,
      descr = "Event log filenames(space separated) or directories containing event logs." +
          " eg: s3a://<BUCKET>/eventlog1 /path/to/eventlog2")
  val filterCriteria: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter newest or oldest N eventlogs for processing." +
          "eg: 100-newest-filesystem (for processing newest 100 event logs). " +
          "eg: 100-oldest-filesystem (for processing oldest 100 event logs).")
  val matchEventLogs: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose filenames contain the input string.")
  val compare: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Compare Applications (Note this may require more memory if comparing " +
          "a large number of applications. Default is false.")
  val numOutputRows: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of output rows for each Application. Default is 1000.")
  val generateDot: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Generate query visualizations in DOT format. Default is false.")
  val printPlans: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Print the SQL plans to a file named 'planDescriptions.log'." +
        " Default is false.")
  val generateTimeline: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Write an SVG graph out for the full application timeline.")
  val numThreads: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of thread to use for parallel processing. The default is the " +
        "number of cores on host divided by 4.")
  val csv: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Output each table to a CSV file as well creating the summary text file.")
  val combined: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Collect mode but combine all applications into the same tables.")
  val timeout: ScallopOption[Long] =
    opt[Long](required = false,
      descr = "Maximum time in seconds to wait for the event logs to be processed. " +
        "Default is 24 hours (86400 seconds) and must be greater than 3 seconds. If it " +
        "times out, it will report what it was able to process up until the timeout.",
      default = Some(86400))
  val startAppTime: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose application start occurred within the past specified " +
        "time period. Valid time periods are min(minute),h(hours),d(days),w(weeks)," +
        "m(months). If a period is not specified it defaults to days.")
  val autoTuner: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Toggle auto-tuner module.",
      default = Some(false))
  val workerInfo: ScallopOption[String] =
    opt[String](required = false,
      descr = "File path containing the system information of a worker node. It is assumed " +
        "that all workers are homogenous. It requires the AutoTuner to be enabled. Default is" +
        "current directory.",
      default = Some(AutoTuner.DEFAULT_WORKER_INFO))

  validate(filterCriteria) {
    case crit if (crit.endsWith("-newest-filesystem") ||
        crit.endsWith("-oldest-filesystem")) => Right(Unit)
    case _ => Left("Error, the filter criteria must end with either -newest-filesystem " +
        "or -oldest-filesystem")
  }

  validate(timeout) {
    case timeout if (timeout > 3) => Right(Unit)
    case _ => Left("Error, timeout must be greater than 3 seconds.")
  }

  validate(startAppTime) {
    case time if (AppFilterImpl.parseAppTimePeriod(time) > 0L) => Right(Unit)
    case _ => Left("Time period specified, must be greater than 0 and valid periods " +
      "are min(minute),h(hours),d(days),w(weeks),m(months).")
  }

  verify()
}
