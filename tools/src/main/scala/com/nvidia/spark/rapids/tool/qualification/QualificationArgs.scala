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

import org.rogach.scallop.{ScallopConf, ScallopOption}

class QualificationArgs(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""
RAPIDS Accelerator for Apache Spark qualification tool

Usage: java -cp rapids-4-spark-tools_2.12-<version>.jar:$SPARK_HOME/jars/*
       com.nvidia.spark.rapids.tool.qualification.QualificationMain [options]
       <eventlogs | eventlog directories ...>
    """)

  val outputDirectory: ScallopOption[String] =
    opt[String](required = false,
      descr = "Base output directory. Default is current directory for the default filesystem." +
        " The final output will go into a subdirectory called" +
        " rapids_4_spark_qualification_output. It will overwrite any existing directory" +
        " with the same name.",
      default = Some("."))
  val eventlog: ScallopOption[List[String]] =
    trailArg[List[String]](required = true,
      descr = "Event log filenames(space separated) or directories containing event logs." +
          " eg: s3a://<BUCKET>/eventlog1 /path/to/eventlog2")
  val filterCriteria: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter newest or oldest N eventlogs for processing." +
          "eg: 100-newest (for processing newest 100 event logs). " +
          "eg: 100-oldest (for processing oldest 100 event logs)")
  val applicationName: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose application name matches exactly with input string" +
          "i.e no regular expressions supported.")
  val matchEventLogs: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose filenames contain the input string")
  val numOutputRows: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of output rows in the summary report. Default is 1000.",
      default = Some(1000))
  val order: ScallopOption[String] =
    opt[String](required = false,
      descr = "Specify the sort order of the report. desc or asc, desc is the default. " +
        "desc (descending) would report applications most likely to be accelerated at the top " +
        "and asc (ascending) would show the least likely to be accelerated at the top.")
  val numThreads: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of thread to use for parallel processing. The default is the " +
        "number of cores on host divided by 4.")
  val readScorePercent: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "The percent the read format and datatypes apply to the score. Default is " +
        "20 percent.",
      default = Some(20))
  val reportReadSchema: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Whether to output the read formats and datatypes to the CSV file. This can " +
        "be very long. Default is false.",
      default = Some(false))
  val timeout: ScallopOption[Long] =
    opt[Long](required = false,
      descr = "Maximum time in seconds to wait for the event logs to be processed. " +
        "Default is 24 hours (86400 seconds) and must be greater than 3 seconds. If it " +
        "times out, it will report what it was able to process up until the timeout.",
      default = Some(86400))

  validate(order) {
    case o if (QualificationArgs.isOrderAsc(o) || QualificationArgs.isOrderDesc(o)) => Right(Unit)
    case _ => Left("Error, the order must either be desc or asc")
  }

  validate(filterCriteria) {
    case crit if (crit.endsWith("-newest") || crit.endsWith("-oldest")) => Right(Unit)
    case _ => Left("Error, the filter criteria must end with either -newest or -oldest")
  }

  validate(timeout) {
    case timeout if (timeout > 3) => Right(Unit)
    case _ => Left("Error, timeout must be greater than 3 seconds.")
  }

  validate(readScorePercent) {
    case percent if (percent >= 0) && (percent <= 100) => Right(Unit)
    case _ => Left("Error, read score percent must be between 0 and 100.")
  }

  verify()
}

object QualificationArgs {
  def isOrderAsc(order: String): Boolean = {
    order.toLowerCase.startsWith("asc")
  }

  def isOrderDesc(order: String): Boolean = {
    order.toLowerCase.startsWith("desc")
  }
}
