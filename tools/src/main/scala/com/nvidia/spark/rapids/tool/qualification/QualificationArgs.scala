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

import org.rogach.scallop.{ScallopConf, ScallopOption}

import org.apache.spark.sql.rapids.tool.AppFilterImpl

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
      descr = "Filter newest or oldest N eventlogs based on application start timestamp, " +
          "unique application name or filesystem timestamp. Filesystem based filtering " +
          "happens before any application based filtering." +
          "For application based filtering, the order in which filters are" +
          "applied is: application-name, start-app-time, filter-criteria." +
          "Application based filter-criteria are:" +
          "100-newest (for processing newest 100 event logs based on timestamp inside" +
          "the eventlog) i.e application start time)  " +
          "100-oldest (for processing oldest 100 event logs based on timestamp inside" +
          "the eventlog) i.e application start time)  " +
          "100-newest-per-app-name (select at most 100 newest log files for each unique " +
          "application name) " +
          "100-oldest-per-app-name (select at most 100 oldest log files for each unique " +
          "application name)" +
          "Filesystem based filter criteria are:" +
          "100-newest-filesystem (for processing newest 100 event logs based on filesystem " +
          "timestamp). " +
          "100-oldest-filesystem (for processing oldest 100 event logsbased on filesystem " +
          "timestamp).")
  val applicationName: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs by application name. The string specified can be a " +
          "regular expression, substring, or exact match. For filtering based on complement " +
          "of application name, use ~APPLICATION_NAME. i.e Select all event logs except the ones " +
          "which have application name as the input string.")
  val any: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Apply multiple event log filtering criteria and process only logs for which any " +
          "condition is satisfied." +
          "Example: <Filter1> <Filter2> <Filter3> --any -> result is <Filter1> OR " +
          "<Filter2> OR <Filter3>")
  val all: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Apply multiple event log filtering criteria and process only logs for which all " +
          "conditions are satisfied." +
          "Example: <Filter1> <Filter2> <Filter3> --all -> result is <Filter1> AND " +
          "<Filter2> AND <Filter3>. Default is all=true")
  val startAppTime: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose application start occurred within the past specified " +
        "time period. Valid time periods are min(minute),h(hours),d(days),w(weeks)," +
        "m(months). If a period is not specified it defaults to days.")
  val matchEventLogs: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose filenames contain the input string. Filesystem " +
              "based filtering happens before any application based filtering.")
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
  val reportReadSchema: ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Whether to output the read formats and datatypes to the CSV file. This can " +
        "be very long. Default is false.",
      default = Some(false))
  val sparkProperty: ScallopOption[List[String]] =
    opt[List[String]](required = false,
      descr = "Filter applications based on certain Spark properties that were set during " +
          "launch of the application. It can filter based on key:value pair or just based on " +
          "keys. Multiple configs can be provided where the filtering is done if any of the" +
          "config is present in the eventlog. " +
          "filter on specific configuration: --spark-property=spark.eventLog.enabled:true" +
          "filter all eventlogs which has config: --spark-property=spark.driver.port" +
          "Multiple configs: --spark-property=spark.eventLog.enabled:true " +
          "--spark-property=spark.driver.port")
  val timeout: ScallopOption[Long] =
    opt[Long](required = false,
      descr = "Maximum time in seconds to wait for the event logs to be processed. " +
        "Default is 24 hours (86400 seconds) and must be greater than 3 seconds. If it " +
        "times out, it will report what it was able to process up until the timeout.",
      default = Some(86400))
  val userName: ScallopOption[String] =
    opt[String](required = false,
      descr = "Applications which a particular user has submitted." )
  val htmlReport : ScallopOption[Boolean] =
    toggle("html-report",
      default = Some(true),
      prefix = "no-",
      descrYes = "Generates an HTML Report. Enabled by default.",
      descrNo = "Disables generating the HTML report.")
  val perSql : ScallopOption[Boolean] =
    opt[Boolean](required = false,
      descr = "Report at the individual SQL query level.")
  val maxSqlDescLength: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Maximum length of the SQL description string output with the " +
        "per sql output. Default is 100.",
      default = Some(100))

  validate(order) {
    case o if (QualificationArgs.isOrderAsc(o) || QualificationArgs.isOrderDesc(o)) => Right(Unit)
    case _ => Left("Error, the order must either be desc or asc")
  }

  validate(filterCriteria) {
    case crit if (crit.endsWith("-newest-filesystem") || crit.endsWith("-oldest-filesystem")
        || crit.endsWith("-newest-per-app-name") || crit.endsWith("-oldest-per-app-name")
        || crit.endsWith("-oldest") || crit.endsWith("-newest")) => Right(Unit)
    case _ => Left("Error, the filter criteria must end with -newest, -oldest, " +
        "-newest-filesystem, -oldest-filesystem, -newest-per-app-name or -oldest-per-app-name")
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

object QualificationArgs {
  def isOrderAsc(order: String): Boolean = {
    order.toLowerCase.startsWith("asc")
  }

  def isOrderDesc(order: String): Boolean = {
    order.toLowerCase.startsWith("desc")
  }
}
