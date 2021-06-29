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
  val matchEventLogs: ScallopOption[String] =
    opt[String](required = false,
      descr = "Filter event logs whose filenames contain the input string")
  val numOutputRows: ScallopOption[Int] =
    opt[Int](required = false,
      descr = "Number of output rows. Default is 1000.")

  validate(filterCriteria) {
    case crit if (crit.endsWith("-newest") || crit.endsWith("-oldest")) => Right(Unit)
    case _ => Left("Error, the filter criteria must end with either -newest or -oldest")
  }

  verify()
}
