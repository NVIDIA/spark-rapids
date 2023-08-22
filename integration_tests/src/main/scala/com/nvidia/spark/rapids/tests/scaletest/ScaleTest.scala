/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tests.scaletest

import scala.concurrent.duration.NANOSECONDS

import scopt.OptionParser

import org.apache.spark.sql.SparkSession

/**
 * Entry point for Scale Test
 */
object ScaleTest {

  // case class represents all input commandline arguments
  case class Config(scaleFactor: Int = 1,
      complexity: Int = 1,
      inputDir: String = "",
      outputDir: String = "",
      queryFile: String = "",
      reportPath: String = "",
      format: String = "parquet",
      version: String = "1.0.0",
      seed: Int = 41,
      overwrite: Boolean = false)

  private def runScaleTest(config: Config): Unit = {
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test ")
      .getOrCreate()
    val querySpecs = new QuerySpecs(config, spark)
    querySpecs.initViews()
    val queryMap = querySpecs.processQueryFile(config.queryFile)
    // TODO: more iterations: we want one query to run several time in a line or want the whole
    //  suite to run round by round? The following code should change accordingly
    var executionTime = Map[String, Long]()

    for ((queryName, query) <- queryMap) {
      println(s"Running Query: $queryName")
      println(s"$query")
      val outputPath = s"${config.outputDir}/$queryName"
      val start = System.nanoTime()

      config.overwrite match {
        case true =>
          spark.sql(query).write.mode("overwrite").format(config.format).save(outputPath)
        case false =>
          spark.sql(query).write.format(config.format).save(outputPath)
      }
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      executionTime += (queryName -> elapsed)
    }
    val report = new TestReport(config, executionTime)
    report.save()
  }

  private def initArgParser(): OptionParser[Config] = {
    val supportFormats = List("parquet", "orc")
    new OptionParser[Config]("DataGenEntry") {
      head("Scale Test Data Generation Application", "1.0.0")
      arg[Int]("<scale factor>")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scale factor for data size")
      arg[Int]("<complexity>")
        .required()
        .action((x, c) => c.copy(complexity = x))
        .text("complexity level for processing")
      arg[String]("<format>")
        .validate(x =>
          if (supportFormats.contains(x.toLowerCase)) success
          else failure(s"Format must be one of ${supportFormats.mkString(",")}")
        )
        .action((x, c) => c.copy(format = x.toLowerCase))
        .text("output format for the data")
      arg[String]("<input directory>")
        .required()
        .action((x, c) => c.copy(inputDir = x))
        .text("input directory for table data")
      arg[String]("<output directory>")
        .required()
        .action((x, c) => c.copy(outputDir = x))
        .text("directory for query output")
      arg[String]("<path to query file>")
        .required()
        .action((x, c) => c.copy(queryFile = x))
        .text("text file contains template queries")
      arg[String]("<path to save report file>")
        .required()
        .action((x, c) => c.copy(reportPath = x))
        .text("path to save the report file that contains test results")
      opt[Int]('d', "seed")
        .optional()
        .action((x, c) => c.copy(seed = x))
        .text("seed used to generate random data columns. default is 41 if not specified")
      opt[Unit]("overwrite")
        .optional()
        .action((_, c) => c.copy(overwrite = true))
        .text("Flag argument. Whether to overwrite the existing data in the path.")
    }
  }

  def main(args: Array[String]): Unit = {
    val OParser = initArgParser()
    OParser.parse(args, Config()) match {
      case Some(config) =>
        runScaleTest(config)
      case _ =>
        sys.exit(1)
    }
  }
}
