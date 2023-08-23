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
      reportPath: String = "",
      format: String = "parquet",
      version: String = "1.0.0",
      seed: Int = 41,
      iterations: Int = 1,
      queries: Map[String, Int] = Map(),
      overwrite: Boolean = false)

  /**
   *
   * @param query the final query string to run in Spark SQL
   * @param iterations number of iterations for the query to run in a line
   * @param baseOutputPath base path for the output, the final output will append a postfix for
   *                       iterations
   * @param overwrite overwrite the output or not
   * @param format parquet or orc are possible to pass in now
   * @param spark SparkSession instance
   * @return a Sequence of elapses for all iterations
   */
  private def runOneQueryForIterations(query: String,
      iterations: Int,
      baseOutputPath: String,
      overwrite: Boolean,
      format: String,
      spark: SparkSession): Seq[Long]
  = {
    val mode = if (overwrite == true) "overwrite" else "error"
    (1 to iterations).map(i => {
      println(s"Iteration: $i")
      val start = System.nanoTime()
      spark.sql(query).write.mode(mode).format(format).save(s"${baseOutputPath}_$i")
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      elapsed
    })
  }
  private def runScaleTest(config: Config): Unit = {
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test")
      .getOrCreate()
    val querySpecs = new QuerySpecs(config, spark)
    querySpecs.initViews()
    val queryMap = querySpecs.getCandidateQueries()
    var executionTime = Map[String, Seq[Long]]()
    for ((queryName, query) <- queryMap) {
      val outputPath = s"${config.outputDir}/$queryName"
      println(s"Running Query: $queryName for ${query.iterations} iterations")
      println(s"${query.content}")
      // run one query for several iterations in a row
      val elapses = runOneQueryForIterations(query.content, query.iterations, outputPath, config
        .overwrite, config.format, spark)
      executionTime += (queryName -> elapses)
    }
    val report = new TestReport(config, executionTime)
    report.save()
  }

  private def initArgParser(): OptionParser[Config] = {
    val supportFormats = List("parquet", "orc")
    new OptionParser[Config]("ScaleTest") {
      head("Scale Test", "1.0.0")
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
      opt[Int]("iterations")
        .optional()
        .action((x, c) => c.copy(iterations = x))
        .text("iterations to run for each query. default: 1")
      opt[String]("queries")
        .optional()
        .action((x, c) => c.copy(queries = x.split(",").map{ pair =>
          val Array(qName, iter) = pair.split(":")
          qName.toLowerCase -> iter.toInt
        }.toMap))
        .text("Specify queries with iterations to run specifically. the format must be " +
          "<query-name>:<iterations-for-this-query> with comma separated entries. e.g. --tables " +
          "q1:2,q2:3,q3:4. If not specified, all queries will be run for `--iterations` rounds")
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
