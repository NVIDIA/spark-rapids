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

import java.util.concurrent._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.NANOSECONDS
import scala.concurrent.ExecutionContext.Implicits.global

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
      queries: Seq[String] = Seq(),
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
      timeout: Long,
      baseOutputPath: String,
      overwrite: Boolean,
      format: String,
      spark: SparkSession,
      idleSessionListener: IdleSessionListener): Seq[Long]
  = {
    val mode = if (overwrite == true) "overwrite" else "error"
    val executionTimes = ListBuffer[Long]()

    (1 to iterations).foreach(i => {
      idleSessionListener.isIdle()
      println(s"Iteration: $i")
      while (idleSessionListener.isBusy()){
        // Scala Test aims for stability not performance. And the sleep time will not be calculated
        // into execution time.
        Thread.sleep(1000)
      }
      try {
        val future = scala.concurrent.Future {
          val start = System.nanoTime()
          spark.sql(query).write.mode(mode).format(format).save(s"${baseOutputPath}_$i")
          val end = System.nanoTime()
          val elapsed = NANOSECONDS.toMillis(end - start)
          executionTimes += elapsed
        }
        scala.concurrent.Await.result(future,
          scala.concurrent.duration.Duration(timeout, TimeUnit.MILLISECONDS))
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          println(s"Timeout at iteration $i")
          // use "-1" to mark timeout execution
          executionTimes += -1
          spark.sparkContext.cancelAllJobs()
        case e: Exception =>
          // We don't want a query failure to fail over the whole test.
          println(s"Query failed: $query - ${e.getMessage}")
      }
    })
    executionTimes
  }


  private def runScaleTest(config: Config): Unit = {
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test")
      .getOrCreate()
    val idleSessionListener = new IdleSessionListener()
    spark.sparkContext.addSparkListener(idleSessionListener)
    val querySpecs = new QuerySpecs(config, spark)
    querySpecs.initViews()
    val queryMap = querySpecs.getCandidateQueries()
    var executionTime = Map[String, Seq[Long]]()
    for ((queryName, query) <- queryMap) {
      val outputPath = s"${config.outputDir}/$queryName"
      println(s"Running Query: $queryName for ${query.iterations} iterations")
      println(s"${query.content}")
      // run one query for several iterations in a row
      val elapses = runOneQueryForIterations(query.content, query.iterations, query.timeout,
        outputPath, config.overwrite, config.format, spark, idleSessionListener)
      executionTime += (queryName -> elapses)
    }
    val report = new TestReport(config, executionTime, spark)
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
        .text("path to save the JSON report file that contains test results")
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
        .action((x, c) => c.copy(queries = x.split(",").map(t => t.toLowerCase())))
        .text("Specify queries to run specifically. the format must be " +
          "query names with comma separated. e.g. --tables " +
          "q1,q2,q3. If not specified, all queries will be run for `--iterations` rounds")
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
