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

package com.nvidia.rapids.tests.scaletest

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object ScaleTestDataGen{

  // case class represents all input commandline arguments
  private case class Config(scaleFactor: Int = 1,
    complexity: Int = 1,
    tables: Array[String] = Array(),
    outputDir: String = "",
    format: String = "parquet",
    version: String = "",
    seed: Int = 41)

  private def runDataGen(config: Config): Unit ={
    // Init SparkSession
    val spark = SparkSession.builder()
      .appName("Scale Test Data Generation")
      .getOrCreate()

    val tableGenerator = new TableGenerator(config.scaleFactor, config.complexity, config.seed, spark)
    val tableMap = tableGenerator.genTables(config.tables)
    val baseOutputPath = s"${config.outputDir}/" +
      s"SCALE_" +
      s"${config.scaleFactor}_" +
      s"${config.complexity}_" +
      s"${config.format}_" +
      s"${config.version}" +
      s"${config.seed}"
    // 512 Mb block size requirement
    val setBlockSizeTables = Seq("b_data", "g_data")

    for ((tableName, df) <- tableMap) {
      if (setBlockSizeTables.contains(tableName)) {
        config.format match {
          case "parquet" => df.write
            .option("parquet.block.size", 512*1024*1024)
            .parquet(s"$baseOutputPath/$tableName")
          case "orc" => df.write
            .option("orc.block.size", 512*1024*1024)
            .orc(s"$baseOutputPath/$tableName")
        }
      } else {
        config.format match {
          case "parquet" => df.write
            .parquet(s"$baseOutputPath/$tableName")
          case "orc" => df.write
            .orc(s"$baseOutputPath/$tableName")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val tableList = List("a_facts", "b_data", "c_data", "d_data", "e_data", "f_facts", "g_data")
    val supportFormats = List("parquet","orc")
    val OParser = new OptionParser[Config]("DataGenEntry"){

        head("Scale Test Data Generation Application", "0.1")

        opt[Int]('s', "scale_factor")
          .required()
          .action((x, c) => c.copy(scaleFactor = x))
          .text("scale factor for data size")

        opt[Int]('c', "complexity")
          .required()
          .action((x, c) => c.copy(complexity = x))
          .text("complexity level for processing")

        opt[String]('t', "tables")
          .optional()
          .action((x, c) => c.copy(tables = x.split(",")))
          .validate(x =>
            if (x.split(",").forall(t => tableList.contains(t))) success
            else failure(s"Invalid table name. Must be one of ${tableList.mkString(",")}")
          )
          .text("tables to generate")

        opt[String]('o', "output_dir")
          .required()
          .action((x, c) => c.copy(outputDir = x))
          .validate(x =>
            if(x.nonEmpty) success else failure("Output dir required")
          )
          .text("output directory")

        opt[String]('f', "format")
          .validate( x =>
            if(supportFormats.contains(x.toLowerCase)) success
            else failure(s"Format must be one of ${supportFormats.mkString(",")}")
          )
          .action((x, c) => c.copy(format = x.toLowerCase))
          .text("output format")

        opt[String]('v', "version")
          .action((x, c) => c.copy(version = x))
          .text("version")

        opt[Int]('d', "seed")
          .optional()
          .action((x, c) => c.copy(seed = x))
          .text("seed used to generate random data columns")
    }

    OParser.parse(args, Config()) match {
      case Some(config) =>
        runDataGen(config)
      case _ =>
        sys.exit(1)
    }
  }
}
