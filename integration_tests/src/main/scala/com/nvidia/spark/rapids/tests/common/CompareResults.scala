/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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
package com.nvidia.spark.rapids.tests.common

import org.rogach.scallop.ScallopConf

import org.apache.spark.sql.SparkSession

/**
 * Utility for comparing two csv or parquet files, such as the output from a benchmark, to
 * verify that they match, allowing for differences in precision.
 *
 * This utility is intended to be run via spark-submit.
 *
 * Example usage:
 *
 * <pre>
 * \$SPARK_HOME/bin/spark-submit --jars \$SPARK_RAPIDS_PLUGIN_JAR,\$CUDF_JAR \
 *   --master local[*] \
 *   --class com.nvidia.spark.rapids.tests.common.CompareResults \
 *   \$SPARK_RAPIDS_PLUGIN_INTEGRATION_TEST_JAR \
 *   --input1 /path/to/result1 \
 *   --input2 /path/to/result2 \
 *   --input-format parquet
 * </pre>
 */
object CompareResults {
  def main(arg: Array[String]): Unit = {
    val conf = new Conf(arg)

    val spark = SparkSession.builder
        .appName("CompareResults")
        // disable plugin so that we can see FilePartition rather than DatasourceRDDPartition and
        // can retrieve individual partition filenames
        .config("spark.rapids.sql.enabled", "false")
        .getOrCreate()

    val (df1, df2) = conf.inputFormat() match {
      case "csv" =>
        (spark.read.csv(conf.input1()), spark.read.csv(conf.input2()))
      case "parquet" =>
        (spark.read.parquet(conf.input1()), spark.read.parquet(conf.input2()))
    }

    val readPathAction = conf.inputFormat() match {
      case "csv" =>
        path: String => spark.read.csv(path)
      case "parquet" =>
        path: String => spark.read.parquet(path)
    }

    BenchUtils.compareResults(
      df1,
      df2,
      readPathAction,
      conf.ignoreOrdering(),
      conf.useIterator(),
      conf.maxErrors(),
      conf.epsilon())
  }
}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  /** Path to first data set */
  val input1 = opt[String](required = true)
  /** Path to second data set */
  val input2 = opt[String](required = true)
  /** Input format (csv or parquet) */
  val inputFormat = opt[String](required = true)
  /** Sort the data collected from the DataFrames before comparing them. */
  val ignoreOrdering = opt[Boolean](required = false, default = Some(false))
  /**
   * When set to true, use `toLocalIterator` to load one partition at a time into driver memory,
   * reducing memory usage at the cost of performance because processing will be single-threaded.
   */
  val useIterator = opt[Boolean](required = false, default = Some(false))
  /** Maximum number of differences to report */
  val maxErrors = opt[Int](required = false, default = Some(10))
  /** Allow for differences in precision when comparing floating point values */
  val epsilon = opt[Double](required = false, default = Some(0.00001))
  verify()
}