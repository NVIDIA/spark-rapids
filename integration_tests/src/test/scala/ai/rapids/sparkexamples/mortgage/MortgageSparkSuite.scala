/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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

package ai.rapids.sparkexamples.mortgage

import ai.rapids.spark.RapidsShuffleManager
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MortgageSparkSuite extends FunSuite {
  lazy val  session: SparkSession = {
    var builder = SparkSession.builder
      .master("local[2]")
      .appName("MortgageTests")
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.plugins", "ai.rapids.spark.SQLPlugin")
      .config("spark.rapids.sql.explain", true)
      .config("spark.rapids.sql.test.enabled", false)
      .config("spark.rapids.sql.incompatibleOps.enabled", true)
      .config("spark.rapids.sql.hasNans", false)
    val rapidsShuffle = classOf[RapidsShuffleManager].getCanonicalName
    val prop = System.getProperty("rapids.shuffle.manager.override", "false")
    if (prop.equalsIgnoreCase("true")) {
      println("RAPIDS SHUFFLE MANAGER ACTIVE")
      builder = builder.config("spark.shuffle.manager", rapidsShuffle)
    } else {
      println("RAPIDS SHUFFLE MANAGER INACTIVE")
    }
    builder.getOrCreate()
  }

  test("extract mortgage data") {
    val df = Run.csv(
      session,
      "src/test/resources/Performance_2007Q3.txt_0",
      "src/test/resources/Acquisition_2007Q3.txt"
    ).sort(col("loan_id"), col("monthly_reporting_period"))

    assert(df.count() === 10000)
  }

  test("convert data to parquet") {
    ReadPerformanceCsv(session, "src/test/resources/Performance_2007Q3.txt_0")
      .write.mode("overwrite").parquet("target/test_output/perf")

    ReadAcquisitionCsv(session, "src/test/resources/Acquisition_2007Q3.txt")
      .write.mode("overwrite").parquet("target/test_output/acq")
  }

  test("run on parquet data") {
    val df = Run.parquet(
      session,
      "src/test/resources/parquet_perf",
      "src/test/resources/parquet_acq"
    ).sort(col("loan_id"), col("monthly_reporting_period"))

    assert(df.count() === 10000)
  }

  test("compute some basic aggregates") {
    val df = SimpleAggregates.csv(
      session,
      "src/test/resources/Performance_2007Q3.txt_0",
      "src/test/resources/Acquisition_2007Q3.txt"
    )

    assert(df.count() === 1660)
  }

  test("compute aggregates with percentiles") {
    val df = AggregatesWithPercentiles.csv(
      session,
      "src/test/resources/Performance_2007Q3.txt_0"
    )

    assert(df.count() === 177)
  }

  test("compute aggregates with joins") {
    val df = AggregatesWithJoin.csv(
      session,
      "src/test/resources/Performance_2007Q3.txt_0",
      "src/test/resources/Acquisition_2007Q3.txt"
    )

    assert(df.count() === 177)
  }
}
