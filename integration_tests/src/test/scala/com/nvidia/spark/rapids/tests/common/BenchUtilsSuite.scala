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

import java.io.File

import com.nvidia.spark.rapids.AdaptiveQueryExecSuite.TEST_FILES_ROOT
import com.nvidia.spark.rapids.TestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.sql.SparkSession

object BenchUtilsSuite {
  val TEST_FILES_ROOT: File = TestUtils.getTempDir(this.getClass.getSimpleName)
}

class BenchUtilsSuite extends FunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    TEST_FILES_ROOT.mkdirs()
  }

  override def afterEach(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(TEST_FILES_ROOT)
  }

  test("round-trip serialize benchmark results") {

    val report = BenchmarkReport(
      filename = "foo.bar",
      startTime = 0,
      env = Environment(
        Map("foo" -> "bar"),
        Map("spark.sql.adaptive.enabled" -> "true"),
        "3.0.1"),
      testConfiguration = TestConfiguration(gcBetweenRuns = false),
      action = "csv",
      writeOptions = Map("header" -> "true"),
      query = "q1",
      queryPlan = QueryPlan("logical", "physical"),
      Seq.empty,
      rowCounts = Seq(10, 10, 10),
      queryTimes = Seq(99, 88, 77),
      queryStatus = Seq("Completed", "Completed", "Completed"),
      exceptions = Seq.empty)

    val filename = s"$TEST_FILES_ROOT/BenchUtilsSuite-${System.currentTimeMillis()}.json"
    BenchUtils.writeReport(report, filename)

    val report2 = BenchUtils.readReport(new File(filename))
    assert(report == report2)
  }

  test("validate coalesce/repartition arguments - no duplicates") {
    BenchUtils.validateCoalesceRepartition(Map("a" -> 1, "b" -> 1), Map("c" -> 1, "d" -> 1))
  }

  test("validate coalesce/repartition arguments - with duplicates") {
    assertThrows[IllegalArgumentException] {
      BenchUtils.validateCoalesceRepartition(Map("a" -> 1, "b" -> 1), Map("c" -> 1, "b" -> 1))
    }
  }

  // this test is to check that the following code (based on docs/benchmarks.md) compiles
  ignore("test TPC-DS documented usage") {
    val spark = SparkSession.builder().getOrCreate()

    import com.nvidia.spark.rapids.tests.tpcds._

    // convert using minimal args
    TpcdsLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output")

    // convert with explicit partitioning
    TpcdsLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output",
      coalesce=Map("customer_address" -> 1), repartition=Map("web_sales" -> 8))

    // set up prior to running benchmarks
    TpcdsLikeSpark.setupAllParquet(spark, "/path/to/tpcds")

    import com.nvidia.spark.rapids.tests._
    val benchmark = new BenchmarkRunner(new TpcdsLikeBench())

    // run benchmarks
    benchmark.collect(spark, "q5", iterations=3)
    benchmark.writeParquet(spark, "q5", "/path/to/output", iterations=3)
  }

  // this test is to check that the following code (based on docs/benchmarks.md) compiles
  ignore("test TPC-H documented usage") {
    val spark = SparkSession.builder().getOrCreate()

    import com.nvidia.spark.rapids.tests.tpch._

    // convert using minimal args
    TpchLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output")

    // convert with explicit partitioning
    TpchLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output",
      coalesce=Map("orders" -> 8), repartition=Map("lineitem" -> 8))

    // set up prior to running benchmarks
    import com.nvidia.spark.rapids.tests._
    val benchmark = new BenchmarkRunner(new TpchLikeBench())

    // run benchmarks
    benchmark.collect(spark, "q5", iterations=3)
    benchmark.writeParquet(spark, "q5", "/path/to/output", iterations=3)
  }

  // this test is to check that the following code (based on docs/benchmarks.md) compiles
  ignore("test TPCx-BB documented usage") {
    val spark = SparkSession.builder().getOrCreate()

    import com.nvidia.spark.rapids.tests.tpcxbb._

    // convert using minimal args
    TpcxbbLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output")

    // convert with explicit partitioning
    TpcxbbLikeSpark.csvToParquet(spark, "/path/to/input", "/path/to/output",
      coalesce=Map("customer" -> 1), repartition=Map("item" -> 8))

    // set up prior to running benchmarks
    import com.nvidia.spark.rapids.tests._
    val benchmark = new BenchmarkRunner(new TpcxbbLikeBench())

    // run benchmarks
    benchmark.collect(spark, "q5", iterations=3)
    benchmark.writeParquet(spark, "q5", "/path/to/output", iterations=3)
  }

  // this test is to check that the following code (based on docs/benchmarks.md) compiles
  ignore("test documented usage for comparing results") {
    val spark = SparkSession.builder().getOrCreate()

    import com.nvidia.spark.rapids.tests.common._
    val cpu = spark.read.parquet("/data/tpcxbb/q5-cpu")
    val gpu = spark.read.parquet("/data/tpcxbb/q5-gpu")
    BenchUtils.compareResults(cpu, gpu, "parquet", ignoreOrdering=true, epsilon=0.0001)
  }

}
