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
      queryTimes = Seq(99, 88, 77))

    val filename = s"$TEST_FILES_ROOT/BenchUtilsSuite-${System.currentTimeMillis()}.json"
    BenchUtils.writeReport(report, filename)

    val report2 = BenchUtils.readReport(new File(filename))
    assert(report == report2)
  }

}
