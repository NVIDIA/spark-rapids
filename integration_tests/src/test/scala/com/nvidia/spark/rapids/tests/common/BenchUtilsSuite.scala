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
      query = "q1",
      queryPlan = QueryPlan("logical", "physical"),
      results = ResultSummary(
        rowCount = 100,
        partialResultLimit = Some(100),
        partialResults = Seq()),
      coldRun = Seq(123),
      hotRun = Seq(99, 88, 77))

    val filename = s"$TEST_FILES_ROOT/BenchUtilsSuite-${System.currentTimeMillis()}.json"
    BenchUtils.writeReport(report, filename)

    val report2 = BenchUtils.readReport(new File(filename))
    assert(report == report2)
  }

}
