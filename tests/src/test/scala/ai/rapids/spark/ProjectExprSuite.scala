/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  test("rand is okay") {
    // We cannot test that the results are exactly equal because our random number
    // generator is not identical to spark, so just make sure it does not crash
    // and all of the numbers are in the proper range
    withGpuSparkSession(session => {
      val df = nullableFloatCsvDf(session)
      val data = df.select(col("floats"), rand().as("RANDOM")).collect()
      data.foreach(row => {
        val d = row.getDouble(1)
        assert(d < 1.0)
        assert(d >= 0.0)
      })
    })
  }

  testSparkResultsAreEqual("Test literal values in select", mixedFloatDf) {
    frame => frame.select(col("floats"), lit(100), lit("hello, world!"))
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.select("time")
  }
}
