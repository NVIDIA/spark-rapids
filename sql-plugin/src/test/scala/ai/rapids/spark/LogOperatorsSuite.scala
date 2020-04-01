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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class LogOperatorsSuite extends SparkQueryCompareTestSuite {

  testLog("log") {
    frame: DataFrame => frame.select(log(col("n")))
  }

  testLog("log2") {
    frame: DataFrame => frame.select(log2(col("n")))
  }

  testLog("log10") {
    frame: DataFrame => frame.select(log10(col("n")))
  }

  testLog("log1p") {
    frame: DataFrame => frame.select(log1p(col("n")))
  }

  testLog("log with variable base") {
    frame: DataFrame => frame.select(log(base = Math.PI, a = col("n")))
  }

  private def testLog(testName: String,
    maxFloatDiff: Double = 0.00001,
    conf: SparkConf = new SparkConf(),
    sort: Boolean = false,
    repart: Integer = 1,
    sortBeforeRepart: Boolean = false,
    incompat: Boolean = true,
    allowNonGpu: Boolean = false,
    execsAllowedNonGpu: Seq[String] = Seq.empty)(f: DataFrame => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf, execsAllowedNonGpu,
        sortBeforeRepart)

    test(qualifiedTestName) {
      testUnaryFunction(testConf,(1 to 20), maxFloatDiff = maxFloatDiff)(f)
      testUnaryFunction(testConf, (1 to 20).map(_ * 0.1f), maxFloatDiff = maxFloatDiff)(f)
      testUnaryFunction(testConf, (1 to 20).map(_ * 0.1d), maxFloatDiff = maxFloatDiff)(f)
      testUnaryFunction(testConf, (-5 to 0).map(_ * 1.0), maxFloatDiff = maxFloatDiff)(f)
      testUnaryFunction(testConf, Seq(Float.NaN, Float.NegativeInfinity, Float.PositiveInfinity), maxFloatDiff = maxFloatDiff)(f)
      testUnaryFunction(testConf, Seq(Double.NaN, Double.NegativeInfinity, Double.PositiveInfinity), maxFloatDiff = maxFloatDiff)(f)
    }
  }

}
