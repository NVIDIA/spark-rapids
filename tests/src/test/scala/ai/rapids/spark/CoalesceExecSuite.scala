/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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
import org.apache.spark.sql.{DataFrame, SparkSession}

class CoalesceExecSuite  extends SparkQueryCompareTestSuite {
  private val smallSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "10")

  private def emptyDf(spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Seq[(String, Integer)]().toDF("strings", "ints")
  }

  testSparkResultsAreEqual("Test coalesce to 2", intsFromCsv, conf=smallSplitsConf) {
    frame => frame.filter("ints_1 == 1").distinct().coalesce(2)
  }

  testSparkResultsAreEqual("Test coalesce to 2000", intsFromCsv, conf=smallSplitsConf) {
    frame => frame.distinct().filter("ints_1 == 1").coalesce(2000)
  }

  testSparkResultsAreEqual("Test coalesce to 1", intsFromCsv, conf=smallSplitsConf) {
    frame => frame.distinct().filter("ints_1 == 1").coalesce(1)
  }

  testSparkResultsAreEqual("Test empty coalesce to 1", emptyDf, conf=smallSplitsConf) {
    frame => frame.distinct().filter("ints == 1").coalesce(1)
  }

  testSparkResultsAreEqual("Test empty coalesce to 10", emptyDf, conf=smallSplitsConf) {
    frame => frame.distinct().filter("ints == 1").coalesce(10)
  }
}
