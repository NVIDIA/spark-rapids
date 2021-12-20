/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

class TimeOperatorsSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("Test from_unixtime", datesPostEpochDf) {
    frame => frame.select(from_unixtime(col("dates")))
  }

  testSparkResultsAreEqual("Test from_unixtime with pattern dd/mm/yyyy", datesPostEpochDf) {
    frame => frame.select(from_unixtime(col("dates"),"dd/MM/yyyy"))
  }

  testSparkResultsAreEqual(
      "Test from_unixtime with alternative month and two digit year", datesPostEpochDf,
      conf = new SparkConf().set(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key, "true")) {
    frame => frame.select(from_unixtime(col("dates"),"dd/LL/yy HH:mm:ss.SSSSSS"))
  }

}
