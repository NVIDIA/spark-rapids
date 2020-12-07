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

import org.apache.spark.sql.functions._

class FilterExprSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual("filter with decimal literals", mixedDf(_), repart = 0) { df =>
    df.select(col("doubles"), col("decimals"),
      lit(BigDecimal(0L)).as("BigDec0"),
      lit(BigDecimal(123456789L, 6)).as("BigDec1"),
      lit(BigDecimal(-2.12314e-8)).as("BigDec2"))
      .filter(col("doubles").gt(3.0))
      .select("BigDec0", "BigDec1", "doubles", "decimals")
  }

  testSparkResultsAreEqual("filter with decimal columns", mixedDf(_), repart = 0) { df =>
    df.filter(col("ints") > 90)
      .filter(col("decimals").isNotNull)
      .select("ints", "strings", "decimals")
  }
}
