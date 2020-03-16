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
package ai.rapids.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.bitwiseNOT
import org.apache.spark.sql.functions.shiftLeft
import org.apache.spark.sql.functions.shiftRight
import org.apache.spark.sql.functions.shiftRightUnsigned

class BitwiseOperatorsSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test bitwise and", biggerLongsDf) {
    frame => frame.select(col("longs") bitwiseAND col("more_longs"))
  }

  testSparkResultsAreEqual("Test bitwise or", biggerLongsDf) {
    frame => frame.select(col("longs") bitwiseOR  col("more_longs"))
  }

  testSparkResultsAreEqual("Test bitwise xor", biggerLongsDf) {
    frame => frame.select(col("longs") bitwiseXOR col("more_longs"))
  }

  testSparkResultsAreEqual("Test bitwise not", biggerLongsDf) {
    frame => frame.select(bitwiseNOT(col("longs")))
  }

  testSparkResultsAreEqual("Bitwise shift left", intsDf) {
    frame => frame.selectExpr("shiftLeft(ints, more_ints)")
  }

  testSparkResultsAreEqual("Bitwise shift right", longsDf) {
    frame => frame.selectExpr("shiftRight(longs, more_longs)")
  }

  testSparkResultsAreEqual("Bitwise shift right unsigned", longsDf) {
    frame => frame.select(shiftRightUnsigned(col("longs"), 4))
  }
}
