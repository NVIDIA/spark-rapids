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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class WindowFunctionSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("Test window aggregations (finite ROWS) ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
                           .rowsBetween(-10,10)

    df => df.select(
      sum("dollars").over(rowsWindow),
      min("dollars").over(rowsWindow),
      max("dollars").over(rowsWindow),
      count("dollars").over(rowsWindow),
      count("*").over(rowsWindow)
    )
  }

  testSparkResultsAreEqual("Test window aggregations (unbounded preceding ROWS) ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
                           .rowsBetween(Window.unboundedPreceding,10)

    df => df.select(
      sum("dollars").over(rowsWindow),
      min("dollars").over(rowsWindow),
      max("dollars").over(rowsWindow),
      count("dollars").over(rowsWindow),
      count("*").over(rowsWindow)
    )
  }

  testSparkResultsAreEqual("Test window aggregations (unbounded preceding and following ROWS) ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
                           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df => df.select(
      sum("dollars").over(rowsWindow),
      min("dollars").over(rowsWindow),
      max("dollars").over(rowsWindow),
      count("dollars").over(rowsWindow),
      count("*").over(rowsWindow)
    )
  }

  testSparkResultsAreEqual("Test window aggregations (unbounded preceding to current ROWS) ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
                           .rowsBetween(Window.unboundedPreceding, 0)

    df => df.select(
      sum("dollars").over(rowsWindow),
      min("dollars").over(rowsWindow),
      max("dollars").over(rowsWindow),
      count("dollars").over(rowsWindow),
      count("*").over(rowsWindow)
    )
  }

  /*
  // TODO: UNBOUNDED PRECEDING is broken for RANGE-window lower-bounds.
  testSparkResultsAreEqual("Test window aggregations (unbounded preceding and following RANGE) ", windowTestDfOrc) {
    val rangeWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rangeBetween(0, Window.unboundedFollowing)

    df => df.select(
      sum("dollars").over(rangeWindow),
      min("dollars").over(rangeWindow),
      max("dollars").over(rangeWindow),
      count("dollars").over(rangeWindow),
      count("*").over(rangeWindow)
    )
  }
   */
}
