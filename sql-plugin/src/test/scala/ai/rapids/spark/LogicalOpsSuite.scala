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

import org.apache.spark.sql.functions.col
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class LogicalOpsSuite extends FunSuite with BeforeAndAfterEach with SparkQueryCompareTestSuite {
  //////////////////
  // LOGICAL TESTS
  /////////////////
  testSparkResultsAreEqual("Test logical not", booleanDf) {
    frame => frame.select(!col("bools"))
  }

  testSparkResultsAreEqual("Test logical and", booleanDf) {
    frame => frame.select(col("bools") && col("more_bools"))
  }

  testSparkResultsAreEqual("Test logical or", booleanDf) {
    frame => frame.select(col("bools") || col("more_bools"))
  }

  //
  // (in)equality with longs
  //
  testSparkResultsAreEqual("Equal to longs", longsDf) {
    frame => frame.selectExpr("longs = more_longs")
  }

  testSparkResultsAreEqual("Not equal to longs", longsDf) {
    frame => frame.selectExpr("longs != more_longs")
  }

  testSparkResultsAreEqual("Less than longs", longsDf) {
    frame => frame.selectExpr("longs < more_longs")
  }

  testSparkResultsAreEqual("Less than or equal longs", longsDf) {
    frame => frame.selectExpr("longs <= more_longs")
  }

  testSparkResultsAreEqual("Greater than longs", longsDf) {
    frame => frame.selectExpr("longs > more_longs")
  }

  testSparkResultsAreEqual("Greater than or equal longs", longsDf) {
    frame => frame.selectExpr("longs >= more_longs")
  }

  //
  // (in)equality with doubles
  //
  testSparkResultsAreEqual("Equal to doubles", doubleDf) {
    frame => frame.selectExpr("doubles = more_doubles")
  }

  testSparkResultsAreEqual("Not equal to doubles", doubleDf) {
    frame => frame.selectExpr("doubles != more_doubles")
  }

  testSparkResultsAreEqual("Less than doubles", doubleDf) {
    frame => frame.selectExpr("doubles < more_doubles")
  }

  testSparkResultsAreEqual("Less than or equal doubles", doubleDf) {
    frame => frame.selectExpr("doubles <= more_doubles")
  }

  testSparkResultsAreEqual("Greater than doubles", doubleDf) {
    frame => frame.selectExpr("doubles > more_doubles")
  }

  testSparkResultsAreEqual("Greater than or equal doubles", doubleDf) {
    frame => frame.selectExpr("doubles >= more_doubles")
  }

  //
  // (in)equality with booleans
  //
  testSparkResultsAreEqual("Equal to booleans", booleanDf) {
    frame => frame.selectExpr("bools = more_bools")
  }

  testSparkResultsAreEqual("Not equal to booleans", booleanDf) {
    frame => frame.selectExpr("bools != more_bools")
  }

  testSparkResultsAreEqual("Less than booleans", booleanDf) {
    frame => frame.selectExpr("bools < more_bools")
  }

  testSparkResultsAreEqual("Less than or equal booleans", booleanDf) {
    frame => frame.selectExpr("bools <= more_bools")
  }

  testSparkResultsAreEqual("Greater than boleans", booleanDf) {
    frame => frame.selectExpr("bools > more_bools")
  }

  testSparkResultsAreEqual("Greater than or equal", booleanDf) {
    frame => frame.selectExpr("bools >= more_bools")
  }
  ///////
}
