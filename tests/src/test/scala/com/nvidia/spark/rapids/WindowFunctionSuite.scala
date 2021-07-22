/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class WindowFunctionSuite extends SparkQueryCompareTestSuite {
  // The logical plan optimizer in Spark 3.0.x is non-deterministic when planning windows
  // over the same range, so avoid trying to compare canonicalized plans on Spark 3.0.x
  private val skipRangeCanon = cmpSparkVersion(3, 1, 1) < 0

  def windowAggregationTester(windowSpec: WindowSpec): DataFrame => DataFrame =
    (df : DataFrame) => df.select(
      sum("dollars").over(windowSpec).as("sum_dollars"),
      min("dollars").over(windowSpec).as("min_dollars"),
      max("dollars").over(windowSpec).as("max_dollars"),
      count("*").over(windowSpec).as("count_all")
    ).orderBy("sum_dollars", "min_dollars", "max_dollars", "count_all")

  def rowNumberAggregationTester(windowSpec: WindowSpec): DataFrame => DataFrame =
    (df : DataFrame) => df.select(
      sum("dollars").over(windowSpec).as("sum_dollars"),
      min("dollars").over(windowSpec).as("min_dollars"),
      max("dollars").over(windowSpec).as("max_dollars"),
      row_number().over(windowSpec).as("row_num"),
      count("*").over(windowSpec).as("count_all")
    ).orderBy("sum_dollars", "min_dollars", "max_dollars",
              "row_num", "count_all")

  private def windowAggregationTesterForDecimal(
    windowSpec: WindowSpec, scale: Int = 0): DataFrame => DataFrame =
    (df: DataFrame) => {
      val decimalType = DecimalType(precision = 18, scale = scale)
      val sumDecimalType = DecimalType(precision = 8, scale = 0)
      df.select(
        col("uid").cast(decimalType),
        col("dollars"),
        col("dateLong").cast(decimalType)
      ).withColumn("decimalDollars", col("dollars").cast(decimalType)
      ).withColumn("sumDecimalDollars", col("dollars").cast(sumDecimalType)
      ).select(
        sum("sumDecimalDollars").over(windowSpec).as("sum_dollars"),
        min("decimalDollars").over(windowSpec).as("min_dollars"),
        max("decimalDollars").over(windowSpec).as("max_dollars"),
        count("decimalDollars").over(windowSpec).as("count_dollars")
      ).orderBy("sum_dollars", "min_dollars", "max_dollars",
                "count_dollars")
    }

  private def rowNumberAggregationTesterForDecimal(
    windowSpec: WindowSpec, scale: Int = 0): DataFrame => DataFrame =
    (df: DataFrame) => {
      val decimalType = DecimalType(precision = 18, scale = scale)
      val sumDecimalType = DecimalType(precision = 8, scale = 0)
      df.select(
        col("uid").cast(decimalType),
        col("dollars"),
        col("dateLong").cast(decimalType)
      ).withColumn("decimalDollars", col("dollars").cast(decimalType)
      ).withColumn("sumDecimalDollars", col("dollars").cast(sumDecimalType)
      ).select(
        sum("sumDecimalDollars").over(windowSpec).as("sum_dollars"),
        min("decimalDollars").over(windowSpec).as("min_dollars"),
        max("decimalDollars").over(windowSpec).as("max_dollars"),
        row_number().over(windowSpec).as("row_num"),
        count("*").over(windowSpec).as("count_all")
      ).orderBy("sum_dollars", "min_dollars", "max_dollars",
                "row_num", "count_all")
    }

  testSparkResultsAreEqual("[Window] [ROWS] [-2, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("uid", "dateLong", "dollars")
                           .rowsBetween(-2, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [-2, CURRENT ROW] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(-2, 0)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 1)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [-2, UNBOUNDED FOLLOWING] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(-2, Window.unboundedFollowing)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(0, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 3)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, CURRENT ROW] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(0, 0)
    windowAggregationTesterForDecimal(rowsWindow, scale = 4)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(0, Window.unboundedFollowing)
    windowAggregationTesterForDecimal(rowsWindow, scale = 5)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(Window.unboundedPreceding, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 10)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] ",
      windowTestDfOrc,
      new SparkConf().set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(Window.unboundedPreceding, 0)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = -1)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] [NO PART]",
    windowTestDfOrc,
    new SparkConf().set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")) {
    val rowsWindow = Window
        .orderBy("uid", "dateLong", "dollars")
        .rowsBetween(Window.unboundedPreceding, 0)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = -1)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] [ROW_NUMBER]",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("uid", "dateLong", "dollars")
      .rowsBetween(Window.unboundedPreceding, 0)
    rowNumberAggregationTester(rowsWindow)
    rowNumberAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual(
    "[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] [ROW_NUMBER] [WITHOUT PARTITIONBY]",
    windowTestDfOrc) {
    val rowsWindow = Window.orderBy("uid", "dateLong", "dollars")
      .rowsBetween(Window.unboundedPreceding, 0)
    rowNumberAggregationTester(rowsWindow)
    rowNumberAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("uid", "dateLong", "dollars")
                           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [Unspecified ordering] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow)
  }

  def testAllWindowAggregations(windowClause : String): DataFrame => DataFrame =
    (df : DataFrame) => {
      df.createOrReplaceTempView("mytable")
      df.sparkSession.sql(
        s"""
           | SELECT
           |   SUM(dollars)   OVER $windowClause,
           |   MIN(dollars)   OVER $windowClause,
           |   MAX(dollars)   OVER $windowClause,
           |   COUNT(dollars) OVER $windowClause,
           |   COUNT(1)       OVER $windowClause,
           |   COUNT(*)       OVER $windowClause
           | FROM mytable
           | ORDER BY 1,2,3,4,5,6
           |
           |""".stripMargin)
    }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [-2 DAYS, 3 DAYS] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [-2 DAYS, 3 DAYS] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [-2 DAYS, CURRENT ROW] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [-2 DAYS, CURRENT ROW] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [CURRENT ROW, 3 DAYS] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN CURRENT ROW AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [CURRENT ROW, 3 DAYS] ", windowTestDfOrc,
    skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN CURRENT ROW AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [CURRENT ROW, CURRENT ROW] ",
      windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [CURRENT ROW, CURRENT ROW] ",
      windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Integral Type]",
    windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY dollars ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Short Type]", windowTestDfOrc,
      new SparkConf().set("spark.rapids.sql.window.range.short.enabled", "true"),
      skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS SHORT) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Long Type]",
    windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS LONG) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Byte Type]", windowTestDfOrc,
      new SparkConf().set("spark.rapids.sql.window.range.byte.enabled", "true"),
      skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS Byte) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Date Type]",
    windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY dt ASC
        | RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }


  IGNORE_ORDER_testSparkResultsAreEqual("[Window] [MIXED WINDOW SPECS] ",
      windowTestDfOrc, skipCanonicalizationCheck = skipRangeCanon) {
    (df : DataFrame) => {
      df.createOrReplaceTempView("mytable")
      // scalastyle:off line.size.limit
      df.sparkSession.sql(
        s"""
           | SELECT
           |  SUM(dollars)  OVER (PARTITION BY uid   ORDER BY CAST(dateLong AS TIMESTAMP)  ASC  RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW) first,
           |  MIN(dollars)  OVER (PARTITION BY uid   ORDER BY CAST(dateLong AS TIMESTAMP) DESC  RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING) second,
           |  MAX(dollars)  OVER (PARTITION BY uname ORDER BY CAST(dateLong AS TIMESTAMP)  ASC  ROWS  BETWEEN 5 PRECEDING and 5 FOLLOWING) third,
           |  COUNT(*)      OVER (PARTITION BY uname ORDER BY CAST(dateLong AS TIMESTAMP) DESC  ROWS  BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) fourth,
           |  ROW_NUMBER()  OVER (PARTITION BY uid   ORDER BY CAST(dateLong AS TIMESTAMP) DESC  ROWS  BETWEEN UNBOUNDED PRECEDING and CURRENT ROW) fifth,
           |  MAX(dateLong) OVER (PARTITION BY uid   ORDER BY dateLong                     ASC  ROWS  BETWEEN CURRENT ROW and UNBOUNDED FOLLOWING) sixth
           | FROM mytable
           |""".stripMargin)
      // scalastyle:on line.size.limit
    }
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual(
    "[Window] [RANGE] [ ASC] [-2 DAYS, 3 DAYS] ",
    windowTestDfOrc,
    Seq("AggregateExpression",
      "Alias",
      "AttributeReference",
      "Count",
      "Literal",
      "SpecifiedWindowFrame",
      "WindowExec",
      "WindowExpression",
      "WindowSpecDefinition"),
    skipCanonicalizationCheck = skipRangeCanon) {
    (df : DataFrame) => {
      df.createOrReplaceTempView("mytable")
      // scalastyle:off line.size.limit
      df.sparkSession.sql(
        """
          | SELECT COUNT(dollars+1) OVER
          |   (PARTITION BY uid
          |    ORDER BY CAST(dateLong AS TIMESTAMP) ASC
          |    RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
          | FROM mytable
          |
          |""".stripMargin)
      // scalastyle:on line.size.limit
    }
  }
}
