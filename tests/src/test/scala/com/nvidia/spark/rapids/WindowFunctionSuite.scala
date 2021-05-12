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

  def windowAggregationTester(windowSpec: WindowSpec): DataFrame => DataFrame =
    (df : DataFrame) => df.select(
      sum("dollars").over(windowSpec),
      min("dollars").over(windowSpec),
      max("dollars").over(windowSpec),
      count("*").over(windowSpec)
    )

  def rowNumberAggregationTester(windowSpec: WindowSpec): DataFrame => DataFrame =
    (df : DataFrame) => df.select(
      sum("dollars").over(windowSpec),
      min("dollars").over(windowSpec),
      max("dollars").over(windowSpec),
      row_number().over(windowSpec),
      count("*").over(windowSpec)
    )

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
        sum("sumDecimalDollars").over(windowSpec),
        min("decimalDollars").over(windowSpec),
        max("decimalDollars").over(windowSpec),
        count("decimalDollars").over(windowSpec)
      )
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
        sum("sumDecimalDollars").over(windowSpec),
        min("decimalDollars").over(windowSpec),
        max("decimalDollars").over(windowSpec),
        row_number().over(windowSpec),
        count("*").over(windowSpec)
      )
    }

  /* There is no easy way to make dateLong not nullable with how these test are written
  Very similar functionality is covered by the python integration tests.  When
  https://github.com/NVIDIA/spark-rapids/issues/1039 is fixed then we can enable these tests again
  testSparkResultsAreEqual("[Window] [ROWS/RANGE] [default] ", windowTestDfOrcNonNullable,
      execsAllowedNonGpu=Seq("DeserializeToObjectExec", "CreateExternalRow",
        "Invoke", "StaticInvoke")) {
    val rowsWindow = Window.partitionBy("uid")
        .orderBy("dateLong")
    windowAggregationTester(rowsWindow)
  }
  */

  testSparkResultsAreEqual("[Window] [ROWS] [-2, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
                           .rowsBetween(-2, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [-2, CURRENT ROW] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(-2, 0)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 1)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [-2, UNBOUNDED FOLLOWING] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(-2, Window.unboundedFollowing)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(0, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 3)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, CURRENT ROW] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(0, 0)
    windowAggregationTesterForDecimal(rowsWindow, scale = 4)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [CURRENT ROW, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(0, Window.unboundedFollowing)
    windowAggregationTesterForDecimal(rowsWindow, scale = 5)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(Window.unboundedPreceding, 3)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = 10)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] ",
      windowTestDfOrc,
      new SparkConf().set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(Window.unboundedPreceding, 0)
    windowAggregationTester(rowsWindow)
    windowAggregationTesterForDecimal(rowsWindow, scale = -1)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] [ROW_NUMBER]",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(Window.unboundedPreceding, 0)
    rowNumberAggregationTester(rowsWindow)
    rowNumberAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual(
    "[Window] [ROWS] [UNBOUNDED PRECEDING, CURRENT ROW] [ROW_NUMBER] [WITHOUT PARTITIONBY]",
    windowTestDfOrc) {
    val rowsWindow = Window.orderBy("dateLong")
      .rowsBetween(Window.unboundedPreceding, 0)
    rowNumberAggregationTester(rowsWindow)
    rowNumberAggregationTesterForDecimal(rowsWindow, scale = 2)
  }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
                           .orderBy("dateLong")
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
           |
           |""".stripMargin)
    }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [-2 DAYS, 3 DAYS] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [-2 DAYS, 3 DAYS] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [-2 DAYS, CURRENT ROW] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [-2 DAYS, CURRENT ROW] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  /* There is no easy way to make dateLong not nullable with how these test are written
  Very similar functionality is covered by the python integration tests.  When
  https://github.com/NVIDIA/spark-rapids/issues/1039 is fixed then we can enable these tests again
  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [-2 DAYS, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [-2 DAYS, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }
  */

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [CURRENT ROW, 3 DAYS] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN CURRENT ROW AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [CURRENT ROW, 3 DAYS] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN CURRENT ROW AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [CURRENT ROW, CURRENT ROW] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [CURRENT ROW, CURRENT ROW] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN CURRENT ROW AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Integral Type]",
    windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY dollars ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Short Type]", windowTestDfOrc,
      new SparkConf().set("spark.rapids.sql.window.range.short.enabled", "true")) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS SHORT) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Long Type]",
    windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS LONG) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Byte Type]", windowTestDfOrc,
      new SparkConf().set("spark.rapids.sql.window.range.byte.enabled", "true")) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST (dollars AS Byte) ASC
        | RANGE BETWEEN 20 PRECEDING AND 20 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ASC] [Date Type]",
    windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY dt ASC
        | RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }


  /* There is no easy way to make dateLong not nullable with how these test are written
  Very similar functionality is covered by the python integration tests.  When
  https://github.com/NVIDIA/spark-rapids/issues/1039 is fixed then we can enable these tests again
  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [CURRENT ROW, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [CURRENT ROW, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [UNBOUNDED PRECEDING, 3 DAYS] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [UNBOUNDED PRECEDING, 3 DAYS] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 3 DAYS FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [UNBOUNDED PRECEDING, CURRENT ROW] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [UNBOUNDED PRECEDING, CURRENT ROW] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING] ",
      windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC
        | RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [ ASC] [Unspecified bounds] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) ASC)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }

  testSparkResultsAreEqual("[Window] [RANGE] [DESC] [Unspecified bounds] ", windowTestDfOrc) {

    val windowClause =
      """
        | (PARTITION BY uid
        | ORDER BY CAST(dateLong AS TIMESTAMP) DESC)
        |""".stripMargin

    testAllWindowAggregations(windowClause)
  }
  */

  IGNORE_ORDER_testSparkResultsAreEqual("[Window] [MIXED WINDOW SPECS] ",
      windowTestDfOrc) {
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
      "WindowSpecDefinition")) {
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
