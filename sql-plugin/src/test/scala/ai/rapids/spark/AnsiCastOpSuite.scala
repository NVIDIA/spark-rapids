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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class AnsiCastOpSuite extends GpuExpressionTestSuite {

  private val sparkConf = new SparkConf()
    .set("spark.sql.ansi.enabled", "true")
    .set("spark.sql.storeAssignmentPolicy", "ANSI") // note this is the default in 3.0.0

  testNotSupportedOnGpu("Write doubles to long", testData(DataTypes.DoubleType), sparkConf) {
    // ansi cast from double to long is not yet implemented on GPU
    frame => doTableInsert(frame, "BIGINT")
  }

  testSparkResultsAreEqual("Write ints to long", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableInsert(frame, "BIGINT")
  }

  testSparkResultsAreEqual("Write long to float", testData(DataTypes.LongType), sparkConf) {
    frame => doTableInsert(frame, "BIGINT")
  }

  testNotSupportedOnGpu("Copy doubles to long", testData(DataTypes.DoubleType), sparkConf) {
    // ansi cast from double to long is not yet implemented on GPU
    frame => doTableCopy(frame, "DOUBLE", "BIGINT")
  }

  testSparkResultsAreEqual("Copy ints to long", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableCopy(frame, "INT", "BIGINT")
  }

  testSparkResultsAreEqual("Copy long to float", testData(DataTypes.LongType), sparkConf) {
    frame => doTableCopy(frame, "BIGINT", "FLOAT")
  }

  /**
   * Insert data into a table, causing a cast to happen.
   */
  private def doTableInsert(frame: DataFrame, sqlDataType: String): DataFrame = {
    val spark = frame.sparkSession
    val now = System.currentTimeMillis()
    val t1 = s"AnsiCastOpSuite_doTableInsert_${sqlDataType}_t1_$now"
    val t2 = s"AnsiCastOpSuite_doTableInsert_${sqlDataType}_t2_$now"
    frame.createOrReplaceTempView(t1)
    spark.sql(s"CREATE TABLE $t2 (a $sqlDataType)")
    spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1")
    spark.sql(s"SELECT a FROM $t2")
  }

  /**
   * Copy data between two tables, causing ansi_cast() expressions to be inserted into the plan.
   */
  private def doTableCopy(frame: DataFrame, sqlSourceType: String, sqlDestType: String): DataFrame = {
    val spark = frame.sparkSession
    val now = System.currentTimeMillis()
    val t1 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t1_$now"
    val t2 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t2_$now"
    val t3 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t3_$now"
    frame.createOrReplaceTempView(t1)
    spark.sql(s"CREATE TABLE $t2 (c0 $sqlSourceType)")
    spark.sql(s"CREATE TABLE $t3 (c0 $sqlDestType)")
    // insert into t2
    spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1")
    // copy from t2 to t1, with an ansi_cast()
    spark.sql(s"INSERT INTO $t3 SELECT c0 FROM $t2")
    spark.sql(s"SELECT c0 FROM $t3")
  }

  private def testNotSupportedOnGpu(testName: String, frame: SparkSession => DataFrame, sparkConf: SparkConf)(transformation: DataFrame => DataFrame): Unit = {
    test(testName) {
      try {
        withGpuSparkSession(spark => {
          val input = frame(spark).repartition(1)
          transformation(input).collect()
        }, sparkConf)
        fail("should not run on GPU")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.startsWith("Part of the plan is not columnar"))
      }
    }
  }

  private def testData(dt: DataType)(spark: SparkSession) = {
    val schema = FuzzerUtils.createSchema(Seq(dt))
    FuzzerUtils.generateDataFrame(spark, schema, 100)
  }

}
