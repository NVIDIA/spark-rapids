/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// TODO move test to shim layer
class KeyGroupedPartitioningSuite extends SparkQueryCompareTestSuite {

  test("SPARK-44641: duplicated records when SPJ is not triggered") {

    val conf = new SparkConf()
      .set(SQLConf.USE_V1_SOURCE_LIST.key, "") // force v2 read
      .set(SQLConf.V2_BUCKETING_ENABLED.key, "true")

    withCpuSparkSession(spark => {
      import spark.implicits._

      val items = Seq(
        (1, "aa", 40.0, "2020-01-01"),
        (1, "aa", 41.0, "2020-01-15"),
        (2, "bb", 10.0, "2020-01-01"),
        (2, "bb", 10.5, "2020-01-01"),
        (3, "cc", 15.5, "2020-02-01"),
      ).toDF("id", "name", "price", "arrive_time_str")

      items.withColumn("arrive_time", col("arrive_time_str").cast(DataTypes.TimestampType))
        .drop("arrive_time_str")
        .write
        .bucketBy(8, "id")
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .option("path", "/tmp/items")
        .saveAsTable("_items")

      spark.read.parquet("/tmp/items").createTempView("items")

      val purchases = Seq(
        (1, 42.0, "2020-01-01"),
        (1, 44.0, "2020-01-15"),
        (1, 45.0, "2020-01-15"),
        (2, 11.0, "2020-01-01"),
        (3, 19.5, "2020-02-01"),
      ).toDF("item_id", "price", "time_str")

      purchases.withColumn("time", col("time_str").cast(DataTypes.TimestampType))
        .drop("time_str").write
        .bucketBy(8, "item_id")
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .option("path", "/tmp/purchases")
        .saveAsTable("_purchases")

      spark.read.parquet("/tmp/purchases").createTempView("purchases")

      null.asInstanceOf[DataFrame]
    }, conf)

    val query =
      """
      SELECT id, name, i.price as purchase_price, p.item_id, p.price as sale_price
      FROM items i JOIN purchases p
      ON i.arrive_time = p.time ORDER BY id, purchase_price, p.item_id, sale_price
      """

    Seq(true, false).foreach { pushDownValues =>
      Seq(true, false).foreach { partiallyClusteredEnabled =>
        val conf = new SparkConf()
          .set(RapidsConf.EXPLAIN.key, "ALL")
          .set(SQLConf.USE_V1_SOURCE_LIST.key, "") // force v2 read
          .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
          .set(SQLConf.V2_BUCKETING_ENABLED.key, "true")
          .set(SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED.key, pushDownValues.toString)
          .set(SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED.key,
            partiallyClusteredEnabled.toString)
        // TODO assert that CPU plan contains BatchScanExec
        val fromCpu = withCpuSparkSession(spark => {
          val df = spark.sql(query)
          val rows = df.collect()
          print(df.queryExecution.executedPlan)
          rows
        }, conf)
        // TODO assert that GPU plan contains GpuBatchScanExec
        val fromGpu = withGpuSparkSession(spark => {
          val df = spark.sql(query)
          print(df.queryExecution.executedPlan)
          val rows = df.collect()
          rows
        }, conf)
        println(s"CPU: ${fromCpu.mkString}")
        println(s"GPU: ${fromGpu.mkString}")
        compareResults(sort = false, floatEpsilon = 0.0001, fromCpu, fromGpu)
      }
    }
  }
}
