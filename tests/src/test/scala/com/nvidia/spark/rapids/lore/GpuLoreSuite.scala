/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, GpuColumnarToRowExec, RapidsConf, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions, DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf

class GpuLoreSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir with Logging {
  test("Aggregate") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("10[*]") { spark =>
      spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))
    }
  }

  test("Broadcast join") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("32[*]") { spark =>
      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("Subquery Filter") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("13[*]") { spark =>
      spark.range(0, 100, 1, 10)
        .createTempView("df1")

      spark.range(50, 1000, 1, 10)
        .createTempView("df2")

      spark.sql("select * from df1 where id > (select max(id) from df2)")
    }
  }

  test("Subquery in projection") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("11[*]") { spark =>
      spark.sql(
        """
          |CREATE TEMPORARY VIEW t1
          |AS SELECT * FROM VALUES
          |(1, "a"),
          |(2, "a"),
          |(3, "a") t(id, value)
          |""".stripMargin)

      spark.sql(
        """
          |SELECT *, (SELECT COUNT(*) FROM t1) FROM t1
          |""".stripMargin)
    }
  }

  test("No broadcast join") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("30[*]") { spark =>
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("AQE broadcast") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("93[*]") { spark =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("AQE Exchange") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("28[*]") { spark =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))
    }
  }

  test("Partition only") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[0 2]")

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      val res = df.collect().length
      println(s"Length of original: $res")


      val restoredRes = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-3"), spark))
        .executeCollect()
        .length

      assert(20 == restoredRes)
    }
  }

  test("Non-empty lore dump path") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[*]")

      //Create a file in the root path
      val path = new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/test")
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      withResource(fs.create(path, true)) { _ =>
      }

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      assertThrows[IllegalArgumentException] {
        df.collect()
      }
    }
  }

  test("GpuShuffledSymmetricHashJoin with SerializedTableColumn") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("56[*]") { spark =>
      // Disable broadcast join, force hash join
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      // Create larger tables to ensure shuffle
      val df1 = spark.range(0, 1000, 1, 100)
      .selectExpr("id % 10 as key", "id as value")
    val df2 = spark.range(0, 1000, 1, 100)
      .selectExpr("id % 10 as key", "id as value")
      // Join with equality condition to trigger hash join
      df1.join(df2, Seq("key"))
    }
  }

  test("GpuShuffledSymmetricHashJoin with in Kudo mode") {
    skipIfAnsiEnabled("https://github.com/NVIDIA/spark-rapids/issues/5114")
    doTestReplay("56[*]") { spark =>
      // Disable broadcast join, force hash join
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      // in Kudo mode
      spark.conf.set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")

      // Create larger tables to ensure shuffle
      val df1 = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id as value")
      val df2 = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id as value")
      // Join with equality condition to trigger hash join
      df1.join(df2, Seq("key"))
    }
  }

  private def doTestReplay(loreDumpIds: String)(dfFunc: SparkSession => DataFrame) = {
    val loreId = OutputLoreId.parse(loreDumpIds).head._1
    withGpuSparkSession { spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, loreDumpIds)

      val df = dfFunc(spark)

      val expectedLength = df.collect().length

      val restoredResultLength = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-$loreId"),
        spark))
        .executeCollect()
        .length

      assert(expectedLength == restoredResultLength)
    }
  }
}
