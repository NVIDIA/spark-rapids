/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf

class GpuLoreSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir with Logging {
  test("Test aggregate replay") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "10")

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))

      val res = df.collect().length


      val restoredRes = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-10"),
          spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(res == restoredRes)
    }
  }

  test("Test broadcast join replay") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "32")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))
      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))

      val df = df1.join(df2, Seq("key"))

      val res = df.collect().length


      val resCount = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
            new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-32"),
            spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(res == resCount)
    }
  }

  test("Test subquery") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "13")

      spark.range(0, 100, 1, 10)
        .createTempView("df1")

      spark.range(50, 1000, 1, 10)
        .createTempView("df2")

      val df = spark.sql("select * from df1 where id > (select max(id) from df2)")

      val res = df.collect().length


      val resCount = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-13"),
        spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(res == resCount)
    }
  }

  test("Test no broadcast join replay") {
    withGpuSparkSession{ spark =>
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "30")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))
      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))

      val df = df1.join(df2, Seq("key"))

      val res = df.collect().length


      val resCount = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-30"),
        spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(res == resCount)
    }
  }

  test("Test AQE replay") {
    withGpuSparkSession{ spark =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "77")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))
      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key").agg(functions.sum("value").as("count"))

      val df = df1.join(df2, Seq("key"))

      val res = df.collect().length


      val resCount = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-77"),
        spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(res == resCount)
    }
  }

  test("Partition only") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[0 2]")

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      val res = df.collect().length
      println(s"Length of original: $res")


      val restoredRes = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-3"),
        spark.sparkContext.hadoopConfiguration))
        .executeCollect()
        .length

      assert(20 == restoredRes)
    }
  }
}
