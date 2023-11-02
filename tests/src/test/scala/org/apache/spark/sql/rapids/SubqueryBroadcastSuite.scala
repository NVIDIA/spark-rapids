/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.util.UUID

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.Utils

class SubqueryBroadcastSuite extends SparkQueryCompareTestSuite {

  private def withTable(spark: SparkSession, tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  test("[SPARK-44897][SQL] Propagating local properties to subquery broadcast exec") {
    withGpuSparkSession(spark => {
      withSQLConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD.key -> "1",
                  "spark.rapids.sql.test.enabled" -> "false") {
      // withSQLConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD.key -> "1") {
        withTable(spark, "a", "b") {
          val confKey = "spark.sql.y"
          import spark.implicits._

          def generateBroadcastDataFrame(confKey: String, confValue: String): Dataset[String] = {
            val df = spark.range(1).mapPartitions { _ =>
              Iterator(TaskContext.get.getLocalProperty(confKey))
            }.filter($"value".contains(confValue)).as("c")
            df.hint("broadcast")
          }

          val confValue1 = UUID.randomUUID().toString()
          Seq((confValue1, "1")).toDF("key", "value")
            .write
            .format("parquet")
            .partitionBy("key")
            .mode("overwrite")
            .saveAsTable("a")
          val df1 = spark.table("a")

          // // set local property and assert
          val df2 = generateBroadcastDataFrame(confKey, confValue1)
          spark.sparkContext.setLocalProperty(confKey, confValue1)
          println("df1 " + df1.collect().mkString(","))
          println("df2 " + df2.collect().mkString(","))
          val checkDF = df1.join(df2).where($"a.key" === $"c.value").select($"a.key", $"c.value")
          println(checkDF.explain())
          val checks = checkDF.collect()
          println("checks " + checks.mkString(","))
          assert(checks.forall(_.toSeq == Seq(confValue1, confValue1)))
          // assert(checks.nonEmpty)

          // change local property and re-assert
          val confValue2 = UUID.randomUUID().toString()
          Seq((confValue2, "1")).toDF("key", "value")
            .write
            .format("parquet")
            .partitionBy("key")
            .mode("overwrite")
            .saveAsTable("b")
          val df3 = spark.table("b")
          val df4 = generateBroadcastDataFrame(confKey, confValue2)
          spark.sparkContext.setLocalProperty(confKey, confValue2)
          println("df3 " + df3.collect().mkString(","))
          println("df4 " + df4.collect().mkString(","))
          val checks2DF = df3.join(df4).where($"b.key" === $"c.value").select($"b.key", $"c.value")
          println("df3.join(df4) " + df3.join(df4).collect().mkString(","))
          println("df3.join(df4).where() " + df3.join(df4).where($"b.key" === $"c.value")
              .collect().mkString(","))
          println("checks2DF " + checks2DF.collect().mkString(","))
          val checks2 = checks2DF.collect()
          println("checks2 " + checks2.mkString(","))
          assert(checks2.forall(_.toSeq == Seq(confValue2, confValue2)))
          assert(checks2.nonEmpty)

          // val checkDF3 = df1.join(df2).where($"a.key" === $"c.value")
          //     .select($"a.key", $"c.value")
          // val checks3 = checkDF3.collect()
          // println("checks3 " + checks3.mkString(","))
        }
      }
    })
  }
}
