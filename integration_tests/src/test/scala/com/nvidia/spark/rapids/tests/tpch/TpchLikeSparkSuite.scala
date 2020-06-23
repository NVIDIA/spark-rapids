/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpch

import com.nvidia.spark.RapidsShuffleManager
import com.nvidia.spark.rapids.{ColumnarRdd, ExecutionPlanCaptureCallback}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class TpchLikeSparkSuite extends FunSuite with BeforeAndAfterAll {

  val adaptiveQueryEnabled = Option(System.getProperty("spark.sql.adaptive.enabled"))
      .getOrElse("false").toBoolean

  lazy val session: SparkSession = {
    var builder = SparkSession.builder
      .master("local[2]")
      .appName("TPCHLikeTest")
      .config("spark.sql.adaptive.enabled", adaptiveQueryEnabled)
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.queryExecutionListeners",
        "com.nvidia.spark.rapids.ExecutionPlanCaptureCallback")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.test.enabled", false)
      .config("spark.rapids.sql.explain", true)
      .config("spark.rapids.sql.incompatibleOps.enabled", true)
      .config("spark.rapids.sql.hasNans", false)
    val rapidsShuffle = classOf[RapidsShuffleManager].getCanonicalName
    val prop = System.getProperty("rapids.shuffle.manager.override", "false")
    if (prop.equalsIgnoreCase("true")) {
      println("RAPIDS SHUFFLE MANAGER ACTIVE")
      builder = builder.config("spark.shuffle.manager", rapidsShuffle)
    } else {
      println("RAPIDS SHUFFLE MANAGER INACTIVE")
    }

    builder.getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TpchLikeSpark.setupAllParquet(session, "src/test/resources/tpch/")
  }

  test("GPU data export with conversion") {
    val df = session.sql(
      """
        | select l_orderkey, SUM(l_quantity), SUM(l_discount), SUM(l_tax) from lineitem
        | group by l_orderkey
      """.stripMargin)
    val rdd = ColumnarRdd(df)
    assert(rdd != null)
    assert(255.0 == rdd.map(table => try {
      table.getRowCount
    } finally {
      table.close
    }).sum())
    // max order key
    assert(999 == rdd.map(table => try {
      table.getColumn(0).max().getLong
    } finally {
      table.close()
    }).max())
  }

  test("zero copy GPU data export") {
    val df = session.sql("""select l_orderkey, l_quantity, l_discount, l_tax from lineitem""")
    val rdd = ColumnarRdd(df)
    assert(rdd != null)
    assert(1000.0 == rdd.map(table => try {
      table.getRowCount
    } finally {
      table.close()
    }).sum())

    // Max order key
    assert(999 == rdd.map(table => try {
      table.getColumn(0).max().getLong
    } finally {
      table.close()
    }).max())
  }

  private def testTpchLike(expectedRowCount: Int, df: DataFrame): Unit = {
    ExecutionPlanCaptureCallback.startCapture()
    val c = df.count()
    val plan = ExecutionPlanCaptureCallback.getResultWithTimeout()
    assert(plan.isDefined)
    if (adaptiveQueryEnabled) {
      assert(plan.get.isInstanceOf[AdaptiveSparkPlanExec])
    } else {
      assert(!plan.get.isInstanceOf[AdaptiveSparkPlanExec])
    }
    assert(expectedRowCount == c)
  }

  test("Something like TPCH Query 1") {
    val df = Q1Like(session)
    testTpchLike(4, df)
  }

  test("Something like TPCH Query 2") {
    val df = Q2Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 3") {
    val df = Q3Like(session)
    testTpchLike(3, df)
  }

  test("Something like TPCH Query 4") {
    val df = Q4Like(session)
    testTpchLike(5, df)
  }

  test("Something like TPCH Query 5") {
    val df = Q5Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 6") {
    val df = Q6Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 7") {
    val df = Q7Like(session)
    testTpchLike(0, df)
  }

  test("Something like TPCH Query 8") {
    val df = Q8Like(session)
    testTpchLike(0, df)
  }

  test("Something like TPCH Query 9") {
    val df = Q9Like(session)
    testTpchLike(5, df)
  }

  test("Something like TPCH Query 10") {
    val df = Q10Like(session)
    testTpchLike(4, df)
  }

  test("Something like TPCH Query 11") {
    val df = Q11Like(session)
    testTpchLike(47, df)
  }

  test("Something like TPCH Query 12") {
    val df = Q12Like(session)
    testTpchLike(2, df)
  }

  test("Something like TPCH Query 13") {
    val df = Q13Like(session)
    testTpchLike(6, df)
  }

  test("Something like TPCH Query 14") {
    val df = Q14Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 15") {
    val df = Q15Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 16") {
    val df = Q16Like(session)
    testTpchLike(42, df)
  }

  test("Something like TPCH Query 17") {
    val df = Q17Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 18") {
    val df = Q18Like(session)
    testTpchLike(0, df)
  }

  test("Something like TPCH Query 19") {
    val df = Q19Like(session)
    testTpchLike(1, df)
  }

  test("Something like TPCH Query 20") {
    val df = Q20Like(session)
    testTpchLike(0, df)
  }

  test("Something like TPCH Query 21") {
    val df = Q21Like(session)
    testTpchLike(0, df)
  }

  test("Something like TPCH Query 22") {
    val df = Q22Like(session)
    testTpchLike(7, df)
  }
}
