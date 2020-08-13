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

import com.nvidia.spark.rapids.{ColumnarRdd, ExecutionPlanCaptureCallback}
import com.nvidia.spark.rapids.ShimLoader
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class TpchLikeSparkSuite extends FunSuite with BeforeAndAfterAll {

  /**
   * This is intentionally a def rather than a val so that scalatest uses the correct value (from
   * this class or the derived class) when registering tests.
   */
  def adaptiveQueryEnabled = false

  lazy val session: SparkSession = {
    var builder = SparkSession.builder
      .master("local[2]")
      .appName("TPCHLikeTest")
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.queryExecutionListeners",
        classOf[ExecutionPlanCaptureCallback].getCanonicalName)
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.test.enabled", false)
      .config("spark.rapids.sql.explain", true)
      .config("spark.rapids.sql.incompatibleOps.enabled", true)
      .config("spark.rapids.sql.hasNans", false)
    val rapidsShuffle = ShimLoader.getSparkShims.getRapidsShuffleManagerClass
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

  private def testTpchLike(
      name: String,
      expectedRowCount: Int)
      (fun: SparkSession => DataFrame): Unit = {
    var qualifiedName = name
    if (adaptiveQueryEnabled) {
      qualifiedName += " AQE"
    }
    test(qualifiedName) {
      session.conf.set("spark.sql.adaptive.enabled", adaptiveQueryEnabled)
      ExecutionPlanCaptureCallback.startCapture()
      val df = fun(session)
      val c = df.count()
      val plan = ExecutionPlanCaptureCallback.getResultWithTimeout()
      assert(plan.isDefined)
      assertResult(adaptiveQueryEnabled)(plan.get.isInstanceOf[AdaptiveSparkPlanExec])
      assert(expectedRowCount == c)
    }
  }

  testTpchLike("Something like TPCH Query 1", 4) {
    session => Q1Like(session)
  }

  testTpchLike("Something like TPCH Query 2", 1) {
    session => Q2Like(session)
  }

  testTpchLike("Something like TPCH Query 3", 3) {
    session => Q3Like(session)
  }

  testTpchLike("Something like TPCH Query 4", 5) {
    session => Q4Like(session)
  }

  testTpchLike("Something like TPCH Query 5", 1) {
    session => Q5Like(session)
  }

  testTpchLike("Something like TPCH Query 6", 1) {
    session => Q6Like(session)
  }

  testTpchLike("Something like TPCH Query 7", 0) {
    session => Q7Like(session)
  }

  testTpchLike("Something like TPCH Query 8", 0) {
    session => Q8Like(session)
  }

  testTpchLike("Something like TPCH Query 9", 5) {
    session => Q9Like(session)
  }

  testTpchLike("Something like TPCH Query 10", 4) {
    session => Q10Like(session)
  }

  testTpchLike("Something like TPCH Query 11", 47) {
    session => Q11Like(session)
  }

  testTpchLike("Something like TPCH Query 12", 2) {
    session => Q12Like(session)
  }

  testTpchLike("Something like TPCH Query 13", 6) {
    session => Q13Like(session)
  }

  testTpchLike("Something like TPCH Query 14", 1) {
    session => Q14Like(session)
  }

  testTpchLike("Something like TPCH Query 15", 1) {
    session => Q15Like(session)
  }

  testTpchLike("Something like TPCH Query 16", 42) {
    session => Q16Like(session)
  }

  testTpchLike("Something like TPCH Query 17", 1) {
    session => Q17Like(session)
  }

  testTpchLike("Something like TPCH Query 18", 0) {
    session => Q18Like(session)
  }

  testTpchLike("Something like TPCH Query 19", 1) {
    session => Q19Like(session)
  }

  testTpchLike("Something like TPCH Query 20", 0) {
    session => Q20Like(session)
  }

  testTpchLike("Something like TPCH Query 21", 0) {
    session => Q21Like(session)
  }

  testTpchLike("Something like TPCH Query 22", 7) {
    session => Q22Like(session)
  }
}
