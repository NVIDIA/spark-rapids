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

package ai.rapids.sparkexamples.tpch

import java.util.concurrent.atomic.AtomicReference

import scala.util.{Failure, Success, Try}

import ai.rapids.spark.{ColumnarRdd, GpuExec, RapidsShuffleManager}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.util.QueryExecutionListener

class TpchLikeSparkSuite extends FunSuite with BeforeAndAfterAll {

  lazy val  session: SparkSession = {
    var builder = SparkSession.builder
        .master("local[2]")
        .appName("TPCHLikeTest")
        .config("spark.sql.adaptive.enabled", true) //TODO: enable this when all tests pass
        .config("spark.sql.join.preferSortMergeJoin", false)
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.plugins", "ai.rapids.spark.SQLPlugin")
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

  private val executedPlan = new AtomicReference[Try[SparkPlan]](null)

  override def beforeAll(): Unit = {
    super.beforeAll()
    TpchLikeSpark.setupAllParquet(session, "src/test/resources/tpch/")
    session.listenerManager.register(new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        println(qe)
        println("FINAL PLAN:\n" + qe.executedPlan)
        executedPlan.set(Success(qe.executedPlan))
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        executedPlan.set(Failure(exception))
      }
    })
  }

  //  ignore("GPU data export with conversion") {
  //    val df = session.sql(
  //      """
  //        | select l_orderkey, SUM(l_quantity), SUM(l_discount), SUM(l_tax) from lineitem
  //        | group by l_orderkey
  //      """.stripMargin)
  //    val rdd = ColumnarRdd(df)
  //    assert(rdd != null)
  //    assert(255.0 == rdd.map(table => try {
  //      table.getRowCount
  //    } finally {
  //      table.close
  //    }).sum())
  //    // max order key
  //    assert(999 == rdd.map(table => try {
  //      table.getColumn(0).max().getLong
  //    } finally {
  //      table.close()
  //    }).max())
  //  }
  //
  //  ignore("zero copy GPU data export") {
  //    val df = session.sql("""select l_orderkey, l_quantity, l_discount, l_tax from lineitem""")
  //    val rdd = ColumnarRdd(df)
  //    assert(rdd != null)
  //    assert(1000.0 == rdd.map(table => try {
  //      table.getRowCount
  //    } finally {
  //      table.close()
  //    }).sum())
  //
  //    // Max order key
  //    assert(999 == rdd.map(table => try {
  //      table.getColumn(0).max().getLong
  //    } finally {
  //      table.close()
  //    }).max())
  //  }

  private def count(df: DataFrame): Long = {
    executedPlan.set(null)
    // execute the query
    println("EXECUTING...")
    val count = df.count()
    // wait for async result from listener
    val startTime = System.currentTimeMillis()
    val timeout = 30000
    while (executedPlan.get() == null) {
      if (System.currentTimeMillis() > startTime + timeout) {
        fail("timed out waiting for query to finish or fail")
      }
      Thread.sleep(100)
    }
    executedPlan.get() match {
      case Success(a: AdaptiveSparkPlanExec) => a.executedPlan match {
        case WholeStageCodegenExec(child) => assert(child.isInstanceOf[GpuExec])
        case other => assert(other.isInstanceOf[GpuExec])
      }
      case Success(plan) => plan match {
        case WholeStageCodegenExec(child) => assert(child.isInstanceOf[GpuExec])
        case other => assert(other.isInstanceOf[GpuExec])
      }
      case Failure(e) => fail(e)
    }
    count
  }

    test("Something like TPCH Query 1") {
      val df = Q1Like(session)
      assertResult(4)(count(df))
    }

    ignore("Something like TPCH Query 2") {
      val df = Q2Like(session)
      assertResult(1)(count(df))
    }

    test("Something like TPCH Query 3") {
      val df = Q3Like(session)
      assertResult(3)(count(df))
    }

    ignore("Something like TPCH Query 4") {
      val df = Q4Like(session)
      assertResult(5)(count(df))
    }

    ignore("Something like TPCH Query 5") {
      val df = Q5Like(session)
      assertResult(1)(count(df))
    }

  test("Something like TPCH Query 6") {
      val df = Q6Like(session)
      assertResult(1)(count(df))
    }

    ignore("Something like TPCH Query 7") {
      val df = Q7Like(session)
      assertResult(0)(count(df))
    }

    ignore("Something like TPCH Query 8") {
      val df = Q8Like(session)
      assertResult(0)(count(df))
    }

    ignore("Something like TPCH Query 9") {
      val df = Q9Like(session)
      assertResult(5)(count(df))
    }

  test("Something like TPCH Query 10") {
      val df = Q10Like(session)
      assertResult(4)(count(df))
    }

    ignore("Something like TPCH Query 11") {
      val df = Q11Like(session)
      assertResult(47)(count(df))
    }

    ignore("Something like TPCH Query 12") {
      val df = Q12Like(session)
      assertResult(2)(count(df))
    }

    ignore("Something like TPCH Query 13") {
      val df = Q13Like(session)
      assertResult(6)(count(df))
    }

  test("Something like TPCH Query 14") {
    val df = Q14Like(session)
    assertResult(1)(count(df))
  }

    ignore("Something like TPCH Query 15") {
      val df = Q15Like(session)
      assertResult(1)(count(df))
    }

    ignore("Something like TPCH Query 16") {
      val df = Q16Like(session)
      assertResult(42)(count(df))
    }

    // TODO some kind of infinite loop happening here
    ignore("Something like TPCH Query 17") {
      val df = Q17Like(session)
      assertResult(1)(count(df))
    }

    ignore("Something like TPCH Query 18") {
      val df = Q18Like(session)
      assertResult(0)(count(df))
    }

  test("Something like TPCH Query 19") {
      val df = Q19Like(session)
      assertResult(1)(count(df))
    }

    ignore("Something like TPCH Query 20") {
      val df = Q20Like(session)
      assertResult(0)(count(df))
    }

    ignore("Something like TPCH Query 21") {
      val df = Q21Like(session)
      assertResult(0)(count(df))
    }

    ignore("Something like TPCH Query 22") {
      val df = Q22Like(session)
      assertResult(7)(count(df))
    }
}
