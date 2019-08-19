/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SparkSession

class TpchLikeSparkTest extends FunSuite with BeforeAndAfterAll {

  lazy val  session: SparkSession = {
    SparkSession.builder
      .master("local[2]")
      .appName("TPCHLikeTest")
      .config("spark.sql.extensions", "ai.rapids.spark.Plugin")
      .config("spark.executor.plugins", "ai.rapids.spark.GpuResourceManager")
      .getOrCreate()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TpchLikeSpark.setupAllParquet(session, "src/test/resources/tpch/")
  }

  test("Something like TPCH Query 1") {
    val df = Q1Like(session)
    assertResult(4)(df.count())
  }

  test("Something like TPCH Query 2") {
    val df = Q2Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 3") {
    val df = Q3Like(session)
    assertResult(3)(df.count())
  }

  test("Something like TPCH Query 4") {
    val df = Q4Like(session)
    assertResult(5)(df.count())
  }

  test("Something like TPCH Query 5") {
    val df = Q5Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 6") {
    val df = Q6Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 7") {
    val df = Q7Like(session)
    assertResult(0)(df.count())
  }

  test("Something like TPCH Query 8") {
    val df = Q8Like(session)
    assertResult(0)(df.count())
  }

  test("Something like TPCH Query 9") {
    val df = Q9Like(session)
    assertResult(5)(df.count())
  }

  test("Something like TPCH Query 10") {
    val df = Q10Like(session)
    assertResult(4)(df.count())
  }

  test("Something like TPCH Query 11") {
    val df = Q11Like(session)
    assertResult(47)(df.count())
  }

  test("Something like TPCH Query 12") {
    val df = Q12Like(session)
    assertResult(2)(df.count())
  }

  test("Something like TPCH Query 13") {
    val df = Q13Like(session)
    assertResult(6)(df.count())
  }

  test("Something like TPCH Query 14") {
    val df = Q14Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 15") {
    val df = Q15Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 16") {
    val df = Q16Like(session)
    assertResult(42)(df.count())
  }

  test("Something like TPCH Query 17") {
    val df = Q17Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 18") {
    val df = Q18Like(session)
    assertResult(0)(df.count())
  }

  test("Something like TPCH Query 19") {
    val df = Q19Like(session)
    assertResult(1)(df.count())
  }

  test("Something like TPCH Query 20") {
    val df = Q20Like(session)
    assertResult(0)(df.count())
  }

  test("Something like TPCH Query 21") {
    val df = Q21Like(session)
    assertResult(0)(df.count())
  }

  test("Something like TPCH Query 22") {
    val df = Q22Like(session)
    assertResult(7)(df.count())
  }
}
