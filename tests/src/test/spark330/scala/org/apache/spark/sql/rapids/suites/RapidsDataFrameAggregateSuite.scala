/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.{DataFrameAggregateSuite, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsTrait

class RapidsDataFrameAggregateSuite extends DataFrameAggregateSuite with RapidsSQLTestsTrait {
  // example to show how to replace the logic of an excluded test case in Vanilla Spark
  testRapids("collect functions" ) {  // "collect functions" was excluded at RapidsTestSettings
    // println("...")
  }
  import testImplicits._

  test("groupBy22") {

    com.nvidia.spark.rapids.profiling.DumpedExecReplayer.main(
      Array("/tmp/lore/plan_id=290/partition_id=0"))
    println("xxx")
    scala.io.StdIn.readLine()


    testData2.createOrReplaceTempView("testData2")
    spark.sql("select a, sum(b) from testData2 where a < (select max(y) from values" +
      " (1,2),(2,3),(3,4) as data(x,y)    ) group by a").show()
    println("...")
    scala.io.StdIn.readLine()


    checkAnswer(
      testData2.groupBy("a").agg(sum($"b")),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3))
    )
    println("...")
    scala.io.StdIn.readLine()

  }
}
