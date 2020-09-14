/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.tpcds

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TpcdsLikeBench extends Logging {

  /**
   * This method can be called from Spark shell using the following syntax:
   *
   * TpcdsLikeBench.runBench(spark, "q5")
   */
  def runBench(
      spark: SparkSession,
      query: String,
      numColdRuns: Int = 1,
      numHotRuns: Int = 3): Unit = {

    val coldRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numColdRuns) {
      println(s"*** Start cold run $i:")
      val start = System.nanoTime()
      TpcdsLikeSpark.run(spark, query).collect
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      coldRunElapsed.append(elapsed)
      println(s"*** Cold run $i took $elapsed msec.")
    }

    val hotRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numHotRuns) {
      println(s"*** Start hot run $i:")
      val start = System.nanoTime()
      TpcdsLikeSpark.run(spark, query).collect
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      hotRunElapsed.append(elapsed)
      println(s"*** Hot run $i took $elapsed msec.")
    }

    for (i <- 0 until numColdRuns) {
      println(s"Cold run $i for query $query took ${coldRunElapsed(i)} msec.")
    }
    println(s"Average cold run took ${coldRunElapsed.sum.toDouble/numColdRuns} msec.")

    for (i <- 0 until numHotRuns) {
      println(s"Hot run $i for query $query took ${hotRunElapsed(i)} msec.")
    }
    println(s"Query $query: " +
        s"best: ${hotRunElapsed.min} msec; " +
        s"worst: ${hotRunElapsed.max} msec; " +
        s"average: ${hotRunElapsed.sum.toDouble/numHotRuns} msec.")
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val query = args(1)

    val spark = SparkSession.builder.appName("TPC-DS Like Bench").getOrCreate()
    TpcdsLikeSpark.setupAllParquet(spark, input)

    println(s"*** RUNNING TPC-DS QUERY $query")
    runBench(spark, query)
  }
}
