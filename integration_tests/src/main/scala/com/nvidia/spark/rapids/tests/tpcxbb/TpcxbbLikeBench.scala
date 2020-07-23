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

package com.nvidia.spark.rapids.tests.tpcxbb

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object TpcxbbLikeBench extends Logging {

  /**
   * This method can be called from Spark shell using the following syntax:
   *
   * TpcxbbLikeBench.runBench(spark, Q5Like.apply)
   */
  def runBench(
      spark: SparkSession,
      queryRunner: SparkSession => DataFrame,
      numColdRuns: Int = 1,
      numHotRuns: Int = 3): Unit = {

    val coldRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numColdRuns) {
      println(s"*** Start cold run $i:")
      val start = System.nanoTime()
      queryRunner(spark).collect
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      coldRunElapsed.append(elapsed)
      println(s"*** Cold run $i took $elapsed msec.")
    }

    val hotRunElapsed = new ListBuffer[Long]()
    for (i <- 0 until numHotRuns) {
      println(s"*** Start hot run $i:")
      val start = System.nanoTime()
      queryRunner(spark).collect
      val end = System.nanoTime()
      val elapsed = NANOSECONDS.toMillis(end - start)
      hotRunElapsed.append(elapsed)
      println(s"*** Hot run $i took $elapsed msec.")
    }

    for (i <- 0 until numColdRuns) {
      println(s"Cold run $i took ${coldRunElapsed(i)} msec.")
    }
    println(s"Average cold run took ${coldRunElapsed.sum.toDouble/numColdRuns} msec.")

    for (i <- 0 until numHotRuns) {
      println(s"Hot run $i took ${hotRunElapsed(i)} msec.")
    }
    println(s"Average hot run took ${hotRunElapsed.sum.toDouble/numHotRuns} msec.")
  }

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val queryIndex = args(1).toInt

    val spark = SparkSession.builder.appName("TPCxBB Bench").getOrCreate()
    TpcxbbLikeSpark.setupAllParquet(spark, input)

    val queryRunner: SparkSession => DataFrame = queryIndex match {
      case 1 => Q1Like.apply
      case 2 => Q2Like.apply
      case 3 => Q3Like.apply
      case 4 => Q4Like.apply
      case 5 => Q5Like.apply
      case 6 => Q6Like.apply
      case 7 => Q7Like.apply
      case 8 => Q8Like.apply
      case 9 => Q9Like.apply
      case 10 => Q10Like.apply
      case 11 => Q11Like.apply
      case 12 => Q12Like.apply
      case 13 => Q13Like.apply
      case 14 => Q14Like.apply
      case 15 => Q15Like.apply
      case 16 => Q16Like.apply
      case 17 => Q17Like.apply
      case 18 => Q18Like.apply
      case 19 => Q19Like.apply
      case 20 => Q20Like.apply
      case 21 => Q21Like.apply
      case 22 => Q22Like.apply
      case 23 => Q23Like.apply
      case 24 => Q24Like.apply
      case 25 => Q25Like.apply
      case 26 => Q26Like.apply
      case 27 => Q27Like.apply
      case 28 => Q28Like.apply
      case 29 => Q29Like.apply
      case 30 => Q30Like.apply
      case _ => throw new IllegalArgumentException(s"Unknown TPCx-BB query number: $queryIndex")
    }

    println(s"*** RUNNING TPCx-BB QUERY $queryIndex")
    runBench(spark, queryRunner)
  }
}
