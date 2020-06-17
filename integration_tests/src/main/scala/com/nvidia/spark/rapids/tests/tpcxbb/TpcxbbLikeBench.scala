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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}


object TpcxbbLikeBench extends Logging {
  def runBench(
      spark: SparkSession,
      queryRunner: SparkSession => DataFrame): Unit = {
    var start = System.nanoTime()
    queryRunner(spark).collect
    var end = System.nanoTime()
    println(s"*** Cold run 1 msec: ${NANOSECONDS.toMillis(end - start)}")

    println("*** Start hot run 1:")
    start = System.nanoTime()
    queryRunner(spark).collect
    end = System.nanoTime()
    println(s"*** Hot run 1 msec: ${NANOSECONDS.toMillis(end - start)}")

    println("*** Start hot run 2:")
    start = System.nanoTime()
    queryRunner(spark).collect
    end = System.nanoTime()
    println(s"*** Hot run 2 msec: ${NANOSECONDS.toMillis(end - start)}")

    println("*** Start hot run 3:")
    start = System.nanoTime()
    queryRunner(spark).collect
    end = System.nanoTime()
    println(s"*** Hot run 3 msec: ${NANOSECONDS.toMillis(end - start)}")
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
