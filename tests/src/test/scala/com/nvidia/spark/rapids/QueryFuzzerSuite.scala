/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.SparkSessionHolder.withSparkSession
import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

class QueryFuzzerSuite extends SparkQueryCompareTestSuite {

  // this is a manual test that we should never enable
  ignore("Execute random plans forever until we hit a failure case") {
    // sometimes we generate plans that are trivial and we end up testing the
    // same seed multiple times if we always use the current time in millis, so
    // we start with current time and then just increment it
    val initialSeed = System.currentTimeMillis()
    var i = 0
    while (true) {
      val seed = initialSeed + i
      println(s"Running query #$i with seed=$seed")
      compareRandomPlansCpuGpu(seed)
      i += 1
    }
  }

  def compareRandomPlansCpuGpu(seed: Long) {

    def executeRandomPlan(spark: SparkSession): Array[Row] = {
      // create new fuzzer here so that CPU and GPU runs use the same seed
      // and get the same initial query plans
      val fuzzer = new QueryFuzzer(seed)
      val df = fuzzer.randomOperator(spark, 0, 9)
      println(df.queryExecution.executedPlan)
      val rows = df.collect()
      println(df.queryExecution.executedPlan)
      rows
    }

    val fuzzer = new QueryFuzzer(seed)
    val sparkConf = fuzzer.generateConfig()

    val tryCpu = Try(withCpuSparkSession(executeRandomPlan, sparkConf))

    val tryGpu = Try(withGpuSparkSession(executeRandomPlan, sparkConf))

    (tryCpu, tryGpu) match {
      case (Success(cpu), Success(gpu)) =>
        compareResults(sort = true, maxFloatDiff = 0.0001, cpu, gpu)
      case (Success(_), Failure(gpu)) =>
        fail(s"[seed=$seed] CPU run succeeded. GPU run failed.", gpu)
      case (Failure(cpu), Success(_)) =>
        fail(s"[seed=$seed] GPU run succeeded. CPU run failed.", cpu)
      case (Failure(cpu), Failure(gpu)) =>
        // This is fine for now, but it would be nice to see if we could do a better
        // job of determining if they both failed for the same reason. It is not always
        // possible for us to produce the exact same error message as Spark.
        if (cpu.getMessage != gpu.getMessage) {
          println(s"[seed=$seed] Query failed both on CPU and GPU:\nCPU: $cpu\nGPU: $gpu")
          showStackTrace("CPU", cpu)
          showStackTrace("GPU", gpu)
        }
    }
  }

  private def showStackTrace(title: String, e: Throwable): Unit = {
    println(s"===== $title stack trace =====")
    e.printStackTrace()
  }

  override def withGpuSparkSession[U](
      f: SparkSession => U,
      conf: SparkConf = new SparkConf()): U = {
    val c = conf.clone()
      .set(RapidsConf.SQL_ENABLED.key, "true")
      .set(RapidsConf.EXPLAIN.key, "ALL")
    withSparkSession(c, f)
  }

  override def compareResults(
      sort: Boolean,
      maxFloatDiff:Double,
      fromCpu: Array[Row],
      fromGpu: Array[Row]): Unit = {

    if (fromCpu.length != fromGpu.length) {
      fail(s"Row counts: CPU=${fromCpu.length}. GPU=${fromGpu.length}")
    }

    val cpu = fromCpu.map(_.toSeq).sortWith(seqLt)
    val gpu = fromGpu.map(_.toSeq).sortWith(seqLt)

    var count = 0

    for ((l,r) <- cpu.zip(gpu)) {
      if (compare(l, r, maxFloatDiff)) {
        count += 1
      } else {
        fail(
          s"""Failed after $count matching rows
             |CPU: $l
             |GPU: $r
         """.
            stripMargin)
      }
    }
  }
}

