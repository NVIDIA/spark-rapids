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

import java.util.concurrent.TimeUnit

import ai.rapids.sparkexamples.DebugRange

import org.apache.spark.sql.SparkSession

object Benchmarks {
  def session: SparkSession = {
    val builder = SparkSession.builder.appName("TPCHLikeJob")

    val master = System.getenv("SPARK_MASTER")
    if (master != null) {
      builder.master(master)
    }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    spark.sqlContext.clearCache()

    spark
  }

  def runQueries(output: String, args: Array[String]): Unit = {
    (1 until args.length).foreach(index => {
      val query = args(index)
      System.err.println(s"QUERY: ${query}")
      val df = query match {
        case "1" => Q1Like(session)
        case "2" => Q2Like(session)
        case "3" => Q3Like(session)
        case "4" => Q4Like(session)
        case "5" => Q5Like(session)
        case "6" => Q6Like(session)
        case "7" => Q7Like(session)
        case "8" => Q8Like(session)
        case "9" => Q9Like(session)
        case "10" => Q10Like(session)
        case "11" => Q11Like(session)
        case "12" => Q12Like(session)
        case "13" => Q13Like(session)
        case "14" => Q14Like(session)
        case "15" => Q15Like(session)
        case "16" => Q16Like(session)
        case "17" => Q17Like(session)
        case "18" => Q18Like(session)
        case "19" => Q19Like(session)
        case "20" => Q20Like(session)
        case "21" => Q21Like(session)
        case "22" => Q22Like(session)
      }
      val start = System.nanoTime()
      val range = new DebugRange(s"QUERY: ${query}")
      try {
        df.write.mode("overwrite").csv(output + "/" + query)
      } finally {
        range.close()
      }
      val end = System.nanoTime()
      System.err.println(s"QUERY: ${query} took ${TimeUnit.NANOSECONDS.toMillis(end - start)} ms")
    })
  }
}

object CSV {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val session = Benchmarks.session

    TpchLikeSpark.setupAllCSV(session, input)
    Benchmarks.runQueries(output, args.slice(1, args.length))
  }
}

object Parquet {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val session = Benchmarks.session

    TpchLikeSpark.setupAllParquet(session, input)
    Benchmarks.runQueries(output, args.slice(1, args.length))
  }
}
