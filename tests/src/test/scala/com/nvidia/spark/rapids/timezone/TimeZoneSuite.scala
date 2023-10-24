/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.timezone

import java.time.{Instant, ZoneId}
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class TimeZoneSuite extends SparkQueryCompareTestSuite {
  /**
   * create timestamp column vector
   */
  def createColumnVector(epochSeconds: Array[Long]): ColumnVector = {
    val rows = epochSeconds.length
    withResource(HostColumnVector.builder(DType.TIMESTAMP_MICROSECONDS, rows)) { builder =>
      var idx = 0
      while (idx < rows) {
        // change seconds to micro seconds
        builder.append(epochSeconds(idx) * 1000000L)
        idx += 1
      }
      withResource(builder.build()) { b =>
        b.copyToDevice()
      }
    }
  }

  /**
   * create Spark data frame with a timestamp with name ts
   * @return
   */
  def createDF(spark: SparkSession, epochSeconds: Array[Long]): DataFrame = {
    val data = epochSeconds.indices.map(i => Row(epochSeconds(i)))
    val schema = StructType(Array(StructField("ts_long", LongType)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    // to_timestamp cast UTC seconds to timestamp
    df.withColumn("ts", to_timestamp(df.col("ts_long")))
  }

  /**
   * Use spark to generate parquet file that will be tested
   */
  def writeTimestampToParquet(longs: Array[Long], path: String): Unit = {
    withCpuSparkSession(
      spark => createDF(spark, longs).write.mode("overwrite").parquet(path),
      // write in UTC time zone
      new SparkConf().set("spark.sql.session.timeZone", "UTC")
          .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED"))
  }

  /**
   * assert result from CpuTimeZoneDB and Spark
   */
  def assertRet(cpuPocRetCv: ColumnVector, sparkRet: Seq[Row]): Unit = {
    withResource(cpuPocRetCv.copyToHost()) { host =>
      assert(cpuPocRetCv.getRowCount == sparkRet.length)
      for (i <- 0 until cpuPocRetCv.getRowCount.toInt) {
        val sparkInstant = sparkRet(i).getTimestamp(0).toInstant
        val sparkMicro = sparkInstant.getEpochSecond * 1000000L + sparkInstant.getNano / 1000L
        assert(host.getLong(i) == sparkMicro)
      }
    }
  }

  test("test all time zones") {
    assume(false,
      "It's time consuming for test all time zones, by default it's disabled")
    //    val zones = ZoneId.getAvailableZoneIds.asScala.toList.map(z => ZoneId.of(z)).filter { z =>
    //      val rules = z.getRules
    //      // remove this line when we support repeat rules
    //      rules.isFixedOffset && rules.getTransitionRules.isEmpty
    //    }

    // Currently only test China time zone
    val zones = Array[ZoneId](ZoneId.of("Asia/Shanghai"))
    // iterate zone, year and seconds every 15 minutes
    for (zone <- zones) {
      // For the years before 1900, there are diffs, need to investigate
      for (year <- 1900 until 9999 by 10) {
        val s = Instant.parse("%04d-01-01T00:00:00z".format(year)).getEpochSecond
        val e = Instant.parse("%04d-01-01T00:00:00z".format(year + 1)).getEpochSecond
        val longs = mutable.ArrayBuffer[Long]()
        for (v <- s until e by TimeUnit.MINUTES.toSeconds(15)) {
          longs += v
        }
        // data saves epoch seconds
        val data = longs.toArray

        withTempPath { file =>
          writeTimestampToParquet(data, file.getAbsolutePath)
          val sparkRet = withCpuSparkSession(
            spark => {
              spark.read.parquet(file.getAbsolutePath).createOrReplaceTempView("tab")
              // refer to https://spark.apache.org/docs/latest/api/sql/#from_utc_timestamp
              // convert timestamp to Shanghai timezone
              spark.sql("select from_utc_timestamp(ts, 'Asia/Shanghai') from tab").collect()
            },
            new SparkConf().set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"))

          withResource(createColumnVector(data)) { inputCv =>
            // convert time zone from UTC to Shanghai
            withResource(CpuTimeZoneDB.convertFromUTC(
              inputCv,
              ZoneId.of("Asia/Shanghai"))) { cpuPocRetCv =>
              assertRet(cpuPocRetCv, sparkRet)
            }
          }
        }
      }
    }
  }
}
