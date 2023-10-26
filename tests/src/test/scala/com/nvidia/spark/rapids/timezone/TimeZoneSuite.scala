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
        // convert seconds to micro seconds
        builder.append(epochSeconds(idx) * 1000000L)
        idx += 1
      }
      withResource(builder.build()) { b =>
        b.copyToDevice()
      }
    }
  }

  /**
   * create Spark data frame, schema is [(ts_long: long)]
   * @return
   */
  def createDF(spark: SparkSession, epochSeconds: Array[Long]): DataFrame = {
    val data = epochSeconds.indices.map(i => Row(epochSeconds(i)))
    val schema = StructType(Array(StructField("ts_long", LongType)))
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * assert result with Spark result
   */
  def assertRet(actualRet: ColumnVector, sparkRet: Seq[Row]): Unit = {
    withResource(actualRet.copyToHost()) { host =>
      assert(actualRet.getRowCount == sparkRet.length)
      for (i <- 0 until actualRet.getRowCount.toInt) {
        val sparkInstant = sparkRet(i).getInstant(0)
        val sparkMicro = sparkInstant.getEpochSecond * 1000000L + sparkInstant.getNano / 1000L
        assert(host.getLong(i) == sparkMicro)
      }
    }
  }

  /**
  * Get all epoch seconds every 15 minutes in [startYear, endYear].
  */
  def getEpochSeconds(startYear: Int, endYear: Int): Array[Long] = {
    val s = Instant.parse("%04d-01-01T00:00:00z".format(startYear)).getEpochSecond
    val e = Instant.parse("%04d-01-01T00:00:00z".format(endYear)).getEpochSecond
    val epochSeconds = mutable.ArrayBuffer[Long]()
    for (epoch <- s until e by TimeUnit.MINUTES.toSeconds(15)) {
      epochSeconds += epoch
    }
    // Note: in seconds, not micro seconds
    epochSeconds.toArray
  }

  def testFromUTC(epochSeconds: Array[Long], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createDF(spark, epochSeconds).createOrReplaceTempView("tab")
        // refer to https://spark.apache.org/docs/latest/api/sql/#from_utc_timestamp
        // convert from UTC timestamp
        // first cast long value as timestamp
        spark.sql(s"select from_utc_timestamp(cast(ts_long as timestamp), '$zoneStr') from tab").collect()
      },
      new SparkConf()
        // use UTC time zone to create date frame
        .set("spark.sql.session.timeZone", "UTC")
        // by setting this, the Spark output for datetime type is java.time.Instant instead
        // of java.sql.Timestamp, it's convenient to compare result via java.time.Instant
        .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    val actualRet = withResource(createColumnVector(epochSeconds)) { inputCv =>
      // convert time zone from UTC to specific timezone
      CpuTimeZoneDB.convertFromUTC(
        inputCv,
        ZoneId.of(zoneStr))
    }

    withResource(actualRet) { _ =>
      assertRet(actualRet, sparkRet)
    }
  }

  def testToUTC(epochSeconds: Array[Long], zoneStr: String): Unit = {
    // get result from Spark
    val sparkRet = withCpuSparkSession(
      spark => {
        createDF(spark, epochSeconds).createOrReplaceTempView("tab")
        // refer to https://spark.apache.org/docs/latest/api/sql/#to_utc_timestamp
        // convert to UTC timezone
        // first cast long value as timestamp
        spark.sql(s"select to_utc_timestamp(cast(ts_long as timestamp), '$zoneStr') from tab").collect()
      },
      new SparkConf()
        // use UTC time zone to create date frame
        .set("spark.sql.session.timeZone", "UTC")
        // by setting this, the Spark output for datetime type is java.time.Instant instead
        // of java.sql.Timestamp, it's convenient to compare result via java.time.Instant
        .set("spark.sql.datetime.java8API.enabled", "true"))

    // get result from TimeZoneDB
    val actualRet = withResource(createColumnVector(epochSeconds)) { inputCv =>
      // convert time zone from UTC to specific timezone
      CpuTimeZoneDB.convertToUTC(
        inputCv,
        ZoneId.of(zoneStr))
    }

    withResource(actualRet) { _ =>
      assertRet(actualRet, sparkRet)
    }
  }

  test("test all time zones") {
    assume(true,
      "It's time consuming for test all time zones, by default it's disabled")
    //    val zones = ZoneId.getAvailableZoneIds.asScala.toList.map(z => ZoneId.of(z)).filter { z =>
    //      val rules = z.getRules
    //      // remove this line when we support repeat rules
    //      rules.isFixedOffset || rules.getTransitionRules.isEmpty
    //    }

    // Currently only test one zone
    val zones = Array[String]("Asia/Shanghai")
    // iterate zones
    for (zoneStr <- zones) {
      // iterate years
      for (year <- 1 until 9999) {
        val epochSeconds = getEpochSeconds(year, year + 1)
        testFromUTC(epochSeconds, zoneStr)
        testToUTC(epochSeconds, zoneStr)
      }
    }
  }
}
