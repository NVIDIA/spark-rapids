/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.io.File
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.{Random, TimeZone}
import java.util.concurrent.TimeUnit

import com.nvidia.spark.rapids.SparkQueryCompareTestSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Test suite for ORC reader/writer timezones.
 *
 * Test all combinations of writer/reader for the following timezones:
 *   - `UTC`
 *   - `America/New_York`
 *   - `America/Los_Angeles`
 *   - `Asia/Shanghai`
 *
 * For each of the 16 writer/reader timezone pairs x 2 datasource versions (v1/v2), the suite:
 *   1. Writes an ORC file on CPU with JVM default timezone set to the writer timezone.
 *   2. Reads it back with JVM default timezone set to each reader timezone.
 *   3. Compares CPU and GPU read results for correctness.
 *
 * Note: reader/writer timezones are controlled by `TimeZone.getDefault`.
 * TimeZone must be set INSIDE the session lambda because resetSparkSessionConf
 * restores spark.sql.session.timeZone to the original value (UTC),
 * which also resets TimeZone.getDefault().
 *
 * Run it manually with:
 *   mvn test -DwildcardSuites=com.nvidia.spark.rapids.OrcTimezoneSuite -Dbuildver=xxx
 *
 * Note: use `orc-tool meta -t orc_file` to view the timezone in each stripe metadata.
 * Each stripe has a timezone in its metadata.
 */
class OrcTimezoneSuite extends SparkQueryCompareTestSuite {

  private val RowCount = 1024L * 1024L

  private val timezones = Seq(
    "UTC",
    "America/New_York",
    "America/Los_Angeles",
    "Asia/Shanghai"
  )

  // start timestamp from 1970-01-02 instead of 1970-01-01 to avoid the epoch boundary issue:
  // https://github.com/rapidsai/cudf/issues/21993

  private val minTs =
    LocalDateTime.of(1970, 1, 2, 0, 0, 0).toEpochSecond(ZoneOffset.UTC) *
      TimeUnit.SECONDS.toMicros(1)
  private val maxTs =
    LocalDateTime.of(9999, 12, 31, 23, 59, 59).toEpochSecond(ZoneOffset.UTC) *
      TimeUnit.SECONDS.toMicros(1) + 999999L

  private def setSessionTimeZone(spark: SparkSession, tzId: String): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone(tzId))
    spark.conf.set("spark.sql.session.timeZone", tzId)
  }

  private def fileDataFrame(spark: SparkSession, random: Random): DataFrame = {
    import spark.implicits._
    val rowCount = RowCount.toInt
    val micros = random.longs(rowCount.toLong, minTs, maxTs).toArray
    val rows = (0 until rowCount).map { i =>
      val us = micros(i)
      val seconds = Math.floorDiv(us, TimeUnit.SECONDS.toMicros(1))
      val microsWithinSecond = Math.floorMod(us, TimeUnit.SECONDS.toMicros(1))
      val ts = Timestamp.from(Instant.ofEpochSecond(seconds, microsWithinSecond * 1000L))
      (i.toLong, ts)
    }
    rows.toDF("id", "ts")
  }

  private val v1SourceLists = Seq("orc", "")

  private def baseConf(v1SourceList: String): SparkConf = {
    new SparkConf()
      .set("spark.sql.sources.useV1SourceList", v1SourceList)
  }

  private def writeFile(spark: SparkSession, outputPath: File, random: Random): Unit = {
    fileDataFrame(spark, random)
      .coalesce(1)
      .write
      .mode("overwrite")
      .orc(outputPath.getCanonicalPath)
  }

  private def readFile(path: File)(spark: SparkSession): DataFrame = {
    spark.read.orc(path.getCanonicalPath)
  }

  for {
    writerTimeZone <- timezones
    v1SourceList <- v1SourceLists
  } {
    val dsLabel = if (v1SourceList == "orc") "v1" else "v2"
    test(s"ORC timezone matrix ($dsLabel) for writer timezone $writerTimeZone") {
      val runSeed = scala.util.Random.nextLong()
      println(s"OrcTimezoneSuite random seed: $runSeed")
      val random = new Random(runSeed)
      val conf = baseConf(v1SourceList)
      val existClass = if (v1SourceList == "orc") "GpuFileSourceScanExec" else "GpuBatchScan"

      withTempPath { fileRoot =>
        withCpuSparkSession(spark => {
          setSessionTimeZone(spark, writerTimeZone)
          writeFile(spark, fileRoot, random)
        }, conf = conf)

        timezones.foreach { readerTimeZone =>
          withClue(s"writerTimezone=$writerTimeZone readerTimezone=$readerTimeZone " +
              s"datasource=$dsLabel") {
            val (fromCpu, fromGpu) = runOnCpuAndGpu(
              spark => {
                setSessionTimeZone(spark, readerTimeZone)
                readFile(fileRoot)(spark)
              },
              _.orderBy("id"),
              conf = conf,
              repart = 0,
              skipCanonicalizationCheck = true,
              existClasses = existClass)
            compareResults(
              sort = false,
              floatEpsilon = 0.0,
              fromCpu = fromCpu,
              fromGpu = fromGpu)
          }
        }
      }
    }
  }
}
