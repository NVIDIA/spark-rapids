/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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

package com.nvidia.spark.rapids.tests.orc

import java.io.File
import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.Arm.{withResource, withResourceIfAllowed}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{BinaryColumnStatistics, BooleanColumnStatistics, CollectionColumnStatistics, ColumnStatistics, DateColumnStatistics, DecimalColumnStatistics, DoubleColumnStatistics, IntegerColumnStatistics, OrcFile, StringColumnStatistics, TimestampColumnStatistics}
import org.apache.orc.impl.{ColumnStatisticsImpl, RecordReaderImpl}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.tests.datagen.DBGen

class OrcSuite extends TestBase {
  test("Statistics tests for ORC files written by GPU") {
    assume(true)
    val rowsNum: Long = 1024 * 1024

    def getStats: SparkSession => OrcStat = { spark =>
      withTempPath { tmpDir =>

        // define table
        val tab = DBGen().addTable("tab", statTestSchema, rowsNum)
        tab("c03").setNullProbability(0.5)
        tab("c08").setNullProbability(0.5)
        tab("c14").setNullProbability(0.5)

        // write to ORC file
        tab.toDF(spark).coalesce(1).write.mode("overwrite").orc(tmpDir.getAbsolutePath)

        // get Stats
        getStatsFromFile(tmpDir)
      }
    }

    // get CPU/GUP stats, assert
    val cpuStats = withCpuSparkSession(getStats)
    val gpuStats = withGpuSparkSession(getStats)
    assertResult(cpuStats)(gpuStats)
  }

  /**
   * Find a orc file in orcDir and get the stats. It's similar to output of `orc-tool meta file`
   *
   * @param orcDir orc file directory
   * @return (encodingKind, dictionarySize) list for each columns passed in
   */
  private def getStatsFromFile(orcDir: File): OrcStat = {
    val orcFile = orcDir.listFiles(f => f.getName.endsWith(".orc"))(0)
    val p = new Path(orcFile.getCanonicalPath)
    val conf = OrcFile.readerOptions(new Configuration())

    // cdh321 and cdh330 use a lower ORC version, and the reader is not a AutoCloseable,
    // so use withResourceIfAllowed
    withResourceIfAllowed(OrcFile.createReader(p, conf)) { reader =>
      val calendar = if (reader.writerUsedProlepticGregorian) {
        "Proleptic Gregorian"
      } else {
        "Julian/Gregorian"
      }
      val stripeColumnStats = reader.getStripeStatistics.asScala.toArray.map { strip =>
        StripColumnStat(strip.getColumnStatistics.map { cs => ColumnStat(cs) })
      }

      val fileColumnStats = reader.getStatistics.map(cs => ColumnStat(cs))

      val stripeFooterStats = withResource(reader.rows().asInstanceOf[RecordReaderImpl]) { rows =>
        reader.getStripes.asScala.toArray.map { stripe =>
          val stripeFooter = rows.readStripeFooter(stripe)
          val streams = stripeFooter.getStreamsList.asScala.toArray.map(stream =>
            StreamStat(stream.getColumn, stream.getKind.name()))
          val encodings = stripeFooter.getColumnsList.asScala.toArray.map { encoding =>
            val encodingName = encoding.getKind.toString
            if (encodingName.startsWith("DICTIONARY")) {
              s"$encodingName[${encoding.getDictionarySize}]"
            } else {
              encodingName
            }
          }
          StripeFooterStat(streams, encodings)
        }
      }

      OrcStat(
        reader.getNumberOfRows,
        reader.getCompressionKind.toString,
        calendar,
        reader.getSchema.toString,
        stripeColumnStats,
        fileColumnStats,
        stripeFooterStats
      )
    }
  }

  case class OrcStat(
      rows: Long,
      compression: String,
      calendar: String,
      schema: String,
      stripeColumnStats: Seq[StripColumnStat],
      fileColumnStats: Seq[ColumnStat],
      stripeFooterStats: Seq[StripeFooterStat])

  case class StripColumnStat(columnStats: Seq[ColumnStat])

  /**
   * Refer to ColumnStatisticsImpl.java
   * Exclude the check for bytesOnDisk attribute
   */
  case class ColumnStat(cs: ColumnStatistics) {
    def equalsForBasicStat(t: ColumnStatistics, o: ColumnStatistics): Boolean = {
      // skip check the bytesOnDisk
      t.getNumberOfValues == o.getNumberOfValues && t.hasNull == o.hasNull
    }

    // Note: no corresponding hashCode, should not be used in Map key
    override def equals(obj: Any): Boolean = obj match {
      case _@ColumnStat(thatCs) =>
        if (!equalsForBasicStat(cs, thatCs)) return false
        (cs, thatCs) match {
          case (dateStat: DateColumnStatistics, otherDateStat: DateColumnStatistics) =>
            Objects.equals(dateStat.getMinimumLocalDate, otherDateStat.getMinimumLocalDate) &&
                Objects.equals(dateStat.getMaximumLocalDate, otherDateStat.getMaximumLocalDate)
          case (doubleStat: DoubleColumnStatistics, otherDoubleStat: DoubleColumnStatistics) =>
            Objects.equals(doubleStat.getMinimum, otherDoubleStat.getMinimum) &&
                Objects.equals(doubleStat.getMaximum, otherDoubleStat.getMaximum) &&
                Objects.equals(doubleStat.getSum, otherDoubleStat.getSum)
          case (decimalStat: DecimalColumnStatistics, otherDecimalStat: DecimalColumnStatistics) =>
            Objects.equals(decimalStat.getMinimum, otherDecimalStat.getMinimum) &&
                Objects.equals(decimalStat.getMaximum, otherDecimalStat.getMaximum) &&
                Objects.equals(decimalStat.getSum, otherDecimalStat.getSum)
          case (boolStat: BooleanColumnStatistics, otherBoolStat: BooleanColumnStatistics) =>
            Objects.equals(boolStat.getFalseCount, otherBoolStat.getFalseCount)
          case (strStat: StringColumnStatistics, otherStrStat: StringColumnStatistics) =>
            Objects.equals(strStat.getLowerBound, otherStrStat.getLowerBound) &&
                Objects.equals(strStat.getUpperBound, otherStrStat.getUpperBound) &&
                Objects.equals(strStat.getMinimum, otherStrStat.getMinimum) &&
                Objects.equals(strStat.getMaximum, otherStrStat.getMaximum) &&
                Objects.equals(strStat.getSum, otherStrStat.getSum)
          case (integerStat: IntegerColumnStatistics, otherIntegerStat: IntegerColumnStatistics) =>
            Objects.equals(integerStat.getMinimum, otherIntegerStat.getMinimum) &&
                Objects.equals(integerStat.getMaximum, otherIntegerStat.getMaximum) &&
                Objects.equals(integerStat.isSumDefined, otherIntegerStat.isSumDefined) &&
                Objects.equals(integerStat.getSum, otherIntegerStat.getSum)
          case (binaryStat: BinaryColumnStatistics, otherBinaryStat: BinaryColumnStatistics) =>
            Objects.equals(binaryStat.getSum, otherBinaryStat.getSum)
          case (cStat: CollectionColumnStatistics, otherCStat: CollectionColumnStatistics) =>
            Objects.equals(cStat.getMinimumChildren, otherCStat.getMinimumChildren) &&
                Objects.equals(cStat.getMaximumChildren, otherCStat.getMaximumChildren) &&
                Objects.equals(cStat.getTotalChildren, otherCStat.getTotalChildren)
          case (tsStat: TimestampColumnStatistics, otherTsStat: TimestampColumnStatistics) =>
            Objects.equals(tsStat.getMinimum, otherTsStat.getMinimum) &&
                Objects.equals(tsStat.getMaximum, otherTsStat.getMaximum) &&
                Objects.equals(tsStat.getMinimumUTC, otherTsStat.getMinimumUTC) &&
                Objects.equals(tsStat.getMaximumUTC, otherTsStat.getMaximumUTC)
          case (t: ColumnStatisticsImpl, o: ColumnStatisticsImpl) => equalsForBasicStat(t, o)
          case (t@_, _@_) => throw new IllegalStateException("Unexpected type: " + t.getClass)
        }
      case _ => false
    }
  }

  case class StripeFooterStat(streams: Seq[StreamStat], encodings: Seq[String])

  case class StreamStat(columnIndex: Int, streamKind: String)

  val statTestSchema =
    """
    struct<
      c01 boolean,
      c02 byte,
      c03 short,
      c04 int,
      c05 long,
      c06 decimal,
      c07 float,
      c08 double,
      c09 string,
      c10 timestamp,
      c11 date,
      c12 struct<
        c12_01 timestamp,
        c12_02 decimal,
        c12_03 double
      >,
      c13 array<
        decimal
      >,
      c14 map<
        byte,
        float
      >,
      c15 array<
        map<
          struct<
            c15_01 date,
            c15_02 short
          >,
          struct<
            c15_01 date,
            c15_02 array<
              long
            >
          >
        >
      >
    >
    """
}
