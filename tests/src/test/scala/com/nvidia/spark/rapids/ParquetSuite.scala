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

package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.Files
import java.util.Objects

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil.fullyDelete
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.{Encoding, EncodingStats}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback
import org.apache.spark.sql.tests.datagen.BigDataGenConsts._
import org.apache.spark.sql.tests.datagen.DBGen
import org.apache.spark.sql.types.{DateType, TimestampType}

class ParquetSuite extends SparkQueryCompareTestSuite {

  private val sparkConf = new SparkConf()
      .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
  test("Statistics tests for Parquet files written by GPU") {
    assume(true, "Move to scale test")
    val rowsNum: Long = 1024L * 1024L

    val testDataPath = Files.createTempDirectory("spark-rapids-parquet-suite").toFile
    def writeScaleTestDataOnCpu(): Unit = {
      withCpuSparkSession(
        spark => {
          // define table
          val gen = DBGen()
          gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
          gen.setDefaultValueRange(DateType, minDateIntForOrc, maxDateIntForOrc)
          val tab = gen.addTable("tab", statTestSchema, rowsNum)
          tab("c03").setNullProbability(0.5)
          tab("c08").setNullProbability(0.5)
          val path = testDataPath.getAbsolutePath
          // write to a file on CPU
          tab.toDF(spark).coalesce(1).write.mode("overwrite").parquet(path)
        },
        sparkConf)
    }

    // write test data on CPU or GPU, then read the stats
    def getStats: SparkSession => ParquetStat = { spark =>
      withTempPath { writePath =>
        // Read from the testing Parquet file and then write to a Parquet file
        spark.read.parquet(testDataPath.getAbsolutePath).coalesce(1)
            .write.mode("overwrite").parquet(writePath.getAbsolutePath)

        // get Stats
        getStatsFromFile(writePath)
      }
    }

    try {
      // Write test data to a file on CPU
      writeScaleTestDataOnCpu()

      // write data and get stats on CPU
      val cpuStats = withCpuSparkSession(getStats, sparkConf)

      // write data and get stats on GPU
      ExecutionPlanCaptureCallback.startCapture()
      val gpuStats = withGpuSparkSession(getStats, sparkConf)
      val plan = ExecutionPlanCaptureCallback.getResultsWithTimeout()
      // Ensure the writing exec is running on GPU
      ExecutionPlanCaptureCallback.assertContains(plan(0), "GpuDataWritingCommandExec")

      // compare stats
      assertResult(cpuStats)(gpuStats)
    } finally {
      fullyDelete(testDataPath)
    }
  }

  /**
   * Find a parquet file in parquetDir and get the stats. It's similar to output of
   * `Parquet-cli meta file`. Parquet-cli:https://github
   * .com/apache/parquet-mr/tree/master/parquet-cli
   *
   * @param parquetDir parquet file directory
   * @return Parquet statistics
   */
  private def getStatsFromFile(parquetDir: File): ParquetStat = {
    val parquetFile = parquetDir.listFiles(f => f.getName.endsWith(".parquet"))(0)
    val p = new Path(parquetFile.getCanonicalPath)
    val footer = ParquetFileReader.readFooter(new Configuration(), p,
      ParquetMetadataConverter.NO_FILTER)
    val columnTypes = footer.getFileMetaData.getSchema.getColumns.asScala.toArray
        .map(c => c.toString)
    val groupStats = footer.getBlocks.asScala.toArray.map { g =>
      val rowCount = g.getRowCount
      val columnChunkStats = g.getColumns.asScala.toArray.map { col =>
        ColumnChunkStat(col.getPrimitiveType, col.getEncodings.asScala.toSet,
          col.getEncodingStats, col.getCodec)
      }
      RowGroupStat(rowCount, columnChunkStats)
    }

    ParquetStat(columnTypes, groupStats)
  }

  case class ParquetStat(
      schema: Seq[String],
      rowGroupStats: Seq[RowGroupStat])

  case class RowGroupStat(rowCount: Long, columnStats: Seq[ColumnChunkStat])

  case class ColumnChunkStat(primitiveType: PrimitiveType, encodings: Set[Encoding],
      encodingStats: EncodingStats, codecName: CompressionCodecName) {

    private def encodingStatsEquals(l: EncodingStats, r: EncodingStats): Boolean = {
      Objects.equals(l.getDictionaryEncodings, r.getDictionaryEncodings) &&
          Objects.equals(l.getDataEncodings, r.getDataEncodings) &&
          l.usesV2Pages() == r.usesV2Pages()
    }

    override def equals(obj: Any): Boolean = obj match {
      case _@ColumnChunkStat(primitiveType, encodings, encodingStats, codecName) =>
        this.primitiveType.equals(primitiveType) &&
            this.encodings.equals(encodings) &&
            // EncodingStats does not have equals method
            encodingStatsEquals(this.encodingStats, encodingStats) &&
            this.codecName.equals(codecName)
      case _ => false
    }
  }

  val statTestSchema =
    """
    struct<
      c01: boolean,
      c02: byte,
      c03: short,
      c04: int,
      c05: long,
      c06: decimal,
      c07: float,
      c08: double,
      c09: string,
      c10: timestamp,
      c11: date
    >
    """
}
