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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.PrimitiveType

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.tests.datagen.{DBGen, TableGen}
import org.apache.spark.sql.tests.datagen.BigDataGenConsts.{maxTimestampForOrc, minTimestampForOrc}
import org.apache.spark.sql.types._

class ParquetScaleTestSuite extends SparkQueryCompareTestSuite with Logging {
  private val sparkConf = new SparkConf()
      .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") // for Spark 32x, 33x and ...
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") // for Spark 31x
      .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") // for Spark 32x, 33x and ...
      .set("spark.rapids.sql.explain", "ALL")

  // by default cuDF splits row groups by 1,000,000 rows, we expect one row group
  /**
   * Refer to cuDF parquet.hpp
   * default_row_group_size_bytes   = 128 * 1024 * 1024;  ///< 128MB per row group
   * default_row_group_size_rows = 1000000;     ///< 1 million rows per row group
   */
  private val rowsNum: Long = 1000L * 1000L

  case class ParquetStat(
      schema: Seq[String],
      rowGroupStats: Seq[RowGroupStat])

  case class RowGroupStat(rowCount: Long, columnStats: Seq[ColumnChunkStat])

  case class ColumnChunkStat(
      primitiveType: PrimitiveType,
      min: Comparable[_],
      max: Comparable[_],
      hasNonNullValue: Boolean,
      isNumNullsSet: Boolean,
      numNulls: Long
  )

  def writeScaleTestDataOnCpu(testDataPath: File, gen: SparkSession => DataFrame): Unit = {
    withCpuSparkSession(
      spark => {
        // define table
        val path = testDataPath.getAbsolutePath
        // write to a file on CPU
        gen(spark).coalesce(1).write.mode("overwrite").parquet(path)
      },
      sparkConf)
  }

  // write test data on CPU or GPU, then read the stats
  def getStats(filePath: File): SparkSession => ParquetStat = { spark =>
    withTempPath { writePath =>
      // Read from the testing Parquet file and then write to a Parquet file
      spark.read.parquet(filePath.getAbsolutePath).coalesce(1)
          .write.mode("overwrite").parquet(writePath.getAbsolutePath)

      // get Stats
      getStatsFromFile(writePath)
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
        ColumnChunkStat(
          col.getPrimitiveType,
          col.getStatistics.genericGetMin(),
          col.getStatistics.genericGetMax(),
          col.getStatistics.hasNonNullValue,
          col.getStatistics.isNumNullsSet,
          col.getStatistics.getNumNulls)
      }
      RowGroupStat(rowCount, columnChunkStats)
    }

    ParquetStat(columnTypes, groupStats)
  }

  private def checkStats(genDf: SparkSession => DataFrame): Unit = {
    withTempPath { testDataFile =>
      // Write test data to a file on CPU
      writeScaleTestDataOnCpu(testDataFile, genDf)

      // write data and get stats on CPU
      val cpuStats = withCpuSparkSession(getStats(testDataFile), sparkConf)
      val cpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // write data and get stats on GPU
      val gpuStats = withGpuSparkSession(getStats(testDataFile), sparkConf)
      val gpuFileSize = testDataFile.listFiles(f => f.getName.endsWith(".parquet"))(0).length()

      // compare stats
      assertResult(cpuStats)(gpuStats)

      // Check the Gpu file size is not too large.
      assert(gpuFileSize < 2 * cpuFileSize)
    }
  }

  private val basicTypes = Seq(
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    "boolean",
    "byte",
    "short",
    "int",
    "long",
    "decimal",
    "string",
    "date",
    "timestamp")

  test("Statistics tests for Parquet files written by GPU, float/double") {
    assume(false, "Blocked by https://github.com/rapidsai/cudf/issues/13948")
    assume(false, "Move to scale test")

    val schema = StructType(Seq(
      StructField("c01", FloatType),
      StructField("c02", DoubleType)
    ))
    // test 2 rows with NaN
    val data = Seq(Row(1.1f, Double.NaN), Row(Float.NaN, 2.2d))

    def genDf(schema: StructType, data: Seq[Row]): SparkSession => DataFrame = spark =>
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    checkStats(genDf(schema, data))

    // After https://github.com/rapidsai/cudf/issues/13948 is fixed
    // Change this test to the following code:
    //    val schemaStr =
    //      """
    //      struct<
    //        c01: float,
    //        c02: double
    //      >
    //      """
    //    val gen = DBGen()
    //    val tab = gen.addTable("tab", schemaStr, rowsNum)
    //    tab("c01").setNullProbability(0.5)
    //    checkStats(tab)
  }

  test("Statistics tests for Parquet files written by GPU, basic types") {
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c01: boolean,
        c02: byte,
        c03: short,
        c04: int,
        c05: long,
        c06: decimal,
        c07: string,
        c08: date,
        c09: timestamp
      >
      """
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c01").setNullProbability(0.5)
    tab("c06").setNullProbability(0.5)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array") {
    assume(true, "Move to scale test")
    val types = Seq(
      "array<boolean>",
      "array<byte>",
      "array<short>",
      "array<int>",
      "array<long>",
      "array<decimal>",
      "array<string>",
      "array<date>",
      "array<timestamp>")
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    types.foreach { t =>
      val schema = s"struct<c01: $t>"
      val nullProbabilities = Seq(0d, 0.5d)
      nullProbabilities.foreach { nullProbability =>
        try {
          val gen = DBGen()
          gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
          val tab = gen.addTable("tab", schema, rowsNum)
          tab("c01").setNullProbability(nullProbability)
          tab("c01").setLength(5) // avoid row group exceeds 128M, we expect one row group

          def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

          checkStats(genDf(tab))
        } catch {
          case e: Exception =>
            logError(s"check $schema, $nullProbability failed", e)
            throw e
        }
      }
    }
  }

  test("Statistics tests for Parquet files written by GPU, map") {
    assume(true, "Move to scale test")
    basicTypes.foreach { t =>
      val nullProbabilities = Seq(0d, 0.5d)
      nullProbabilities.foreach { nullProbability =>
        val schema = s"struct<c01: map<$t, $t>>"
        val gen = DBGen()
        gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
        val tab = gen.addTable("tab", schema, rowsNum)
        tab("c01").setNullProbability(nullProbability)
        tab("c01").setLength(3) // avoid row group exceeds 128M, we expect one row group

        def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

        checkStats(genDf(tab))
      }
    }
  }

  test("Statistics tests for Parquet files written by GPU, struct") {
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c1: struct<
          c01: boolean,
          c02: byte,
          c03: short,
          c04: int,
          c05: long,
          c06: decimal,
          c07: string,
          c08: date,
          c09: timestamp
        >
      >
      """
    // "float",  "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    // "double", "Blocked by https://github.com/rapidsai/cudf/issues/13948"
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.5)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, struct(array, map, struct)") {
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c1: struct<
          c01: array<string>
        >,
        c2: struct<
          c01: map<string, int>
        >,
        c3: struct<
          c301: struct<
            c30101: int,
            c30102: byte
          >
        >
      >
      """
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.9)
    tab("c2").setNullProbability(0.9)
    tab("c3").setNullProbability(0.9)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array(struct, array)") {
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c1: array<
          struct<
            c201: long,
            c202: int
          >
        >,
        c2: array<array<string>>
      >
      """
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c2").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c2").setLength(3)
    tab("c2")("child").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  test("Statistics tests for Parquet files written by GPU, array(map)") {
    // TODO Seems blocked by:
    assume(false, "https://github.com/rapidsai/cudf/issues/13664")
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c1: array<map<string, long>>
      >
      """
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
    tab("c1")("child").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }

  // TODO map(map) has an issue
  // TODO map(struct)
  test("Statistics tests for Parquet files written by GPU, map(array, map, struct)") {
    assume(true, "Move to scale test")
    val schema =
      """
      struct<
        c1: map<
          array<long>,
          array<string>
        >
      >
      """
    val gen = DBGen()
    gen.setDefaultValueRange(TimestampType, minTimestampForOrc, maxTimestampForOrc)
    val tab = gen.addTable("tab", schema, rowsNum)
    tab("c1").setNullProbability(0.8)
//    tab("c2").setNullProbability(0.8)
//    tab("c3").setNullProbability(0.8)
    tab("c1").setLength(3)
    tab("c1")("key").setLength(3)
    tab("c1")("value").setLength(3)
//    tab("c2").setLength(3)
//    tab("c2")("key").setLength(3)
//    tab("c2")("value").setLength(3)
//    tab("c3").setLength(3)

    def genDf(tab: TableGen): SparkSession => DataFrame = spark => tab.toDF(spark)

    checkStats(genDf(tab))
  }
}
