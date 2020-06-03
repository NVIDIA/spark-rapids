/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}

class FileSourceScanExecSuite extends SparkQueryCompareTestSuite {
  private def fileSourceCSV(
      tableName: String,
      resourceName: String,
      schema: StructType,
      options: Map[String, String] = Map.empty): SparkSession => DataFrame = {
    s: SparkSession => {
      s.sql(s"DROP TABLE IF EXISTS `$tableName`")
      val resource = TestResourceFinder.getResourcePath(resourceName)
      s.catalog.createTable(tableName, "csv", schema, options ++ Map("path" -> resource))
    }
  }

  private def fileSourceIntsCsv(
      options: Map[String, String] = Map.empty): SparkSession => DataFrame = {
    val schema = StructType(Array(
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)))
    fileSourceCSV("ints_csv", "ints.csv", schema, options)
  }

  private def fileSourceIntsHeaderCsv: SparkSession => DataFrame = {
    fileSourceIntsCsv(Map("header" -> "true"))
  }

  private def fileSourcePartitionedIntsCsv: SparkSession => DataFrame = {
    val schema = StructType(Array(
      StructField("partKey", IntegerType),
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)))
    fileSourceCSV("part_ints_csv", "partitioned-csv", schema)
  }

  private def fileSourceFloatsCsv: SparkSession => DataFrame = {
    val schema = StructType(Array(
      StructField("floats", FloatType, nullable = false),
      StructField("more_floats", FloatType, nullable = false)
    ))
    fileSourceCSV("floats_csv", "floats.csv", schema)
  }

  private def fileSourceParquet(filename: String): SparkSession => DataFrame = {
    val path = TestResourceFinder.getResourcePath(filename)
    s: SparkSession => s.sql(s"select * from parquet.`$path`")
  }

  private def fileSourceOrc(filename: String): SparkSession => DataFrame = {
    val path = TestResourceFinder.getResourcePath(filename)
    s: SparkSession => s.sql(s"select * from orc.`$path`")
  }

  private val csvSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "10")

  private val fileSplitsParquet = fileSourceParquet("file-splits.parquet")
  private val parquetSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "10000")

  private val fileSplitsOrc = frameFromOrc("file-splits.orc")
  private val orcSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "30000")

  testSparkResultsAreEqual("Test CSV", fileSourceIntsCsv()) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test CSV count", fileSourceIntsCsv())(frameCount)

  testSparkResultsAreEqual("Test CSV count with headers",
    fileSourceIntsHeaderCsv)(frameCount)

  testSparkResultsAreEqual("Test CSV splits", fileSourceIntsCsv(), conf=csvSplitsConf) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test CSV splits with header", fileSourceFloatsCsv,
    conf=csvSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test CSV splits with chunks", fileSourceFloatsCsv,
    conf= new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1")) {
    frame => frame.select(col("floats"))
  }

  testSparkResultsAreEqual("Test CSV count chunked", fileSourceIntsCsv(),
    conf= new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "1"))(frameCount)

  testSparkResultsAreEqual("Test partitioned CSV splits", fileSourcePartitionedIntsCsv,
    conf = csvSplitsConf) {
    frame =>
      frame.select(col("partKey"),
        col("ints_1"),
        col("ints_3"),
        col("ints_5"))
  }

  testSparkResultsAreEqual("File source scan parquet",
    fileSourceParquet("test.snappy.parquet")) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test Parquet file splitting", fileSplitsParquet,
    conf=parquetSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet with chunks", fileSplitsParquet,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "100")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet count", fileSplitsParquet,
    conf=parquetSplitsConf)(frameCount)

  testSparkResultsAreEqual("Test Parquet predicate push-down", fileSplitsParquet) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"),
      col("zip")).where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test Parquet splits predicate push-down", fileSplitsParquet,
    conf=parquetSplitsConf) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"),
      col("zip")).where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test partitioned Parquet",
    fileSourceParquet("partitioned-parquet")) {
    frame => frame.select(col("partKey"), col("ints_1"),
      col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test partitioned Parquet predicate push-down",
    fileSourceParquet("partitioned-parquet")) {
    frame => frame.select(col("partKey"), col("ints_1"),
      col("ints_3"), col("ints_5")).where(col("partKey") === 100)
  }

  testSparkResultsAreEqual("File source scan ORC",
    fileSourceOrc("test.snappy.orc")) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test ORC file splitting", fileSplitsOrc,
    conf=orcSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test ORC with chunks", fileSplitsOrc,
    conf = new SparkConf().set(RapidsConf.MAX_READER_BATCH_SIZE_ROWS.key, "2048")) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test ORC count", fileSplitsOrc,
    conf=orcSplitsConf)(frameCount)

  testSparkResultsAreEqual("Test ORC predicate push-down", fileSplitsOrc) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"),
      col("zip")).where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test ORC splits predicate push-down", fileSplitsOrc,
    conf=orcSplitsConf) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"),
      col("zip")).where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test partitioned ORC",
    fileSourceOrc("partitioned-orc")) {
    frame => frame.select(col("partKey"), col("ints_5"),
      col("ints_3"), col("ints_1"))
  }

  testSparkResultsAreEqual("Test partitioned ORC predicate push-down",
    fileSourceOrc("partitioned-orc")) {
    frame => frame.select(col("partKey"), col("ints_1"),
      col("ints_3"), col("ints_5")).where(col("partKey") === 100)
  }
}
