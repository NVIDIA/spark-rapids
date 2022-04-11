/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.FileUtils.withTempPath
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.rapids.GpuFileSourceScanExec

trait FileSourceSuite extends SparkQueryCompareTestSuite with Arm {

  /** File format */
  protected def format: String

  /** Check if use multithreaded reading */
  private def checkMultiThreadedReading(
      readerFactory: PartitionReaderFactory,
      inputFile: Array[String],
      expected: Boolean) = {
    readerFactory match {
      case factory: MultiFilePartitionReaderFactoryBase =>
        assert(factory.useMultiThread(inputFile) == expected)
      case _ => assert(false, "PERFILE is Use")
    }
  }

  /**
   * Test if multithreaded reading is Use.
   *
   * @param conf SparkConf
   * @param files input files
   * @param multithreadedReadingExpected true: multithreaded reading, false: coalescing reading.
   * @param hasInputExpression if has input expression
   */
  private def testMultiThreadedReader(
      conf: SparkConf,
      files: Array[String],
      multithreadedReadingExpected: Boolean,
      hasInputExpression: Boolean = false): Unit = {
    withTempPath { file =>
      withCpuSparkSession(spark => {
        import spark.implicits._
        Seq(1).toDF("a").write.format(format).save(file.getCanonicalPath)
      })

      withGpuSparkSession(spark => {
        val rawDf = spark.read.format(format).load(file.toString)
        val df = if (hasInputExpression) rawDf.withColumn("input", input_file_name()) else rawDf
        val plans = df.queryExecution.executedPlan.collect {
          case plan: GpuBatchScanExec =>
            checkMultiThreadedReading(plan.readerFactory, files, multithreadedReadingExpected)
            plan
          case plan: GpuFileSourceScanExec =>
            checkMultiThreadedReading(plan.readerFactory, files, multithreadedReadingExpected)
            plan
        }
        assert(!plans.isEmpty, "File reader is not running on GPU")
      }, conf)
    }
  }

  test("Use coalescing reading for local files") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testMultiThreadedReader(conf, testFile, false)
    })
  }

  test("Use multithreaded reading for cloud files") {
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testMultiThreadedReader(conf, testFile, true)
    })
  }

  test("Force coalescing reading for cloud files when setting COALESCING ") {
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testMultiThreadedReader(conf, testFile, false)
    })
  }

  test("Force multithreaded reading for local files when setting MULTITHREADED") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "MULTITHREADED")
      testMultiThreadedReader(conf, testFile, true)
    })
  }

  test("Use multithreaded reading for input expression even setting COALESCING") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testMultiThreadedReader(conf, testFile, true, true)
    })
  }

  test("Use multithreaded reading for ignoreCorruptFiles even setting COALESCING") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set("spark.sql.files.ignoreCorruptFiles", "true")
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testMultiThreadedReader(conf, testFile, true )
    })
  }
}

class ParquetSuites extends FileSourceSuite {
  override protected def format: String = "parquet"
}

class OrcSuites extends FileSourceSuite {
  override protected def format: String = "orc"
}
