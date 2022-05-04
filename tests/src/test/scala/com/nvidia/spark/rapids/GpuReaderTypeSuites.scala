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

import com.nvidia.spark.rapids.RapidsReaderType._
import com.nvidia.spark.rapids.shims.GpuBatchScanExec

import org.apache.spark.SparkConf
import org.apache.spark.sql.FileUtils.withTempPath
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.rapids.{ExternalSource, GpuFileSourceScanExec}

trait ReaderTypeSuite extends SparkQueryCompareTestSuite with Arm {

  /** File format */
  protected def format: String

  protected def otherConfs: Iterable[(String, String)] = Seq.empty

  private def checkReaderType(
      readerFactory: PartitionReaderFactory,
      inputFile: Array[String],
      expectedReaderType: RapidsReaderType) = {
    val actualReaderType = readerFactory match {
      case mf: MultiFilePartitionReaderFactoryBase =>
        if (mf.useMultiThread(inputFile)) {
          MULTITHREADED
        } else {
          COALESCING
        }
      case _ => PERFILE
    }
    assert(expectedReaderType == actualReaderType,
      s", expected $expectedReaderType, but got $actualReaderType")
  }

  /**
   * Test if the given reader type will be used.
   *
   * @param conf SparkConf
   * @param files input files
   * @param multithreadedReadingExpected true: multithreaded reading, false: coalescing reading.
   * @param hasInputExpression if has input expression
   */
  protected final def testReaderType(
      conf: SparkConf,
      files: Array[String],
      expectedReaderType: RapidsReaderType,
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
            checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
          case plan: GpuFileSourceScanExec =>
            checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
        }
        assert(!plans.isEmpty, "File reader is not running on GPU")
      }, conf.setAll(otherConfs))
    }
  }
}

trait MultiReaderTypeSuite extends ReaderTypeSuite {

  test("Use coalescing reading for local files") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Use multithreaded reading for cloud files") {
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Force coalescing reading for cloud files when setting COALESCING ") {
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Force multithreaded reading for local files when setting MULTITHREADED") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "MULTITHREADED")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Use multithreaded reading for input expression even setting COALESCING") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED, hasInputExpression=true)
    })
  }

  test("Use multithreaded reading for ignoreCorruptFiles even setting COALESCING") {
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set("spark.sql.files.ignoreCorruptFiles", "true")
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }
}

class GpuParquetReaderTypeSuites extends MultiReaderTypeSuite {
  override protected def format: String = "parquet"
}

class GpuOrcReaderTypeSuites extends MultiReaderTypeSuite {
  override protected def format: String = "orc"
}

class GpuAvroReaderTypeSuites extends ReaderTypeSuite {
  override lazy val format: String = "avro"
  override lazy val otherConfs: Iterable[(String, String)] = Seq(
    ("spark.rapids.sql.format.avro.read.enabled", "true"),
    ("spark.rapids.sql.format.avro.enabled", "true"))

  private lazy val hasAvroJar = ExternalSource.hasSparkAvroJar

  test("Use coalescing reading for local files") {
    assume(hasAvroJar)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Use coalescing reading for cloud files if coalescing can work") {
    assume(hasAvroJar)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Force coalescing reading for cloud files when setting COALESCING ") {
    assume(hasAvroJar)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Use per-file reading for input expression even setting COALESCING") {
    assume(hasAvroJar)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, PERFILE, hasInputExpression=true)
    })
  }

  test("Use per-file reading for ignoreCorruptFiles even setting COALESCING") {
    assume(hasAvroJar)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set("spark.sql.files.ignoreCorruptFiles", "true")
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, PERFILE)
    })
  }
}
