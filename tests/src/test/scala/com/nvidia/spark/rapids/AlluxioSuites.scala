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

trait AlluxioSuite extends SparkQueryCompareTestSuite with Arm {

  /** File format */
  protected def format: String

  protected def otherConfs: Iterable[(String, String)] =
    Seq(("spark.rapids.alluxio.automount.enabled", "true"),
      ("spark.rapids.alluxio.bucket.regex", "^file://.*"))

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
  protected final def testAlluxioPaths(
      conf: SparkConf,
      files: Array[String],
      expectedReaderType: RapidsReaderType,
      algo: String,
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
            // not supported with Alluxio yet
            // checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
          case plan: GpuFileSourceScanExec =>
            if (algo == "CONVERT_TIME") {
              val allAllux = plan.selectedPartitions.forall { p =>
                p.files.forall(p => p.toString.startsWith("alluxio://"))
              }
              assert(allAllux, "All files should start with alluxio")
            } else {
             /*
              // depends on reader type
              plan.readerFactory match {
                case mf: MultiFilePartitionReaderFactoryBase =>
                  if (mf.useMultiThread(files)) {
                    // MULTITHREADED
                    val rdd = plan.
                    mf.createColumnarReader()
                  } else {
                    // COALESCING
                  }
                case _ => PERFILE
                             */
            }
            checkReaderType(plan.readerFactory, files, expectedReaderType)
            plan
        }
        assert(!plans.isEmpty, "File reader is not running on GPU")
      }, conf.setAll(otherConfs))
    }
  }

  // Alluxio only supports V1 datasource and Parquet format right now

  /*
  test("Use defaults Alluxio - coalescing reading at task time") {
    val testFile = Array("/tmp/xyz")
    Seq(format).foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testAlluxioPaths(conf, testFile, COALESCING, "TASK_TIME")
    })
  }
  */

  test("Use coalescing reading Alluxio convert time") {
    val testFile = Array("/tmp/xyz")
    Seq(format).foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testAlluxioPaths(conf, testFile, COALESCING, "CONVERT_TIME")
    })
  }

  /*
  test("Use multithreaded reading for cloud files") {
    assume(testContextOk)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Force coalescing reading for cloud files when setting COALESCING ") {
    assume(testContextOk)
    val testFile = Array("s3:/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, COALESCING)
    })
  }

  test("Force multithreaded reading for local files when setting MULTITHREADED") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "MULTITHREADED")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

  test("Use multithreaded reading for input expression even setting COALESCING") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED, hasInputExpression=true)
    })
  }

  test("Use multithreaded reading for ignoreCorruptFiles even setting COALESCING") {
    assume(testContextOk)
    val testFile = Array("/tmp/xyz")
    Seq(format, "").foreach(useV1Source => {
      val conf = new SparkConf()
        .set("spark.sql.sources.useV1SourceList", useV1Source)
        .set("spark.sql.files.ignoreCorruptFiles", "true")
        .set(s"spark.rapids.sql.format.${format}.reader.type", "COALESCING")
      testReaderType(conf, testFile, MULTITHREADED)
    })
  }

   */
}

class GpuParquetAlluxioSuites extends AlluxioSuite{
  override protected def format: String = "parquet"
}

/*
class GpuOrcAlluxioSuites extends ReaderTypeSuite {
  override protected def format: String = "orc"
}

class GpuAvroAlluxioSuites extends ReaderTypeSuite {
  override lazy val format: String = "avro"
}

 */
