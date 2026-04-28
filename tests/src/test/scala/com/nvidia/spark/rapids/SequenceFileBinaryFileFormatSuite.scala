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

package com.nvidia.spark.rapids

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, SequenceFileAsBinaryInputFormat => OldSequenceFileAsBinaryInputFormat}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.RDDScanExec

/**
 * Unit tests for SequenceFile RDD read behavior with RAPIDS plugin enabled.
 *
 * RDD scans are preserved as-is (no logical-plan rewrite to FileFormat scan).
 */
class SequenceFileBinaryFileFormatSuite extends AnyFunSuite {

  private def withRapidsSession(f: SparkSession => Unit): Unit = {
    SequenceFileTestUtils.withSequenceFileSession("SequenceFileBinaryFileFormatSuite")(f)
  }

  private def withPhysicalReplaceEnabledSession(f: SparkSession => Unit): Unit = {
    SequenceFileTestUtils.withSequenceFileSession(
      "SequenceFileBinaryFileFormatSuite-PhysicalReplace", physicalReplaceEnabled = true)(f)
  }

  private def hasGpuSequenceFileRDDScan(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect {
      case p if p.getClass.getSimpleName == "GpuSequenceFileSerializeFromObjectExec" => 1
    }.nonEmpty
  }

  private def hasCpuRDDScan(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect {
      case _: RDDScanExec => 1
      case p if p.getClass.getSimpleName == "ExternalRDDScanExec" => 1
    }.nonEmpty
  }

  private def withTempDir(prefix: String)(f: File => Unit): Unit = {
    SequenceFileTestUtils.withTempDir(prefix)(f)
  }

  /**
   * Read a SequenceFile using the RDD path.
   */
  private def readSequenceFileViaRDD(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    sc.newAPIHadoopFile(
      path,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (k, v) =>
      (SequenceFileTestUtils.bytesWritablePayload(k.getBytes, k.getLength),
       SequenceFileTestUtils.bytesWritablePayload(v.getBytes, v.getLength))
    }.toDF("key", "value")
  }

  /**
   * Read only the value column from a SequenceFile (common pattern for protobuf payloads).
   */
  private def readSequenceFileValueOnly(spark: SparkSession, path: String): DataFrame = {
    SequenceFileTestUtils.readSequenceFileValueOnly(spark, path)
  }

  /**
   * Read a SequenceFile via RDD and intentionally swap output names relative to tuple order.
   */
  private def readSequenceFileViaRDDWithSwappedNames(
      spark: SparkSession,
      path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    sc.newAPIHadoopFile(
      path,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (k, v) =>
      (SequenceFileTestUtils.bytesWritablePayload(k.getBytes, k.getLength),
       SequenceFileTestUtils.bytesWritablePayload(v.getBytes, v.getLength))
    }.toDF("value", "key")
  }

  /**
   * Write a SequenceFile with raw record format.
   */
  private def writeSequenceFile(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]]): Unit = {
    SequenceFileTestUtils.writeSequenceFile(file, conf, payloads)
  }

  private def writeCompressedSequenceFile(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]]): Unit = {
    val path = new Path(file.toURI)
    val writer = SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      SequenceFile.Writer.compression(CompressionType.RECORD, new DefaultCodec()))
    try {
      payloads.zipWithIndex.foreach { case (p, idx) =>
        val key = new BytesWritable(SequenceFileTestUtils.intToBytes(idx))
        val value = new BytesWritable(p)
        writer.append(key, value)
      }
    } finally {
      writer.close()
    }
  }

  private def bytesToInt(b: Array[Byte]): Int = {
    require(b.length == 4, s"Expected 4 bytes, got ${b.length}")
    ((b(0) & 0xFF) << 24) | ((b(1) & 0xFF) << 16) | ((b(2) & 0xFF) << 8) | (b(3) & 0xFF)
  }

  // ============================================================================
  // Compression tests
  // ============================================================================

  test("Compressed SequenceFile is readable via preserved RDD scan") {
    withTempDir("seqfile-compressed-test") { tmpDir =>
      val file = new File(tmpDir, "compressed.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8)
      )
      writeCompressedSequenceFile(file, conf, payloads)

      withRapidsSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        val got = df.collect().map(_.getAs[Array[Byte]](0)).sortBy(_.length)
        val expected = payloads.sortBy(_.length)
        assert(got.length == expected.length)
        got.zip(expected).foreach { case (actual, exp) =>
          assert(java.util.Arrays.equals(actual, exp),
            s"Expected ${java.util.Arrays.toString(exp)}, got ${java.util.Arrays.toString(actual)}")
        }
      }
    }
  }

  test("Corrupt SequenceFile header is handled gracefully") {
    withTempDir("seqfile-corrupt-test") { tmpDir =>
      val goodFile = new File(tmpDir, "good.seq")
      val corruptFile = new File(tmpDir, "corrupt.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8)
      )
      writeSequenceFile(goodFile, conf, payloads)

      val fos = new FileOutputStream(corruptFile)
      try {
        fos.write(Array[Byte](0x01, 0x02, 0x03, 0x04))
      } finally {
        fos.close()
      }

      withRapidsSession { spark =>
        val path = s"${goodFile.getAbsolutePath},${corruptFile.getAbsolutePath}"
        val thrown = intercept[Exception] {
          readSequenceFileValueOnly(spark, path).collect()
        }
        assert(thrown.getMessage != null)
      }
    }
  }

  test("Swapped key/value output names still preserve query semantics") {
    withTempDir("seqfile-physical-swapped-output-test") { tmpDir =>
      val file = new File(tmpDir, "swapped.seq")
      val conf = new Configuration()
      val payloads = Array(
        Array[Byte](1, 2, 3),
        "swapped".getBytes(StandardCharsets.UTF_8))
      writeSequenceFile(file, conf, payloads)

      withPhysicalReplaceEnabledSession { spark =>
        val df = readSequenceFileViaRDDWithSwappedNames(spark, file.getAbsolutePath)

        val got = df.collect().map { row =>
          (row.getAs[Array[Byte]]("value"), row.getAs[Array[Byte]]("key"))
        }.sortBy { case (value, _) => bytesToInt(value) }

        val expected = payloads.zipWithIndex.map { case (value, idx) =>
          (SequenceFileTestUtils.intToBytes(idx), value)
        }

        assert(got.length == expected.length)
        got.zip(expected).foreach { case ((actualValue, actualKey), (expectedKey, expectedValue)) =>
          assert(java.util.Arrays.equals(actualValue, expectedKey))
          assert(java.util.Arrays.equals(actualKey, expectedValue))
        }
      }
    }
  }

  test("Physical replacement supports key-only reads via parent Project rename") {
    // For single-element RDD[Array[Byte]], Spark's encoder names the column "value".
    // .toDF("key") adds a ProjectExec renaming "value" → "key". The GPU replacement
    // resolves the effective column name "key" from the parent Project and correctly
    // reads the SequenceFile key column.
    withTempDir("seqfile-physical-keyonly-test") { tmpDir =>
      val file = new File(tmpDir, "keyonly.seq")
      val conf = new Configuration()
      val payloads = Array(
        Array[Byte](1, 2, 3),
        "keyonly".getBytes(StandardCharsets.UTF_8))
      writeSequenceFile(file, conf, payloads)

      withPhysicalReplaceEnabledSession { spark =>
        import spark.implicits._
        val sc = spark.sparkContext
        val df = sc.newAPIHadoopFile(
          file.getAbsolutePath,
          classOf[SequenceFileAsBinaryInputFormat],
          classOf[BytesWritable],
          classOf[BytesWritable]
        ).map { case (k, _) =>
          SequenceFileTestUtils.bytesWritablePayload(k.getBytes, k.getLength)
        }.toDF("key")

        assert(hasGpuSequenceFileRDDScan(df),
          s"Expected GPU SequenceFile exec in plan:\n${df.queryExecution.executedPlan}")
        val got = df.collect().map(_.getAs[Array[Byte]](0)).sortBy(b => bytesToInt(b))
        assert(got.length == payloads.length)
        // Keys are 4-byte big-endian integers written by writeSequenceFile
        assert(bytesToInt(got(0)) == 0)
        assert(bytesToInt(got(1)) == 1)
      }
    }
  }

  test("Physical replacement falls back to CPU for compressed SequenceFile") {
    withTempDir("seqfile-physical-compressed-fallback-test") { tmpDir =>
      val file = new File(tmpDir, "compressed.seq")
      val conf = new Configuration()
      val payloads = Array(
        Array[Byte](8, 9, 10),
        "compressed".getBytes(StandardCharsets.UTF_8))
      writeCompressedSequenceFile(file, conf, payloads)

      withPhysicalReplaceEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        assert(!hasGpuSequenceFileRDDScan(df),
          s"Compressed input should not use SequenceFile GPU physical replacement exec:\n" +
            s"${df.queryExecution.executedPlan}")
        assert(hasCpuRDDScan(df), "Compressed input should remain on CPU scan path")
        val got = df.collect().map(_.getAs[Array[Byte]](0)).sortBy(_.length)
        val expected = payloads.sortBy(_.length)
        assert(got.length == expected.length)
        got.zip(expected).foreach { case (actual, exp) =>
          assert(java.util.Arrays.equals(actual, exp))
        }
      }
    }
  }

  test("Physical replacement falls back to CPU for complex lineage") {
    withTempDir("seqfile-physical-complex-lineage-test") { tmpDir =>
      val file = new File(tmpDir, "complex.seq")
      val conf = new Configuration()
      val payloads = Array(
        "a".getBytes(StandardCharsets.UTF_8),
        "bb".getBytes(StandardCharsets.UTF_8))
      writeSequenceFile(file, conf, payloads)

      withPhysicalReplaceEnabledSession { spark =>
        import spark.implicits._
        val sc = spark.sparkContext
        val df = sc.newAPIHadoopFile(
          file.getAbsolutePath,
          classOf[SequenceFileAsBinaryInputFormat],
          classOf[BytesWritable],
          classOf[BytesWritable]
        ).map { case (_, v) =>
          SequenceFileTestUtils.bytesWritablePayload(v.getBytes, v.getLength)
        }.filter(_.length > 0)
          .union(sc.parallelize(Seq(Array[Byte](7, 7, 7)), 1))
          .filter(v => !(v.length == 3 && v(0) == 7.toByte && v(1) == 7.toByte && v(2) == 7.toByte))
          .toDF("value")

        assert(!hasGpuSequenceFileRDDScan(df),
          s"Complex lineage should remain on CPU:\n${df.queryExecution.executedPlan}")
        assert(hasCpuRDDScan(df))
        assert(df.collect().length == payloads.length)
      }
    }
  }

  test("SequenceFile PERFILE reader type is rejected") {
    withTempDir("seqfile-perfile-reader-test") { tmpDir =>
      val file = new File(tmpDir, "perfile.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](1, 2, 3))
      writeSequenceFile(file, conf, payloads)

      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      val spark = SparkSession.builder()
        .appName("SequenceFileBinaryFileFormatSuite-PERFILE")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.sql.enabled", "true")
        .config("spark.rapids.sql.format.sequencefile.reader.type", "PERFILE")
        .config("spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled", "true")
        .getOrCreate()
      try {
        val e = intercept[IllegalArgumentException] {
          readSequenceFileValueOnly(spark, file.getAbsolutePath).collect()
        }
        assert(e.getMessage.contains("PERFILE reader type is not supported for SequenceFile"))
      } finally {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  // ============================================================================
  // Glob pattern tests
  // ============================================================================

  test("RDD path supports glob patterns in paths") {
    withTempDir("seqfile-glob-test") { tmpDir =>
      // Create subdirectories with data files
      val subDir1 = new File(tmpDir, "2024/01")
      val subDir2 = new File(tmpDir, "2024/02")
      val subDir3 = new File(tmpDir, "2025/01")
      subDir1.mkdirs()
      subDir2.mkdirs()
      subDir3.mkdirs()

      val conf = new Configuration()
      
      // Write different payloads to each subdirectory
      val payloads1 = Array(Array[Byte](1, 1, 1))
      val payloads2 = Array(Array[Byte](2, 2, 2))
      val payloads3 = Array(Array[Byte](3, 3, 3))
      
      writeSequenceFile(new File(subDir1, "part-00000.seq"), conf, payloads1)
      writeSequenceFile(new File(subDir2, "part-00000.seq"), conf, payloads2)
      writeSequenceFile(new File(subDir3, "part-00000.seq"), conf, payloads3)

      withRapidsSession { spark =>
        // Test glob pattern that matches subdirectories: 2024/*
        val globPath = new File(tmpDir, "2024/*").getAbsolutePath
        val df = readSequenceFileViaRDD(spark, globPath)
        
        val results = df.select("value").collect().map(_.getAs[Array[Byte]](0))
        
        // Should find files in 2024/01 and 2024/02 (2 files total)
        assert(results.length == 2, 
          s"Expected 2 results from glob pattern '2024/*', got ${results.length}")
        
        // Verify we got the exact expected payloads
        val sortedResults = results.sortBy(_(0))
        assert(java.util.Arrays.equals(sortedResults(0), payloads1(0)),
          s"First result should be [1,1,1], got ${sortedResults(0).toSeq}")
        assert(java.util.Arrays.equals(sortedResults(1), payloads2(0)),
          s"Second result should be [2,2,2], got ${sortedResults(1).toSeq}")
      }
    }
  }

  test("RDD path supports recursive glob patterns") {
    withTempDir("seqfile-recursive-glob-test") { tmpDir =>
      // Create nested directory structure
      val subDir1 = new File(tmpDir, "data/year=2024/month=01")
      val subDir2 = new File(tmpDir, "data/year=2024/month=02")
      subDir1.mkdirs()
      subDir2.mkdirs()

      val conf = new Configuration()
      
      val payloads1 = Array(Array[Byte](10, 20, 30))
      val payloads2 = Array(Array[Byte](40, 50, 60))
      
      writeSequenceFile(new File(subDir1, "data.seq"), conf, payloads1)
      writeSequenceFile(new File(subDir2, "data.seq"), conf, payloads2)

      withRapidsSession { spark =>
        // Test recursive glob pattern: data/year=2024/*/
        val globPath = new File(tmpDir, "data/year=2024/*").getAbsolutePath
        val df = readSequenceFileViaRDD(spark, globPath)
        
        val results = df.select("value").collect().map(_.getAs[Array[Byte]](0))
        
        assert(results.length == 2,
          s"Expected 2 results from recursive glob, got ${results.length}")
      }
    }
  }

  test("RDD path handles glob pattern with no matches gracefully") {
    withTempDir("seqfile-glob-nomatch-test") { tmpDir =>
      // Create a single file
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](1, 2, 3))
      writeSequenceFile(file, conf, payloads)

      withRapidsSession { spark =>
        // Use a glob pattern that matches nothing
        val globPath = new File(tmpDir, "nonexistent/*").getAbsolutePath
        
        // Hadoop's newAPIHadoopFile throws InvalidInputException when glob matches 0 files.
        // This happens at RDD creation time, before our conversion rule can do anything.
        // We verify that this expected Hadoop behavior occurs.
        val exception = intercept[org.apache.hadoop.mapreduce.lib.input.InvalidInputException] {
          val df = readSequenceFileViaRDD(spark, globPath)
          df.collect() // Force evaluation
        }
        
        assert(exception.getMessage.contains("matches 0 files"),
          s"Expected 'matches 0 files' error, got: ${exception.getMessage}")
      }
    }
  }

  // ============================================================================
  // Configuration tests
  // ============================================================================

  test("RDD SequenceFile path works without conversion config") {
    withTempDir("seqfile-config-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](1, 2, 3))
      writeSequenceFile(file, conf, payloads)

      // Clear any existing sessions to ensure clean state
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      // Create session without any SequenceFile conversion config.
      val spark = SparkSession.builder()
        .appName("SequenceFileBinaryFileFormatSuite-NoConversion")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        // Register RAPIDS SQL extensions.
        .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.sql.enabled", "true")
        .getOrCreate()
      try {
        // This should work via the original RDD path.
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        val results = df.collect()
        assert(results.length == 1)

        val expectedPayload = Array[Byte](1, 2, 3)
        val actualBytes = results(0).getAs[Array[Byte]](0)
        assert(java.util.Arrays.equals(actualBytes, expectedPayload),
          s"Expected payload bytes ${java.util.Arrays.toString(expectedPayload)}, " +
          s"but got ${java.util.Arrays.toString(actualBytes)}")
      } finally {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  // ============================================================================
  // Old API (hadoopRDD) tests
  // ============================================================================

  /**
   * Read a SequenceFile using the old Hadoop API (hadoopRDD).
   * This tests support for org.apache.hadoop.mapred.SequenceFileAsBinaryInputFormat.
   */
  private def readSequenceFileViaOldApi(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val jobConf = new JobConf(sc.hadoopConfiguration)
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, path)
    
    sc.hadoopRDD(
      jobConf,
      classOf[OldSequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (k, v) =>
      (SequenceFileTestUtils.bytesWritablePayload(k.getBytes, k.getLength),
       SequenceFileTestUtils.bytesWritablePayload(v.getBytes, v.getLength))
    }.toDF("key", "value")
  }

  /**
   * Read only the value column using old API hadoopRDD.
   * This tests the pattern: rdd.map(...).toDF("value")
   */
  private def readSequenceFileValueOnlyViaOldApi(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    val jobConf = new JobConf(sc.hadoopConfiguration)
    org.apache.hadoop.mapred.FileInputFormat.setInputPaths(jobConf, path)
    
    sc.hadoopRDD(
      jobConf,
      classOf[OldSequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (_, v) =>
      SequenceFileTestUtils.bytesWritablePayload(v.getBytes, v.getLength)
    }.toDF("value")
  }

  test("Old API hadoopRDD conversion reads key-value correctly") {
    withTempDir("seqfile-oldapi-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8),
        Array.fill[Byte](10)(42.toByte)
      )
      writeSequenceFile(file, conf, payloads)

      withRapidsSession { spark =>
        val df = readSequenceFileViaOldApi(spark, file.getAbsolutePath)

        val got = df.select("key", "value")
          .collect()
          .map { row =>
            val k = row.getAs[Array[Byte]](0)
            val v = row.getAs[Array[Byte]](1)
            (bytesToInt(k), v)
          }
          .sortBy(_._1)

        assert(got.length == payloads.length)
        got.foreach { case (idx, v) =>
          assert(java.util.Arrays.equals(v, payloads(idx)),
            s"Payload mismatch at index $idx: got ${java.util.Arrays.toString(v)}")
        }
      }
    }
  }

  test("Old API hadoopRDD value-only conversion via toDF(\"value\")") {
    withTempDir("seqfile-oldapi-valueonly-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](10, 20, 30),
        Array[Byte](40, 50, 60)
      )
      writeSequenceFile(file, conf, payloads)

      withRapidsSession { spark =>
        val df = readSequenceFileValueOnlyViaOldApi(spark, file.getAbsolutePath)

        // Verify the schema only has "value" column
        assert(df.schema.fieldNames.toSeq == Seq("value"),
          s"Expected schema with only 'value' column, got: ${df.schema.fieldNames.mkString(", ")}")

        val results = df.collect().map(_.getAs[Array[Byte]](0))
        assert(results.length == payloads.length)

        // Sort results to ensure consistent comparison
        val sortedResults = results.sortBy(_(0))
        val sortedPayloads = payloads.sortBy(_(0))
        
        sortedResults.zip(sortedPayloads).zipWithIndex.foreach { case ((result, expected), idx) =>
          assert(java.util.Arrays.equals(result, expected),
            s"Mismatch at index $idx: got ${java.util.Arrays.toString(result)}, " +
            s"expected ${java.util.Arrays.toString(expected)}")
        }
      }
    }
  }

  test("Old API hadoopRDD with glob patterns") {
    withTempDir("seqfile-oldapi-glob-test") { tmpDir =>
      // Create subdirectories with data files
      val subDir1 = new File(tmpDir, "part1")
      val subDir2 = new File(tmpDir, "part2")
      subDir1.mkdirs()
      subDir2.mkdirs()

      val conf = new Configuration()
      
      val payloads1 = Array(Array[Byte](1, 1, 1))
      val payloads2 = Array(Array[Byte](2, 2, 2))
      
      writeSequenceFile(new File(subDir1, "data.seq"), conf, payloads1)
      writeSequenceFile(new File(subDir2, "data.seq"), conf, payloads2)

      withRapidsSession { spark =>
        // Test glob pattern: part*
        val globPath = new File(tmpDir, "part*").getAbsolutePath
        val df = readSequenceFileViaOldApi(spark, globPath)
        
        val results = df.select("value").collect().map(_.getAs[Array[Byte]](0))
        
        assert(results.length == 2, 
          s"Expected 2 results from glob pattern 'part*', got ${results.length}")
        
        val sortedResults = results.sortBy(_(0))
        assert(java.util.Arrays.equals(sortedResults(0), payloads1(0)),
          s"First result should be [1,1,1], got ${sortedResults(0).toSeq}")
        assert(java.util.Arrays.equals(sortedResults(1), payloads2(0)),
          s"Second result should be [2,2,2], got ${sortedResults(1).toSeq}")
      }
    }
  }
}
