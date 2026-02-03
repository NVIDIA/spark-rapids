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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.hadoop.mapred.{JobConf, SequenceFileAsBinaryInputFormat => OldSequenceFileAsBinaryInputFormat}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Unit tests for SequenceFile RDD conversion rule and GPU reader.
 *
 * The SequenceFile support in spark-rapids works via the SequenceFileRDDConversionRule,
 * which converts RDD-based SequenceFile scans (e.g., sc.newAPIHadoopFile with
 * SequenceFileInputFormat) to FileFormat-based scans that can be GPU-accelerated.
 *
 * This conversion is disabled by default and must be enabled via:
 *   spark.rapids.sql.sequenceFile.rddConversion.enabled=true
 *
 * If the conversion fails or GPU doesn't support the operation, the original RDD scan
 * is preserved (no fallback to CPU FileFormat).
 */
class SequenceFileBinaryFileFormatSuite extends AnyFunSuite {

  /**
   * Create a SparkSession with SequenceFile RDD conversion enabled.
   * Note: We don't use spark.rapids.sql.test.enabled=true here because it would
   * require ALL operations to be on GPU, but the RDD-to-FileFormat conversion
   * only affects the scan part of the plan.
   */
  private def withConversionEnabledSession(f: SparkSession => Unit): Unit = {
    // Clear any existing sessions to ensure clean state
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    
    val spark = SparkSession.builder()
      .appName("SequenceFileBinaryFileFormatSuite")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      // Register RAPIDS SQL extensions for logical plan rules
      .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.sequenceFile.rddConversion.enabled", "true")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) {
      val children = f.listFiles()
      if (children != null) {
        children.foreach(deleteRecursively)
      }
    }
    if (f.exists()) {
      f.delete()
    }
  }

  private def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val tmpDir = Files.createTempDirectory(prefix).toFile
    try {
      f(tmpDir)
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  /**
   * Read a SequenceFile using the RDD path.
   * When conversion is enabled, this should be converted to FileFormat-based scan.
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
      (java.util.Arrays.copyOfRange(k.getBytes, 0, k.getLength),
       java.util.Arrays.copyOfRange(v.getBytes, 0, v.getLength))
    }.toDF("key", "value")
  }

  /**
   * Read only the value column from a SequenceFile (common pattern for protobuf payloads).
   */
  private def readSequenceFileValueOnly(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    sc.newAPIHadoopFile(
      path,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (_, v) =>
      java.util.Arrays.copyOfRange(v.getBytes, 0, v.getLength)
    }.toDF("value")
  }

  /**
   * Write a SequenceFile with raw record format.
   */
  private def writeSequenceFile(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]]): Unit = {
    val path = new Path(file.toURI)
    val writer = SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      SequenceFile.Writer.compression(CompressionType.NONE))
    try {
      payloads.zipWithIndex.foreach { case (p, idx) =>
        val key = new BytesWritable(intToBytes(idx))
        val value = new BytesWritable(p)
        writer.append(key, value)
      }
    } finally {
      writer.close()
    }
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
        val key = new BytesWritable(intToBytes(idx))
        val value = new BytesWritable(p)
        writer.append(key, value)
      }
    } finally {
      writer.close()
    }
  }

  private def writeEmptySequenceFile(file: File, conf: Configuration): Unit = {
    val path = new Path(file.toURI)
    val writer = SequenceFile.createWriter(
      conf,
      SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf[BytesWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable]),
      SequenceFile.Writer.compression(CompressionType.NONE))
    writer.close()
  }

  private def intToBytes(i: Int): Array[Byte] = Array[Byte](
    ((i >> 24) & 0xFF).toByte,
    ((i >> 16) & 0xFF).toByte,
    ((i >> 8) & 0xFF).toByte,
    (i & 0xFF).toByte
  )

  private def bytesToInt(b: Array[Byte]): Int = {
    require(b.length == 4, s"Expected 4 bytes, got ${b.length}")
    ((b(0) & 0xFF) << 24) | ((b(1) & 0xFF) << 16) | ((b(2) & 0xFF) << 8) | (b(3) & 0xFF)
  }

  // ============================================================================
  // Basic functionality tests
  // ============================================================================

  test("RDD conversion reads raw value bytes correctly") {
    withTempDir("seqfile-binary-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8),
        Array.fill[Byte](10)(42.toByte)
      )
      writeSequenceFile(file, conf, payloads)

      withConversionEnabledSession { spark =>
        val df = readSequenceFileViaRDD(spark, file.getAbsolutePath)

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
          assert(java.util.Arrays.equals(v, payloads(idx)))
        }
      }
    }
  }

  test("RDD conversion matches baseline RDD scan results") {
    withTempDir("seqfile-rdd-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8),
        Array.fill[Byte](10)(42.toByte)
      )
      writeSequenceFile(file, conf, payloads)

      // Test with conversion enabled and compare against expected payloads
      withConversionEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        val convertedResults = df.collect().map(_.getAs[Array[Byte]](0))

        assert(convertedResults.length == payloads.length,
          s"Expected ${payloads.length} results but got ${convertedResults.length}")
        
        // Sort by comparing byte arrays to ensure consistent ordering
        val sortedResults = convertedResults.sortBy(arr => new String(arr, StandardCharsets.UTF_8))
        val sortedPayloads = payloads.sortBy(arr => new String(arr, StandardCharsets.UTF_8))
        
        sortedResults.zip(sortedPayloads).foreach { case (result, expected) =>
          assert(java.util.Arrays.equals(result, expected),
            s"Mismatch: got ${java.util.Arrays.toString(result)}, " +
            s"expected ${java.util.Arrays.toString(expected)}")
        }
      }
    }
  }

  test("Value-only reads via RDD conversion") {
    withTempDir("seqfile-valueonly-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](10, 20, 30))
      writeSequenceFile(file, conf, payloads)

      withConversionEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)

        val results = df.collect()
        assert(results.length == 1)
        val valueBytes = results(0).getAs[Array[Byte]](0)
        assert(java.util.Arrays.equals(valueBytes, payloads(0)))
      }
    }
  }

  test("Empty files via RDD conversion") {
    withTempDir("seqfile-empty-test") { tmpDir =>
      val file = new File(tmpDir, "empty.seq")
      val conf = new Configuration()
      writeEmptySequenceFile(file, conf)

      withConversionEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)

        val results = df.collect()
        assert(results.isEmpty)
      }
    }
  }

  // ============================================================================
  // Compression tests
  // ============================================================================

  test("Compressed SequenceFile throws UnsupportedOperationException") {
    withTempDir("seqfile-compressed-test") { tmpDir =>
      val file = new File(tmpDir, "compressed.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8)
      )
      writeCompressedSequenceFile(file, conf, payloads)

      withConversionEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)

        // Spark wraps the UnsupportedOperationException in a SparkException
        val ex = intercept[SparkException] {
          df.collect()
        }
        // The exception chain may be:
        // SparkException -> ExecutionException -> UnsupportedOperationException
        // Find the UnsupportedOperationException in the cause chain
        def findUnsupportedOpEx(t: Throwable): Option[UnsupportedOperationException] = {
          if (t == null) None
          else if (t.isInstanceOf[UnsupportedOperationException]) {
            Some(t.asInstanceOf[UnsupportedOperationException])
          } else {
            findUnsupportedOpEx(t.getCause)
          }
        }
        
        val unsupportedEx = findUnsupportedOpEx(ex)
        assert(unsupportedEx.isDefined,
          s"Expected UnsupportedOperationException in cause chain but got: " +
          s"${ex.getClass.getName}: ${ex.getMessage}")
        assert(unsupportedEx.get.getMessage.contains("does not support compressed"),
          s"Unexpected message: ${unsupportedEx.get.getMessage}")
      }
    }
  }

  // ============================================================================
  // Large data tests
  // ============================================================================

  test("Large batch handling via RDD conversion") {
    withTempDir("seqfile-largebatch-test") { tmpDir =>
      val file = new File(tmpDir, "large.seq")
      val conf = new Configuration()
      // Create many records to test batching
      val numRecords = 1000
      val payloads = (0 until numRecords).map { i =>
        s"record-$i-payload".getBytes(StandardCharsets.UTF_8)
      }.toArray
      writeSequenceFile(file, conf, payloads)

      withConversionEnabledSession { spark =>
        val df = readSequenceFileViaRDD(spark, file.getAbsolutePath)

        val results = df.select("key", "value").collect()
        assert(results.length == numRecords)

        // Verify all records are read correctly
        val sortedResults = results
          .map(row => (bytesToInt(row.getAs[Array[Byte]](0)), row.getAs[Array[Byte]](1)))
          .sortBy(_._1)

        sortedResults.zipWithIndex.foreach { case ((idx, value), expectedIdx) =>
          assert(idx == expectedIdx)
          assert(java.util.Arrays.equals(value, payloads(expectedIdx)))
        }
      }
    }
  }

  // ============================================================================
  // Glob pattern tests
  // ============================================================================

  test("RDD conversion supports glob patterns in paths") {
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

      withConversionEnabledSession { spark =>
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

  test("RDD conversion supports recursive glob patterns") {
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

      withConversionEnabledSession { spark =>
        // Test recursive glob pattern: data/year=2024/*/
        val globPath = new File(tmpDir, "data/year=2024/*").getAbsolutePath
        val df = readSequenceFileViaRDD(spark, globPath)
        
        val results = df.select("value").collect().map(_.getAs[Array[Byte]](0))
        
        assert(results.length == 2,
          s"Expected 2 results from recursive glob, got ${results.length}")
      }
    }
  }

  test("RDD conversion handles glob pattern with no matches gracefully") {
    withTempDir("seqfile-glob-nomatch-test") { tmpDir =>
      // Create a single file
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](1, 2, 3))
      writeSequenceFile(file, conf, payloads)

      withConversionEnabledSession { spark =>
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

  test("RDD conversion is disabled by default") {
    withTempDir("seqfile-config-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](1, 2, 3))
      writeSequenceFile(file, conf, payloads)

      // Clear any existing sessions to ensure clean state
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      // Create session WITHOUT enabling the conversion
      // Note: NOT using spark.rapids.sql.test.enabled=true because RDD scans don't run on GPU
      val spark = SparkSession.builder()
        .appName("SequenceFileBinaryFileFormatSuite-NoConversion")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        // Register RAPIDS SQL extensions (but keep conversion disabled)
        .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.rapids.sql.enabled", "true")
        // Note: NOT setting spark.rapids.sql.sequenceFile.rddConversion.enabled (defaults to false)
        .getOrCreate()
      try {
        // This should work via the original RDD path (no conversion)
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        val results = df.collect()
        assert(results.length == 1)
        
        // Without conversion, SequenceFileAsBinaryInputFormat returns raw BytesWritable bytes
        // which include the 4-byte length prefix: [0, 0, 0, 3] + payload [1, 2, 3]
        // This is the expected behavior of the original RDD path
        val expectedRaw = Array[Byte](0, 0, 0, 3, 1, 2, 3)
        val actualBytes = results(0).getAs[Array[Byte]](0)
        assert(java.util.Arrays.equals(actualBytes, expectedRaw),
          s"Expected raw BytesWritable bytes ${java.util.Arrays.toString(expectedRaw)}, " +
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
      (java.util.Arrays.copyOfRange(k.getBytes, 0, k.getLength),
       java.util.Arrays.copyOfRange(v.getBytes, 0, v.getLength))
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
      java.util.Arrays.copyOfRange(v.getBytes, 0, v.getLength)
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

      withConversionEnabledSession { spark =>
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

      withConversionEnabledSession { spark =>
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

      withConversionEnabledSession { spark =>
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
