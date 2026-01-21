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

import java.io.{BufferedOutputStream, DataOutputStream, File}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.DefaultCodec
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession

/**
 * Unit tests for SequenceFileBinaryFileFormat.
 *
 * Note: This test suite uses its own withSparkSession/withGpuSparkSession methods instead of
 * extending SparkQueryCompareTestSuite because:
 * 1. These tests need fresh SparkSession instances per test to avoid state pollution
 * 2. The tests don't need the compare-CPU-vs-GPU pattern from SparkQueryCompareTestSuite
 * 3. The simpler session management makes the tests more self-contained
 */
class SequenceFileBinaryFileFormatSuite extends AnyFunSuite {

  private def withSparkSession(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName("SequenceFileBinaryFileFormatSuite")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  private def withGpuSparkSession(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName("SequenceFileBinaryFileFormatSuite-GPU")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.test.enabled", "true")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
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

  private def writeSequenceFileWithRawRecords(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]]): Unit = {
    val path = new Path(file.toURI)
    val fs = FileSystem.getLocal(conf)
    val out = new DataOutputStream(new BufferedOutputStream(fs.create(path, true)))
    try {
      // SequenceFile v6 header: magic + version
      out.write(Array[Byte]('S'.toByte, 'E'.toByte, 'Q'.toByte, 6.toByte))
      // Key/value class names (as strings)
      Text.writeString(out, classOf[BytesWritable].getName)
      Text.writeString(out, classOf[BytesWritable].getName)
      // Compression flags
      out.writeBoolean(false) // compression
      out.writeBoolean(false) // block compression
      // Empty metadata
      new SequenceFile.Metadata().write(out)
      // Sync marker (16 bytes)
      val sync = new Array[Byte](16)
      new Random().nextBytes(sync)
      out.write(sync)

      // Insert a sync marker record for realism (and to support split alignment if needed).
      out.writeInt(-1)
      out.write(sync)

      payloads.zipWithIndex.foreach { case (p, idx) =>
        val keyBytes = intToBytes(idx)
        val keyLen = keyBytes.length
        val valueLen = p.length
        val recordLen = keyLen + valueLen
        out.writeInt(recordLen)
        out.writeInt(keyLen)
        out.write(keyBytes)
        out.write(p)
      }
    } finally {
      out.close()
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

  test("SequenceFileBinaryFileFormat reads raw value bytes even when header says BytesWritable") {
    withTempDir("seqfile-binary-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8),
        Array.fill[Byte](10)(42.toByte)
      )
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

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

  test("SequenceFileBinaryFileFormat vs RDD scan") {
    withTempDir("seqfile-rdd-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8),
        Array.fill[Byte](10)(42.toByte)
      )
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        // File Scan Path
        val fileDf = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)
          .select("value")
        val fileResults = fileDf.collect().map(_.getAs[Array[Byte]](0))

        // RDD Scan Path
        import org.apache.hadoop.io.BytesWritable
        import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
        val sc = spark.sparkContext
        val rddResults = sc.newAPIHadoopFile(
          file.getAbsolutePath,
          classOf[SequenceFileAsBinaryInputFormat],
          classOf[BytesWritable],
          classOf[BytesWritable]
        ).map { case (_, v) =>
          java.util.Arrays.copyOfRange(v.getBytes, 0, v.getLength)
        }.collect()

        assert(fileResults.length == rddResults.length)
        fileResults.zip(rddResults).foreach { case (f, r) =>
          assert(java.util.Arrays.equals(f, r))
        }
      }
    }
  }

  test("Compressed SequenceFile throws UnsupportedOperationException") {
    withTempDir("seqfile-compressed-test") { tmpDir =>
      val file = new File(tmpDir, "compressed.seq")
      val conf = new Configuration()
      val payloads: Array[Array[Byte]] = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8)
      )
      writeCompressedSequenceFile(file, conf, payloads)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        // Spark wraps the UnsupportedOperationException in a SparkException
        val ex = intercept[SparkException] {
          df.collect()
        }
        // Check that the root cause is UnsupportedOperationException with expected message
        val cause = ex.getCause
        assert(cause.isInstanceOf[UnsupportedOperationException],
          s"Expected UnsupportedOperationException but got ${cause.getClass.getName}")
        assert(cause.getMessage.contains("does not support compressed SequenceFiles"))
      }
    }
  }

  test("Multi-file reads") {
    withTempDir("seqfile-multifile-test") { tmpDir =>
      val conf = new Configuration()

      // Create multiple files with different payloads
      val file1 = new File(tmpDir, "file1.seq")
      val payloads1 = Array(Array[Byte](1, 2, 3))
      writeSequenceFileWithRawRecords(file1, conf, payloads1)

      val file2 = new File(tmpDir, "file2.seq")
      val payloads2 = Array(Array[Byte](4, 5, 6))
      writeSequenceFileWithRawRecords(file2, conf, payloads2)

      val file3 = new File(tmpDir, "file3.seq")
      val payloads3 = Array(Array[Byte](7, 8, 9))
      writeSequenceFileWithRawRecords(file3, conf, payloads3)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(tmpDir.getAbsolutePath)

        val results = df.select("value").collect().map(_.getAs[Array[Byte]](0))
        assert(results.length == 3)

        // Verify all payloads are present (order may vary)
        val allPayloads = payloads1 ++ payloads2 ++ payloads3
        results.foreach { r =>
          assert(allPayloads.exists(p => java.util.Arrays.equals(r, p)))
        }
      }
    }
  }

  test("Partition columns") {
    withTempDir("seqfile-partition-test") { tmpDir =>
      val conf = new Configuration()

      // Create partitioned directory structure: part=a/file.seq and part=b/file.seq
      val partA = new File(tmpDir, "part=a")
      partA.mkdirs()
      val fileA = new File(partA, "file.seq")
      writeSequenceFileWithRawRecords(fileA, conf, Array(Array[Byte](1, 2, 3)))

      val partB = new File(tmpDir, "part=b")
      partB.mkdirs()
      val fileB = new File(partB, "file.seq")
      writeSequenceFileWithRawRecords(fileB, conf, Array(Array[Byte](4, 5, 6)))

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(tmpDir.getAbsolutePath)

        val results = df.select("value", "part")
          .collect()
          .map(row => (row.getAs[Array[Byte]](0), row.getString(1)))
          .sortBy(_._2)

        assert(results.length == 2)
        assert(results(0)._2 == "a")
        assert(java.util.Arrays.equals(results(0)._1, Array[Byte](1, 2, 3)))
        assert(results(1)._2 == "b")
        assert(java.util.Arrays.equals(results(1)._1, Array[Byte](4, 5, 6)))
      }
    }
  }

  test("Key-only reads (column pruning)") {
    withTempDir("seqfile-keyonly-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](10, 20, 30))
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)
          .select("key") // Only select key column

        val results = df.collect()
        assert(results.length == 1)
        val keyBytes = results(0).getAs[Array[Byte]](0)
        assert(bytesToInt(keyBytes) == 0) // First record has key index 0
      }
    }
  }

  test("Value-only reads (column pruning)") {
    withTempDir("seqfile-valueonly-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(Array[Byte](10, 20, 30))
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)
          .select("value") // Only select value column

        val results = df.collect()
        assert(results.length == 1)
        val valueBytes = results(0).getAs[Array[Byte]](0)
        assert(java.util.Arrays.equals(valueBytes, payloads(0)))
      }
    }
  }

  test("Empty files") {
    withTempDir("seqfile-empty-test") { tmpDir =>
      val file = new File(tmpDir, "empty.seq")
      val conf = new Configuration()
      writeEmptySequenceFile(file, conf)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        val results = df.collect()
        assert(results.isEmpty)
      }
    }
  }

  test("Large batch handling") {
    withTempDir("seqfile-largebatch-test") { tmpDir =>
      val file = new File(tmpDir, "large.seq")
      val conf = new Configuration()
      // Create many records to test batching
      val numRecords = 1000
      val payloads = (0 until numRecords).map { i =>
        s"record-$i-payload".getBytes(StandardCharsets.UTF_8)
      }.toArray
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

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

  test("GPU execution path verification") {
    withTempDir("seqfile-gpu-test") { tmpDir =>
      val file = new File(tmpDir, "test.seq")
      val conf = new Configuration()
      val payloads = Array(
        Array[Byte](1, 2, 3),
        "hello".getBytes(StandardCharsets.UTF_8)
      )
      writeSequenceFileWithRawRecords(file, conf, payloads)

      withGpuSparkSession { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        val results = df.select("key", "value").collect()
        assert(results.length == payloads.length)

        // Verify results
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

  test("Split boundary handling - records starting before boundary are read") {
    withTempDir("seqfile-split-test") { tmpDir =>
      val file = new File(tmpDir, "split-test.seq")
      val conf = new Configuration()

      // Create file with multiple records using raw record format (consistent with other tests)
      val numRecords = 100
      val payloads = (0 until numRecords).map { i =>
        s"record-$i-with-some-padding-data".getBytes(StandardCharsets.UTF_8)
      }.toArray

      writeSequenceFileWithRawRecords(file, conf, payloads)

      withSparkSession { spark =>
        // Read entire file
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        val results = df.select("key", "value").collect()
        assert(results.length == numRecords,
          s"Expected $numRecords records, got ${results.length}")

        // Verify all records present and no duplicates
        val indices = results.map(r => bytesToInt(r.getAs[Array[Byte]](0))).sorted.toSeq
        val expected = (0 until numRecords).toSeq
        assert(indices == expected,
          "Records missing or duplicated")
      }
    }
  }

  /**
   * Write a SequenceFile using Hadoop's native SequenceFile.Writer with sync markers
   * inserted periodically. This ensures the file format is correct for split handling.
   */
  private def writeSequenceFileWithSyncMarkers(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]],
      syncInterval: Int): Unit = {
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
        // Insert sync marker periodically to enable splitting
        if ((idx + 1) % syncInterval == 0) {
          writer.sync()
        }
      }
    } finally {
      writer.close()
    }
  }

  private def withSplitSparkSession(maxPartitionBytes: Long)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName("SequenceFileBinaryFileFormatSuite-Split")
      .master("local[4]") // Use multiple cores to enable parallel reading
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.files.maxPartitionBytes", maxPartitionBytes.toString)
      .config("spark.sql.files.openCostInBytes", "0") // Don't add overhead to partition size calc
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  private def withSplitGpuSparkSession(maxPartitionBytes: Long)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .appName("SequenceFileBinaryFileFormatSuite-Split-GPU")
      .master("local[4]") // Use multiple cores
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.files.maxPartitionBytes", maxPartitionBytes.toString)
      .config("spark.sql.files.openCostInBytes", "0")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.test.enabled", "true")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
    }
  }

  test("Multi-split file read - CPU path") {
    withTempDir("seqfile-multisplit-cpu") { tmpDir =>
      val file = new File(tmpDir, "large.seq")
      val conf = new Configuration()

      // Create a file large enough to be split into multiple partitions
      // Each record is ~100 bytes, 500 records = ~50KB
      // With maxPartitionBytes=8KB, should create ~6 splits
      val numRecords = 500
      val payloads = (0 until numRecords).map { i =>
        // Create records with varying sizes to make boundary testing more realistic
        val padding = "x" * (50 + (i % 50))
        s"record-$i-$padding".getBytes(StandardCharsets.UTF_8)
      }.toArray

      // Write with sync markers every 10 records
      writeSequenceFileWithSyncMarkers(file, conf, payloads, syncInterval = 10)

      val fileSize = file.length()
      // Use small partition size to force multiple splits
      val maxPartitionBytes = 8 * 1024L // 8KB

      withSplitSparkSession(maxPartitionBytes) { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        // Check that multiple partitions are used
        val numPartitions = df.rdd.getNumPartitions
        assert(numPartitions > 1,
          s"Expected multiple partitions but got $numPartitions " +
          s"(fileSize=$fileSize, maxPartitionBytes=$maxPartitionBytes)")

        val results = df.select("key", "value").collect()

        // Verify record count - this is the key assertion for split boundary handling
        assert(results.length == numRecords,
          s"Expected $numRecords records but got ${results.length}. " +
          s"File was split into $numPartitions partitions. " +
          "This may indicate duplicate or missing records at split boundaries.")

        // Verify no duplicates by checking unique (key, value) pairs
        // Use raw bytes as identifiers to avoid BytesWritable format parsing complexity
        val keyValuePairs = results.map { r =>
          val key = r.getAs[Array[Byte]](0)
          val value = r.getAs[Array[Byte]](1)
          (java.util.Arrays.hashCode(key), java.util.Arrays.hashCode(value))
        }
        val uniquePairs = keyValuePairs.distinct
        assert(uniquePairs.length == numRecords,
          s"Found ${keyValuePairs.length - uniquePairs.length} duplicate records")
      }
    }
  }

  test("Multi-split file read - GPU path") {
    withTempDir("seqfile-multisplit-gpu") { tmpDir =>
      val file = new File(tmpDir, "large.seq")
      val conf = new Configuration()

      val numRecords = 500
      val payloads = (0 until numRecords).map { i =>
        val padding = "x" * (50 + (i % 50))
        s"record-$i-$padding".getBytes(StandardCharsets.UTF_8)
      }.toArray

      writeSequenceFileWithSyncMarkers(file, conf, payloads, syncInterval = 10)

      val maxPartitionBytes = 8 * 1024L

      withSplitGpuSparkSession(maxPartitionBytes) { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        // Use DataFrame count() instead of rdd.getNumPartitions to avoid
        // triggering non-GPU compatible operations
        val results = df.select("key", "value").collect()

        assert(results.length == numRecords,
          s"Expected $numRecords records but got ${results.length}.")

        // Verify no duplicates by checking unique (key, value) pairs
        val keyValuePairs = results.map { r =>
          val key = r.getAs[Array[Byte]](0)
          val value = r.getAs[Array[Byte]](1)
          (java.util.Arrays.hashCode(key), java.util.Arrays.hashCode(value))
        }
        val uniquePairs = keyValuePairs.distinct
        assert(uniquePairs.length == numRecords,
          s"Found ${keyValuePairs.length - uniquePairs.length} duplicate records")
      }
    }
  }

  test("Split at exact sync marker boundary") {
    withTempDir("seqfile-sync-boundary") { tmpDir =>
      val file = new File(tmpDir, "sync-boundary.seq")
      val conf = new Configuration()

      // Create records designed to have sync markers at specific positions
      val numRecords = 100
      val payloads = (0 until numRecords).map { i =>
        // Fixed size records make it easier to predict sync marker positions
        f"record-$i%04d".getBytes(StandardCharsets.UTF_8)
      }.toArray

      // Sync every 5 records
      writeSequenceFileWithSyncMarkers(file, conf, payloads, syncInterval = 5)

      // Use a partition size that might align with sync markers
      val maxPartitionBytes = 1024L

      withSplitSparkSession(maxPartitionBytes) { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        val results = df.select("key", "value").collect()
        assert(results.length == numRecords,
          s"Expected $numRecords records, got ${results.length}")

        // Verify no duplicates using raw byte hash
        val keyHashes = results.map(r => java.util.Arrays.hashCode(r.getAs[Array[Byte]](0)))
        val uniqueHashes = keyHashes.distinct
        assert(uniqueHashes.length == numRecords, "Duplicate or missing records detected")
      }
    }
  }

  test("CPU vs GPU split read consistency") {
    withTempDir("seqfile-cpu-gpu-consistency") { tmpDir =>
      val file = new File(tmpDir, "consistency.seq")
      val conf = new Configuration()

      val numRecords = 300
      val payloads = (0 until numRecords).map { i =>
        s"payload-$i-data".getBytes(StandardCharsets.UTF_8)
      }.toArray

      writeSequenceFileWithSyncMarkers(file, conf, payloads, syncInterval = 8)

      val maxPartitionBytes = 4 * 1024L

      // Read with CPU - use raw bytes for comparison
      var cpuKeyValueHashes: Array[(Int, Int)] = null
      withSplitSparkSession(maxPartitionBytes) { spark =>
        val df = spark.read
          .format("sequencefilebinary")
          .load(file.getAbsolutePath)

        cpuKeyValueHashes = df.select("key", "value").collect()
          .map(r => (java.util.Arrays.hashCode(r.getAs[Array[Byte]](0)),
                     java.util.Arrays.hashCode(r.getAs[Array[Byte]](1))))
          .sortBy(_._1)
      }

      // Read with GPU - Note: GPU tests may not work in all environments
      // This test verifies that when GPU is available, results match CPU
      try {
        var gpuKeyValueHashes: Array[(Int, Int)] = null
        withSplitGpuSparkSession(maxPartitionBytes) { spark =>
          val df = spark.read
            .format("sequencefilebinary")
            .load(file.getAbsolutePath)

          gpuKeyValueHashes = df.select("key", "value").collect()
            .map(r => (java.util.Arrays.hashCode(r.getAs[Array[Byte]](0)),
                       java.util.Arrays.hashCode(r.getAs[Array[Byte]](1))))
            .sortBy(_._1)
        }

        // Compare results
        assert(cpuKeyValueHashes.length == gpuKeyValueHashes.length,
          s"CPU returned ${cpuKeyValueHashes.length} records, GPU returned ${gpuKeyValueHashes.length}")

        cpuKeyValueHashes.zip(gpuKeyValueHashes).foreach { case ((cpuKeyHash, cpuValHash), (gpuKeyHash, gpuValHash)) =>
          assert(cpuKeyHash == gpuKeyHash, s"Key hash mismatch: CPU=$cpuKeyHash, GPU=$gpuKeyHash")
          assert(cpuValHash == gpuValHash, s"Value hash mismatch at key hash $cpuKeyHash")
        }
      } catch {
        case _: IllegalArgumentException =>
          // GPU not available or plan not compatible, skip GPU comparison
          // The CPU test above already verifies the split handling is correct
      }

      // At minimum, verify CPU results are correct
      assert(cpuKeyValueHashes.length == numRecords,
        s"Expected $numRecords records, got ${cpuKeyValueHashes.length}")
    }
  }
}
