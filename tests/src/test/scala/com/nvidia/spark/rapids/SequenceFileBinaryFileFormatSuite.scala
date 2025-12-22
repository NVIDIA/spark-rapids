/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, SequenceFile, Text}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession

/**
 * Lives in the `tests` module so it can be discovered by the repo's standard
 * `-DwildcardSuites=...` test invocation.
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
    val tmpDir = Files.createTempDirectory("seqfile-binary-test").toFile
    tmpDir.deleteOnExit()
    val file = new File(tmpDir, "test.seq")
    file.deleteOnExit()

    val conf = new Configuration()
    val payloads: Array[Array[Byte]] = Array(
      Array[Byte](1, 2, 3),
      "hello".getBytes(StandardCharsets.UTF_8),
      Array.fill[Byte](10)(42.toByte)
    )
    writeSequenceFileWithRawRecords(file, conf, payloads)

    withSparkSession { spark =>
      val df = spark.read
        // Use the class name (not the short name) to avoid relying on ServiceLoader resources
        // being present in the tests module classpath.
        .format(classOf[SequenceFileBinaryFileFormat].getName)
        .load(file.getAbsolutePath)

      val got = df.select(SequenceFileBinaryFileFormat.KEY_FIELD,
          SequenceFileBinaryFileFormat.VALUE_FIELD)
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


