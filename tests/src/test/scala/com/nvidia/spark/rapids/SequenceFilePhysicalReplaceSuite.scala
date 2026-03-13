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

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}

class SequenceFilePhysicalReplaceSuite extends AnyFunSuite {

  private def withPhysicalReplaceEnabledSession(f: SparkSession => Unit): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder()
      .appName("SequenceFilePhysicalReplaceSuite")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled", "true")
      .config("spark.rapids.sql.explain", "ALL")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val tmpDir = Files.createTempDirectory(prefix).toFile
    try {
      f(tmpDir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          Option(file.listFiles()).getOrElse(Array.empty).foreach(deleteRecursively)
        }
        if (file.exists()) {
          file.delete()
        }
      }
      deleteRecursively(tmpDir)
    }
  }

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
        val key = new BytesWritable(Array[Byte](
          ((idx >> 24) & 0xFF).toByte,
          ((idx >> 16) & 0xFF).toByte,
          ((idx >> 8) & 0xFF).toByte,
          (idx & 0xFF).toByte))
        val value = new BytesWritable(p)
        writer.append(key, value)
      }
    } finally {
      writer.close()
    }
  }

  private def writeLegacySequenceFileHeader(file: File, isCompressed: Boolean): Unit = {
    val out = new DataOutputStream(new FileOutputStream(file))
    try {
      out.writeByte('S')
      out.writeByte('E')
      out.writeByte('Q')
      out.writeByte(4) // pre-block-compression header version
      org.apache.hadoop.io.Text.writeString(out, classOf[BytesWritable].getName)
      org.apache.hadoop.io.Text.writeString(out, classOf[BytesWritable].getName)
      out.writeBoolean(isCompressed)
    } finally {
      out.close()
    }
  }

  private def readSequenceFileValueOnly(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    sc.newAPIHadoopFile(
      path,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (_, v) =>
      SequenceFileBinaryFileFormatSuite.bytesWritablePayload(v.getBytes, v.getLength)
    }.toDF("value")
  }

  private def hasGpuSequenceFileRDDScan(df: DataFrame): Boolean = {
    df.queryExecution.executedPlan.collect {
      case p if p.getClass.getSimpleName == "GpuSequenceFileSerializeFromObjectExec" => 1
    }.nonEmpty
  }

  test("Physical replacement hits GPU SequenceFile RDD scan for simple uncompressed path") {
    withTempDir("seqfile-physical-hit-test") { tmpDir =>
      val file = new File(tmpDir, "simple.seq")
      val conf = new Configuration()
      val payloads = Array(
        Array[Byte](1, 2, 3),
        "simple".getBytes(StandardCharsets.UTF_8))
      writeSequenceFile(file, conf, payloads)

      withPhysicalReplaceEnabledSession { spark =>
        val df = readSequenceFileValueOnly(spark, file.getAbsolutePath)
        assert(hasGpuSequenceFileRDDScan(df),
          s"Expected GPU SequenceFile exec in plan:\n${df.queryExecution.executedPlan}")
        val got = df.collect().map(_.getAs[Array[Byte]](0)).sortBy(_.length)
        val expected = payloads.sortBy(_.length)
        assert(got.length == expected.length)
        got.zip(expected).foreach { case (actual, exp) =>
          assert(java.util.Arrays.equals(actual, exp))
        }
      }
    }
  }

  test("Legacy SequenceFile headers do not read block compression flag") {
    withTempDir("seqfile-legacy-header-test") { tmpDir =>
      val file = new File(tmpDir, "legacy.seq")
      val conf = new Configuration()
      writeLegacySequenceFileHeader(file, isCompressed = false)

      val method = GpuSequenceFileSerializeFromObjectExecMeta.getClass.getDeclaredMethod(
        "isCompressedSequenceFile",
        classOf[Path],
        classOf[Configuration])
      method.setAccessible(true)
      val isCompressed = method.invoke(
        GpuSequenceFileSerializeFromObjectExecMeta,
        new Path(file.toURI),
        conf).asInstanceOf[Boolean]

      assert(!isCompressed)
    }
  }
}
