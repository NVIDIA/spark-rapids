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
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

private[rapids] object SequenceFileTestUtils {
  def withSequenceFileSession(
      appName: String,
      physicalReplaceEnabled: Boolean = false,
      extraConfs: Map[String, String] = Map.empty)(f: SparkSession => Unit): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val builder = SparkSession.builder()
      .appName(appName)
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.extensions", "com.nvidia.spark.rapids.SQLExecPlugin")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.enabled", "true")
      .config(
        "spark.rapids.sql.format.sequencefile.rddScan.physicalReplace.enabled",
        physicalReplaceEnabled.toString)

    if (physicalReplaceEnabled) {
      builder
        .config("spark.rapids.sql.explain", "ALL")
    }
    extraConfs.foreach { case (key, value) =>
      builder.config(key, value)
    }

    val spark = builder.getOrCreate()
    try {
      f(spark)
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val tmpDir = Files.createTempDirectory(prefix).toFile
    try {
      f(tmpDir)
    } finally {
      deleteRecursively(tmpDir)
    }
  }

  def writeSequenceFile(
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

  def readSequenceFileValueOnly(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    val sc = spark.sparkContext
    sc.newAPIHadoopFile(
      path,
      classOf[SequenceFileAsBinaryInputFormat],
      classOf[BytesWritable],
      classOf[BytesWritable]
    ).map { case (_, v) =>
      bytesWritablePayload(v.getBytes, v.getLength)
    }.toDF("value")
  }

  def bytesWritablePayload(bytes: Array[Byte], len: Int): Array[Byte] = {
    if (len < 4) {
      Array.emptyByteArray
    } else {
      val payloadLen = ((bytes(0) & 0xFF) << 24) |
        ((bytes(1) & 0xFF) << 16) |
        ((bytes(2) & 0xFF) << 8) |
        (bytes(3) & 0xFF)
      if (payloadLen > 0 && payloadLen <= len - 4) {
        java.util.Arrays.copyOfRange(bytes, 4, 4 + payloadLen)
      } else {
        Array.emptyByteArray
      }
    }
  }

  def intToBytes(i: Int): Array[Byte] = Array[Byte](
    ((i >> 24) & 0xFF).toByte,
    ((i >> 16) & 0xFF).toByte,
    ((i >> 8) & 0xFF).toByte,
    (i & 0xFF).toByte
  )

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
}
