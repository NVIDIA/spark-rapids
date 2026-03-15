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

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, SparkSession}
class SequenceFilePhysicalReplaceSuite extends AnyFunSuite {

  private def withPhysicalReplaceEnabledSession(f: SparkSession => Unit): Unit = {
    withPhysicalReplaceEnabledSession(Map.empty[String, String])(f)
  }

  private def withPhysicalReplaceEnabledSession(
      extraConfs: Map[String, String])(
      f: SparkSession => Unit): Unit = {
    SequenceFileTestUtils.withSequenceFileSession(
      "SequenceFilePhysicalReplaceSuite",
      physicalReplaceEnabled = true,
      extraConfs = extraConfs)(f)
  }

  private def withTempDir(prefix: String)(f: File => Unit): Unit = {
    SequenceFileTestUtils.withTempDir(prefix)(f)
  }

  private def writeSequenceFile(
      file: File,
      conf: Configuration,
      payloads: Array[Array[Byte]]): Unit = {
    SequenceFileTestUtils.writeSequenceFile(file, conf, payloads)
  }

  private def readSequenceFileValueOnly(spark: SparkSession, path: String): DataFrame = {
    SequenceFileTestUtils.readSequenceFileValueOnly(spark, path)
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

  test("Physical replacement respects ignoreMissingFiles for missing inputs") {
    withTempDir("seqfile-physical-ignore-missing-test") { tmpDir =>
      val existing = new File(tmpDir, "existing.seq")
      val missing = new File(tmpDir, "missing.seq")
      val conf = new Configuration()
      val payloads = Array(
        "keep-a".getBytes(StandardCharsets.UTF_8),
        "keep-b".getBytes(StandardCharsets.UTF_8))
      writeSequenceFile(existing, conf, payloads)

      withPhysicalReplaceEnabledSession(Map("spark.sql.files.ignoreMissingFiles" -> "true")) {
          spark =>
        val path = s"${existing.getAbsolutePath},${missing.getAbsolutePath}"
        val df = readSequenceFileValueOnly(spark, path)
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

}
