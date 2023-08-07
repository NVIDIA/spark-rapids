/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
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
package com.nvidia.spark.rapids.tests.orc

import java.io.{File, IOException}
import java.nio.file.Files
import java.util.UUID

import com.nvidia.spark.rapids.RapidsConf
import org.apache.hadoop.fs.FileUtil
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class TestBase extends AnyFunSuite {
  lazy val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.sql.queryExecutionListeners",
        "org.apache.spark.sql.rapids.ExecutionPlanCaptureCallback")
      .getOrCreate()

  def withGpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    val c = conf.clone()
        .set(RapidsConf.SQL_ENABLED.key, "true")
        .set(RapidsConf.TEST_CONF.key, "true")
        .set(RapidsConf.EXPLAIN.key, "ALL")
    withSparkSession(c, f)
  }

  def withCpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    val c = conf.clone()
        .set(RapidsConf.SQL_ENABLED.key, "false")
    withSparkSession(c, f)
  }

  private def withSparkSession[U](conf: SparkConf, f: SparkSession => U): U = {
    setAllConfs(conf.getAll)
    f(spark)
  }

  private def setAllConfs(confs: Array[(String, String)]): Unit = confs.foreach {
    case (key, value) if spark.conf.get(key, null) != value =>
      spark.conf.set(key, value)
    case _ => // No need to modify it
  }

  def withTempPath[B](func: File => B): B = {
    val rootTmpDir = System.getProperty("java.io.tmpdir")
    val dirFile = new File(rootTmpDir, "spark-test-" + UUID.randomUUID)
    Files.createDirectories(dirFile.toPath)
    if (!dirFile.delete()) throw new IOException(s"Delete $dirFile failed!")
    try func(dirFile) finally FileUtil.fullyDelete(dirFile)
  }
}
