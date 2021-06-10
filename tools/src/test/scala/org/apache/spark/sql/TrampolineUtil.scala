/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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
package org.apache.spark.sql

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

object TrampolineUtil {
  /** Shuts down and cleans up any existing Spark session */
  def cleanupAnyExistingSession(): Unit = SparkSession.cleanupAnyExistingSession()

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempDir(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def createCodec(conf: SparkConf, codecName: String) =
    CompressionCodec.createCodec(conf, codecName)
}
