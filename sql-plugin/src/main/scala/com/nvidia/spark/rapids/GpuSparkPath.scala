/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * This is copied from the `SparkPath` in Apache Spark.
 */
final case class GpuSparkPath private (private val underlying: String) {
  def urlEncoded: String = underlying
  def toUri: URI = new URI(underlying)
  def toPath: Path = new Path(toUri)
  override def toString: String = underlying
}

object GpuSparkPath {
  /**
   * Creates a SparkPath from a hadoop Path string.
   * Please be very sure that the provided string is encoded (or not encoded) in the right way.
   *
   * Please see the hadoop Path documentation here:
   * https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/Path.html#Path-java.lang.String-
   */
  def fromPathString(str: String): GpuSparkPath = fromPath(new Path(str))
  def fromPath(path: Path): GpuSparkPath = fromUri(path.toUri)
  def fromFileStatus(fs: FileStatus): GpuSparkPath = fromPath(fs.getPath)

  /**
   * Creates a SparkPath from a url-encoded string.
   * Note: It is the responsibility of the caller to ensure that str is a valid url-encoded string.
   */
  def fromUrlString(str: String): GpuSparkPath = GpuSparkPath(str)
  def fromUri(uri: URI): GpuSparkPath = fromUrlString(uri.toString)
}
