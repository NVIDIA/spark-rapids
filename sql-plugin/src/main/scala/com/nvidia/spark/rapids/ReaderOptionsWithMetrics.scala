/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * This utility class conveys custom metrics through the DataSource V1 API.
 * It appears as a simple `Map[String,String]` to the Spark APIs, but it can
 * convey GPU-specific metrics the GPU readers can use.
 * @param options reader options map
 * @param metrics GPU metrics map
 */
case class ReaderOptionsWithMetrics(
    options: Map[String, String],
    metrics: Map[String, SQLMetric]) extends Map[String,String] {
  override def +[V1 >: String](kv: (String, V1)): Map[String, V1] =
    throw new UnsupportedOperationException("map is read only")

  override def get(key: String): Option[String] = options.get(key)

  override def iterator: Iterator[(String, String)] = options.iterator

  override def -(key: String): Map[String, String] =
    throw new UnsupportedOperationException("map is read only")
}
