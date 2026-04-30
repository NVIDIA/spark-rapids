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

package com.nvidia.spark.rapids.delta.shims

import org.apache.spark.ShuffleDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ShuffledRowRDD, ShufflePartitionSpec}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * DB-17.3 shim for ShuffledRowRDD construction.
 * DB-17.3's ShuffledRowRDD requires PrismMetrics, numMappers, and refHolder params.
 *
 * prismMetrics=null is safe: only consumed by PrismFetchQueueFactory.createIfEnabled,
 * which is Photon-only and null-tolerant when Prism is disabled (the GPU plugin path).
 * numMappers=-1 matches Databricks' own ShuffleExchangeExec.doExecute convention for
 * "not applicable".
 */
object ShimShuffledRowRDD {
  def create(
      dep: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric]): ShuffledRowRDD = {
    // 5-arg: (dep, metrics, prismMetrics, numMappers, refHolder)
    new ShuffledRowRDD(dep, metrics, null, -1, None)
  }

  def create(
      dep: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric],
      specs: Array[ShufflePartitionSpec]): ShuffledRowRDD = {
    // 7-arg: (dep, metrics, specs, prismMetrics, isBarrier, numMappers, refHolder)
    new ShuffledRowRDD(dep, metrics, specs, null, false, -1, None)
  }
}
