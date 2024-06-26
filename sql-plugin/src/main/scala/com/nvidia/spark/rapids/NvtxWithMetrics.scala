/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}

object NvtxWithMetrics {
  def apply(name: String, color: NvtxColor, metric: Option[GpuMetric]): NvtxRange = {
    metric match {
      case Some(m) => new NvtxWithMetrics(name, color, m)
      case _ => new NvtxRange(name, color)
    }
  }
}

/**
 *  NvtxRange with option to pass one or more nano timing metric(s) that are updated upon close
 *  by the amount of time spent in the range
 */
class NvtxWithMetrics(name: String, color: NvtxColor, val metrics: GpuMetric*)
  extends NvtxRange(name, color) {

  val needTracks = metrics.map(_.tryActivateTimer())
  private val start = System.nanoTime()

  override def close(): Unit = {
    val time = System.nanoTime() - start
    metrics.toSeq.zip(needTracks).foreach { pair =>
      if (pair._2) {
        pair._1.deactivateTimer(time)
      }
    }
    super.close()
  }
}

class MetricRange(val metrics: GpuMetric*) extends AutoCloseable {
  val needTracks = metrics.map(_.tryActivateTimer())
  private val start = System.nanoTime()

  override def close(): Unit = {
    val time = System.nanoTime() - start
    metrics.toSeq.zip(needTracks).foreach { pair =>
      if (pair._2) {
        pair._1.deactivateTimer(time)
      }
    }
  }
}
