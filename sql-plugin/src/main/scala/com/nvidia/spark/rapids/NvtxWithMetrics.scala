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

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}

object NvtxWithMetrics {
  def apply(name: String, color: NvtxColor, metric: Option[GpuMetric]): NvtxRange = {
    metric match {
      case Some(m) => new NvtxWithMetrics(name, color, m)
      case _ => new NvtxRange(name, color)
    }
  }
}

object ThreadLocalMetrics {
  val addressOrdering: Ordering[GpuMetric] = Ordering.by(System.identityHashCode(_))

  val currentThreadMetrics = new ThreadLocal[mutable.TreeMap[GpuMetric, String]] {
    override def initialValue(): mutable.TreeMap[GpuMetric, String] =
      mutable.TreeMap[GpuMetric, String]()(addressOrdering)
  }

  def getStackTraceString(stackTrace: Array[StackTraceElement]): String = {
    stackTrace.map(_.toString).mkString("\n")
  }

  def onMetricsEnter(gpuMetric: GpuMetric): Unit = {
    if (gpuMetric != NoopMetric) {
      if (ThreadLocalMetrics.currentThreadMetrics.get().contains(gpuMetric)) {
        throw new IllegalArgumentException("Cannot add the same metric to the same thread twice: "
          + gpuMetric + ". Last site: \n"
          + ThreadLocalMetrics.currentThreadMetrics.get().get(gpuMetric))
      }
      ThreadLocalMetrics.currentThreadMetrics.get().put(gpuMetric,
        getStackTraceString(new Throwable().getStackTrace))
    }
  }

  def onMetricsExit(gpuMetric: GpuMetric): Unit = {
    if (gpuMetric != NoopMetric) {
      if (!ThreadLocalMetrics.currentThreadMetrics.get().contains(gpuMetric)) {
        throw new IllegalArgumentException("Metric was removed from thread local storage: "
          + gpuMetric)
      }
      ThreadLocalMetrics.currentThreadMetrics.get().remove(gpuMetric)
    }
  }
}
/**
 *  NvtxRange with option to pass one or more nano timing metric(s) that are updated upon close
 *  by the amount of time spent in the range
 */
class NvtxWithMetrics(name: String, color: NvtxColor, val metrics: GpuMetric*)
    extends NvtxRange(name, color) {

  for (metrics <- metrics) {
    ThreadLocalMetrics.onMetricsEnter(metrics)
  }
  private val start = System.nanoTime()

  override def close(): Unit = {
    val time = System.nanoTime() - start
    metrics.foreach { metric =>
      metric += time
    }
    for (metrics <- metrics) {
      ThreadLocalMetrics.onMetricsExit(metrics)
    }
    super.close()
  }
}

class MetricRange(val metrics: GpuMetric*) extends AutoCloseable {
  for (metrics <- metrics) {
    ThreadLocalMetrics.onMetricsEnter(metrics)
  }
  private val start = System.nanoTime()

  override def close(): Unit = {
    val time = System.nanoTime() - start
    metrics.foreach { metric =>
      metric += time
    }
    for (metrics <- metrics) {
      ThreadLocalMetrics.onMetricsExit(metrics)
    }
  }
}
