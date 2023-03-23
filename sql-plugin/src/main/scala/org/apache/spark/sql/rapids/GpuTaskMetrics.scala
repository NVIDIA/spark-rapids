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

package org.apache.spark.sql.rapids

import java.io.ObjectInputStream
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm
import java.{lang => jl}

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{AccumulatorV2, Utils}

case class NanoTime(value: java.lang.Long) {
  override def toString: String = {
    val hours = TimeUnit.NANOSECONDS.toHours(value)
    var remaining = value - TimeUnit.HOURS.toNanos(hours)
    val minutes = TimeUnit.NANOSECONDS.toMinutes(remaining)
    remaining = remaining - TimeUnit.MINUTES.toNanos(minutes)
    val seconds = remaining.toDouble / TimeUnit.SECONDS.toNanos(1)
    val locale = Locale.US
    "%02d:%02d:%02.3f".formatLocal(locale, hours, minutes, seconds)
  }
}

class NanoSecondAccumulator extends AccumulatorV2[jl.Long, NanoTime] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0


  override def copy(): NanoSecondAccumulator = {
    val newAcc = new NanoSecondAccumulator
    newAcc._sum = this._sum
    newAcc
  }

  override def reset(): Unit = {
    _sum = 0
  }

  override def add(v: jl.Long): Unit = {
    _sum += v
  }

  def add (v: Long): Unit = {
    _sum += v
  }

  override def merge(other: AccumulatorV2[jl.Long, NanoTime]): Unit = other match {
    case ns: NanoSecondAccumulator =>
      _sum += ns._sum
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: NanoTime = NanoTime(_sum)
}

class GpuTaskMetrics extends Arm with Serializable {
  private val semWaitTimeNs: NanoSecondAccumulator = new NanoSecondAccumulator

  private val metrics = Map[String, AccumulatorV2[_, _]](
    "semaphore_wait" -> semWaitTimeNs)

  def register(sc: SparkContext): Unit = {
    metrics.foreach { case (k, m) =>
        sc.register(m, k)
    }
  }

  def makeSureRegistered(): Unit = {
    // This is a noop for now, but need to make sure something happens
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    // Now we need to make sure that we are registered with the proper task
    GpuTaskMetrics.registerOnTask(this)
  }

  def semWaitTime[A](f: => A): A = {
    val start = System.nanoTime()
    withResource(new NvtxRange("Acquire GPU", NvtxColor.RED)) { _ =>
      try {
        f
      } finally {
        semWaitTimeNs.add(System.nanoTime() - start)
      }
    }
  }
}

/**
 * Provides task level metrics
 */
object GpuTaskMetrics extends Logging {
  private val taskLevelMetrics = mutable.Map[Long, GpuTaskMetrics]()

  def registerOnTask(metrics: GpuTaskMetrics): Unit = synchronized {
    val tc = TaskContext.get()
    val id = tc.taskAttemptId()
    // avoid double registering the task metrics...
    if (!taskLevelMetrics.contains(id)) {
      taskLevelMetrics.put(id, metrics)
      tc.addTaskCompletionListener { tc =>
        synchronized {
          taskLevelMetrics.remove(tc.taskAttemptId())
        }
      }
    }
  }

  def get: GpuTaskMetrics = synchronized {
    val tc = TaskContext.get()
    val metrics = if (tc != null) {
      taskLevelMetrics.get(tc.taskAttemptId())
    } else {
      None
    }
    // As a backstop better to not have metrics than to crash...
    // Spark does this too for regular task metrics
    metrics.getOrElse(new GpuTaskMetrics)
  }
}
