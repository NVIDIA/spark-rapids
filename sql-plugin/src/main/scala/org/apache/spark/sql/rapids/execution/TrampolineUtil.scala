/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution

import org.json4s.JsonAST

import org.apache.spark.{SparkContext, SparkEnv, SparkUpgradeException, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, IdentityBroadcastMode}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.Utils

object TrampolineUtil {
  def doExecuteBroadcast[T](child: SparkPlan): Broadcast[T] = child.doExecuteBroadcast()

  def isSupportedRelation(mode: BroadcastMode): Boolean = mode match {
    case _ : HashedRelationBroadcastMode => true
    case IdentityBroadcastMode => true
    case _ => false
  }

  def structTypeMerge(left: DataType, right: DataType): DataType = StructType.merge(left, right)

  def jsonValue(dataType: DataType): JsonAST.JValue = dataType.jsonValue

  /** Get a human-readable string, e.g.: "4.0 MiB", for a value in bytes. */
  def bytesToString(size: Long): String = Utils.bytesToString(size)

  /** Returns true if called from code running on the Spark driver. */
  def isDriver(env: SparkEnv): Boolean = {
    if (env != null) {
      env.executorId == SparkContext.DRIVER_IDENTIFIER
    } else {
      false
    }
  }

  /**
   * Return true if the provided predicate function returns true for any
   * type node within the datatype tree.
   */
  def dataTypeExistsRecursively(dt: DataType, f: DataType => Boolean): Boolean = {
    dt.existsRecursively(f)
  }

  def incInputRecordsRows(inputMetrics: InputMetrics, rows: Long): Unit =
    inputMetrics.incRecordsRead(rows)

  def makeSparkUpgradeException(
                                version: String,
                                message: String,
                                cause: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(version, message, cause)
  }

  /** Shuts down and cleans up any existing Spark session */
  def cleanupAnyExistingSession(): Unit = SparkSession.cleanupAnyExistingSession()

  def asNullable(dt: DataType): DataType = dt.asNullable

  /**
   * Increment the task's memory bytes spilled metric. If the current thread does not
   * correspond to a Spark task then this call does nothing.
   * @param amountSpilled amount of memory spilled in bytes
   */
  def incTaskMetricsMemoryBytesSpilled(amountSpilled: Long): Unit = {
    Option(TaskContext.get).foreach(_.taskMetrics().incMemoryBytesSpilled(amountSpilled))
  }

  /**
   * Increment the task's disk bytes spilled metric. If the current thread does not
   * correspond to a Spark task then this call does nothing.
   * @param amountSpilled amount of memory spilled in bytes
   */
  def incTaskMetricsDiskBytesSpilled(amountSpilled: Long): Unit = {
    Option(TaskContext.get).foreach(_.taskMetrics().incDiskBytesSpilled(amountSpilled))
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes read. If
   * getFSBytesReadOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes read on r since t.
   */
  def getFSBytesReadOnThreadCallback(): () => Long = {
    SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
  }

  /** Set the bytes read task input metric */
  def incBytesRead(inputMetrics: InputMetrics, bytesRead: Long): Unit = {
    inputMetrics.incBytesRead(bytesRead)
  }
}
