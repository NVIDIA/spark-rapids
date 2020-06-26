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

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.broadcast.Broadcast
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
}
