/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.hive.rapids

import com.nvidia.spark.rapids.{ExprRule, ShimLoader}

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * The subclass of HiveProvider imports spark-hive classes. This file should not imports
 * spark-hive because `class not found` exception may throw if spark-hive does not exist at
 * runtime. Details see: https://github.com/NVIDIA/spark-rapids/issues/5648
 */
trait HiveProvider {
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]
}

object GpuHiveOverrides {
  def isSparkHiveAvailable: Boolean = {
    try {
      ShimLoader.loadClass("org.apache.spark.sql.hive.HiveSessionStateBuilder")
      ShimLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }

  /**
   * Builds the rules that are specific to spark-hive Catalyst nodes. This will return an empty
   * mapping if spark-hive is unavailable.
   */
  def exprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    if (isSparkHiveAvailable) {
      val className = "org.apache.spark.sql.hive.rapids.HiveSourceProvider"
      ShimLoader.newInstanceOf[HiveProvider](className).getExprs
    } else {
      Map.empty
    }
  }
}
