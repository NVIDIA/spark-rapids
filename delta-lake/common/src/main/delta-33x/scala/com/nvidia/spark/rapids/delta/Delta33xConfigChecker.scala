/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids.RapidsMeta

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.rapids.DeltaRuntimeShim
import org.apache.spark.sql.internal.SQLConf

object Delta33xConfigChecker extends DeltaConfigChecker {
  override def checkIncompatibleConfs(
      meta: RapidsMeta[_, _, _],
      deltaLog: Option[DeltaLog],
      sqlConf: SQLConf,
      options: Map[String, String]
  ): Unit = {
    def getSQLConf(key: String): Option[String] = {
      try {
        Option(sqlConf.getConfString(key))
      } catch {
        case _: NoSuchElementException => None
      }
    }

    val autoCompactEnabled =
      getSQLConf("spark.databricks.delta.autoCompact.enabled").orElse {
        deltaLog.flatMap { log =>
          val metadata = DeltaRuntimeShim.unsafeVolatileSnapshotFromLog(log).metadata
          metadata.configuration.get("delta.autoOptimize.autoCompact").orElse {
            getSQLConf("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact")
          }
        }
      }.exists(_.toBoolean)
    if (autoCompactEnabled) {
      meta.willNotWorkOnGpu("automatic compaction of Delta Lake tables is not supported")
    }
  }
}
