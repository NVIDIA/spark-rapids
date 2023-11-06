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

/*** spark-rapids-shim-json-lines
{"spark": "321db"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

object ParquetFieldIdShims {
  val fieldIdOverrideKey: String = "spark.rapids.sql.parquet.writeFieldIds"

  /** Updates the Hadoop configuration with the Parquet field ID write setting from SQLConf */
  def setupParquetFieldIdWriteConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // No SQL conf for this config in Spark 3.2.x
  }

  /** Get Parquet field ID write enabled configuration value */
  def getParquetIdWriteEnabled(conf: Configuration, sqlConf: SQLConf): Boolean = {
    // No SQL conf for this config in Spark 3.2.x
    conf.get(fieldIdOverrideKey, "false").toBoolean
  }

  /** Set the Parquet field ID write enable override */
  def setWriteIdOverride(conf: Configuration, enabled: Boolean): Unit = {
    conf.set(fieldIdOverrideKey, enabled.toString)
  }
}
