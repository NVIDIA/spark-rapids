/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids.RapidsMeta
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ParquetFieldIdShims {
  /** Updates the Hadoop configuration with the Parquet field ID write setting from SQLConf */
  def setupParquetFieldIdWriteConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // Parquet field ID support configs are not supported until Spark 3.3
  }

  def tagGpuSupportWriteForFieldId(meta: RapidsMeta[_, _, _], schema: StructType,
      conf: SQLConf): Unit = {
    // Parquet field ID support configs are not supported until Spark 3.3
  }

  def tagGpuSupportReadForFieldId(meta: RapidsMeta[_, _, _], conf: SQLConf): Unit = {
    // Parquet field ID support configs are not supported until Spark 3.3
  }
}
