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

package com.nvidia.spark.rapids.shims

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.internal.SQLConf

object ParquetTimestampNTZShims {

  def setupTimestampNTZConfig(conf: Configuration, sqlConf: SQLConf): Unit = {
    // This timestamp_NTZ flag is introduced in Spark 3.4.0
    conf.setBoolean(
      SQLConf.PARQUET_TIMESTAMP_NTZ_ENABLED.key,
      sqlConf.parquetTimestampNTZEnabled)
  }
}
