/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark304

import com.nvidia.spark.rapids.ShimVersion
import com.nvidia.spark.rapids.shims.v2._
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters

class Spark304Shims extends Spark30XShims with Spark30Xuntil33XShims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      dateTimeRebaseModeFromConf: String): ParquetFilters = {
    val datetimeRebaseMode = DataSourceUtils
      .datetimeRebaseMode(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }
}
