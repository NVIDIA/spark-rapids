/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shims.spark312

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2._
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters

class Spark312Shims extends Spark31XShims with Spark30Xuntil33XShims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def hasCastFloatTimestampUpcast: Boolean = true

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      lookupFileMeta: String => String,
      modeByConfig: String): ParquetFilters = {
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive)
  }
}
