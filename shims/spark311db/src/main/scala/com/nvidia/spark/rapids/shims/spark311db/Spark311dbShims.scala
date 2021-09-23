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

package com.nvidia.spark.rapids.shims.spark311db

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.v2.SparkBaseShims
import com.nvidia.spark.rapids.spark311db.RapidsShuffleManager
import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.internal.SQLConf

class Spark311dbShims extends SparkBaseShims {

  override def getSparkShimVersion: ShimVersion = SparkShimServiceProvider.VERSION

  override def getRapidsShuffleManagerClass: String = {
    classOf[RapidsShuffleManager].getCanonicalName
  }

  override def getParquetFilters(
      schema: MessageType,
      pushDownDate: Boolean,
      pushDownTimestamp: Boolean,
      pushDownDecimal: Boolean,
      pushDownStartWith: Boolean,
      pushDownInFilterThreshold: Int,
      caseSensitive: Boolean,
      datetimeRebaseMode: SQLConf.LegacyBehaviorPolicy.Value): ParquetFilters =
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)

  override def int96ParquetRebaseRead(conf: SQLConf): String = {
    conf.getConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ)
  }

  override def int96ParquetRebaseWrite(conf: SQLConf): String = {
    conf.getConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE)
  }

  override def int96ParquetRebaseReadKey: String = {
    SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ.key
  }

  override def int96ParquetRebaseWriteKey: String = {
    SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE.key
  }
}
