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

import org.apache.parquet.schema.MessageType

import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ParquetSchemaClipShims {

  def useFieldId(conf: SQLConf): Boolean = conf.parquetFieldIdReadEnabled
  def timestampNTZEnabled(conf: SQLConf): Boolean = conf.parquetTimestampNTZEnabled

  def clipSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): MessageType = {
    ParquetReadSupport.clipParquetSchema(parquetSchema, catalystSchema, caseSensitive,
      useFieldId, timestampNTZEnabled)
  }
}
