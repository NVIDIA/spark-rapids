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

import org.apache.parquet.schema.{MessageType, Type}

import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

object ParquetSchemaClipShims {
  /** Stubs for configs not defined before Spark 330 */
  def useFieldId(conf: SQLConf): Boolean = false

  def ignoreMissingIds(conf: SQLConf): Boolean = false

  def timestampNTZEnabled(conf: SQLConf): Boolean = false

  def checkIgnoreMissingIds(ignoreMissingIds: Boolean, parquetFileSchema: MessageType,
      catalystRequestedSchema: StructType): Unit = {}

  def hasFieldId(field: StructField): Boolean =
    throw new RuntimeException("This Shim should not invoke `hasFieldId`")

  def getFieldId(field: StructField): Int =
    throw new RuntimeException("This Shim should not invoke `getFieldId`")

  def fieldIdToFieldMap(useFieldId: Boolean, fileType: Type): Map[Int, Type] = Map.empty[Int, Type]

  def fieldIdToNameMap(useFieldId: Boolean,
      fileType: Type): Map[Int, String] = Map.empty[Int, String]

  def getType(map: Map[Int, Type], field: StructField): Option[Type] = None
  def getName(map: Map[Int, String], field: StructField): Option[String] = None

  def clipSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): MessageType = {
    ParquetReadSupport.clipParquetSchema(parquetSchema, catalystSchema, caseSensitive)
  }
}
