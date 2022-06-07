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

import scala.collection.JavaConverters._

import org.apache.parquet.schema.{MessageType, Type}

import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetUtils}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport.containsFieldIds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

object ParquetSchemaClipShims {

  def useFieldId(conf: SQLConf): Boolean = conf.parquetFieldIdReadEnabled

  def ignoreMissingIds(conf: SQLConf): Boolean = conf.ignoreMissingParquetFieldId

  def checkIgnoreMissingIds(ignoreMissingIds: Boolean, parquetFileSchema: MessageType,
      catalystRequestedSchema: StructType): Unit = {
    if (!ignoreMissingIds &&
        !containsFieldIds(parquetFileSchema) &&
        ParquetUtils.hasFieldIds(catalystRequestedSchema)) {
      throw new RuntimeException(
        "Spark read schema expects field Ids, " +
            "but Parquet file schema doesn't contain any field Ids.\n" +
            "Please remove the field ids from Spark schema or ignore missing ids by " +
            s"setting `${SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key} = true`\n" +
            s"""
               |Spark read schema:
               |${catalystRequestedSchema.prettyJson}
               |
               |Parquet file schema:
               |${parquetFileSchema.toString}
               |""".stripMargin)
    }
  }

  def hasFieldId(field: StructField): Boolean = ParquetUtils.hasFieldId(field)

  def getFieldId(field: StructField): Int = ParquetUtils.getFieldId(field)

  def fieldIdToFieldMap(useFieldId: Boolean, fileType: Type): Map[Int, Type] = {
    if (useFieldId) {
      fileType.asGroupType().getFields.asScala.filter(_.getId != null)
          .map(f => f.getId.intValue() -> f).toMap
    } else {
      Map.empty[Int, Type]
    }
  }

  def fieldIdToNameMap(useFieldId: Boolean, fileType: Type): Map[Int, String] = {
    if (useFieldId) {
      fileType.asGroupType().getFields.asScala.filter(_.getId != null)
          .map(f => f.getId.intValue() -> f.getName).toMap
    } else {
      Map.empty[Int, String]
    }
  }

  def getName(map: Map[Int, String], field: StructField): Option[String] = {
    if (ParquetUtils.hasFieldId(field)) {
      val fieldId = ParquetUtils.getFieldId(field)
      map.get(fieldId)
    } else {
      None
    }
  }

  /** The stub for the config not defined in Spark 330 */
  def timestampNTZEnabled(conf: SQLConf): Boolean = false

  def clipSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      timestampNTZEnabled: Boolean): MessageType = {

    // If useFieldId, Spark generate random `_fake_name_UUID` columns for the columns that
    // not exist in `parquetSchema`
    ParquetReadSupport.clipParquetSchema(parquetSchema, catalystSchema, caseSensitive,
      useFieldId)
  }
}
