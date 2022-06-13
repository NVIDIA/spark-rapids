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

package com.nvidia.spark.rapids

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.types._

object GpuParquetFileFormat {
  def tagGpuSupport(
      meta: RapidsMeta[_, _],
      spark: SparkSession,
      options: Map[String, String],
      schema: StructType): Unit = {

    val sqlConf = spark.sessionState.conf
    val parquetOptions = new ParquetOptions(options, sqlConf)

    val columnEncryption = options.getOrElse("parquet.encryption.column.keys", "")
    val footerEncryption = options.getOrElse("parquet.encryption.footer.key", "")

    if (!columnEncryption.isEmpty || !footerEncryption.isEmpty) {
      meta.willNotWorkOnGpu("Encryption is not yet supported on GPU. If encrypted Parquet " +
          "writes are not required unset the \"parquet.encryption.column.keys\" and " +
          "\"parquet.encryption.footer.key\" in Parquet options")
    }

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetWriteEnabled) {
      meta.willNotWorkOnGpu("Parquet output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_WRITE} to true")
    }

    FileFormatChecks.tag(meta, schema, ParquetFormatType, WriteFileOp)

    parseCompressionType(parquetOptions.compressionCodecClassName)
      .getOrElse(meta.willNotWorkOnGpu(
        s"compression codec ${parquetOptions.compressionCodecClassName} is not supported"))

    if (sqlConf.writeLegacyParquetFormat) {
      meta.willNotWorkOnGpu("Spark legacy format is not supported")
    }

    if (!meta.conf.isParquetInt96WriteEnabled && sqlConf.parquetOutputTimestampType ==
      ParquetOutputTimestampType.INT96) {
      meta.willNotWorkOnGpu(s"Writing INT96 is disabled, if you want to enable it turn it on by " +
        s"setting the ${RapidsConf.ENABLE_PARQUET_INT96_WRITE} to true. NOTE: check " +
        "out the compatibility.md to know about the limitations associated with INT96 writer")
    }

    val schemaHasTimestamps = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    if (schemaHasTimestamps) {
      if(!isOutputTimestampTypeSupported(sqlConf.parquetOutputTimestampType)) {
        meta.willNotWorkOnGpu(s"Output timestamp type " +
          s"${sqlConf.parquetOutputTimestampType} is not supported")
      }
    }

    val schemaHasDates = schema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[DateType])
    }

    // Spark 2.x doesn't have the rebase mode because the changes of calendar type weren't made
    // so just skip the checks, since this is just explain only it would depend on how
    // they set when they get to 3.x. The default in 3.x is EXCEPTION which would be good
    // for us.
    /*
    SparkShimImpl.int96ParquetRebaseWrite(sqlConf) match {
      case "EXCEPTION" =>
      case "CORRECTED" =>
      case "LEGACY" =>
        if (schemaHasTimestamps) {
          meta.willNotWorkOnGpu("LEGACY rebase mode for int96 timestamps is not supported")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported rebase mode for int96")
    }

    SparkShimImpl.parquetRebaseWrite(sqlConf) match {
      case "EXCEPTION" => //Good
      case "CORRECTED" => //Good
      case "LEGACY" =>
        if (schemaHasDates || schemaHasTimestamps) {
          meta.willNotWorkOnGpu("LEGACY rebase mode for dates and timestamps is not supported")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported rebase mode")
    }
    */
  }

  // SPARK 2.X  - just return String rather then CompressionType
  def parseCompressionType(compressionType: String): Option[String] = {
    compressionType match {
      case "NONE" | "UNCOMPRESSED" => Some("NONE")
      case "SNAPPY" => Some("SNAPPY")
      case _ => None
    }
  }

  def isOutputTimestampTypeSupported(
     outputTimestampType: ParquetOutputTimestampType.Value): Boolean = {
    outputTimestampType match {
      case ParquetOutputTimestampType.TIMESTAMP_MICROS |
           ParquetOutputTimestampType.TIMESTAMP_MILLIS |
           ParquetOutputTimestampType.INT96 => true
      case _ => false
    }
  }
}
