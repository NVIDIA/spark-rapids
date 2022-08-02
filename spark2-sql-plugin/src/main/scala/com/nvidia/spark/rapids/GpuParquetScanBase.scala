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

import com.nvidia.spark.rapids.shims._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, TrampolineUtil}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object GpuReadParquetFileFormat {
  
  def tagSupport(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val fsse = meta.wrapped
    val session = fsse.sqlContext.sparkSession
    GpuParquetScan.tagSupport(session, fsse.requiredSchema, meta)
  }
}

object GpuParquetScan {

  // Spark 2.x doesn't have datasource v2 so ignore ScanMeta
  // Spark 2.x doesn't have the rebase mode so ignore throwIfNeeded

  def tagSupport(
      sparkSession: SparkSession,
      readSchema: StructType,
      meta: RapidsMeta[_, _]): Unit = {
    val sqlConf = sparkSession.conf

    if (!meta.conf.isParquetEnabled) {
      meta.willNotWorkOnGpu("Parquet input and output has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET} to true")
    }

    if (!meta.conf.isParquetReadEnabled) {
      meta.willNotWorkOnGpu("Parquet input has been disabled. To enable set" +
        s"${RapidsConf.ENABLE_PARQUET_READ} to true")
    }

    FileFormatChecks.tag(meta, readSchema, ParquetFormatType, ReadFileOp)

    val schemaHasTimestamps = readSchema.exists { field =>
      TrampolineUtil.dataTypeExistsRecursively(field.dataType, _.isInstanceOf[TimestampType])
    }
    def isTsOrDate(dt: DataType) : Boolean = dt match {
      case TimestampType | DateType => true
      case _ => false
    }
    val schemaMightNeedNestedRebase = readSchema.exists { field =>
      if (DataTypeUtils.isNestedType(field.dataType)) {
        TrampolineUtil.dataTypeExistsRecursively(field.dataType, isTsOrDate)
      } else {
        false
      }
    }

    // Currently timestamp conversion is not supported.
    // If support needs to be added then we need to follow the logic in Spark's
    // ParquetPartitionReaderFactory and VectorizedColumnReader which essentially
    // does the following:
    //   - check if Parquet file was created by "parquet-mr"
    //   - if not then look at SQLConf.SESSION_LOCAL_TIMEZONE and assume timestamps
    //     were written in that timezone and convert them to UTC timestamps.
    // Essentially this should boil down to a vector subtract of the scalar delta
    // between the configured timezone's delta from UTC on the timestamp data.
    if (schemaHasTimestamps && sparkSession.sessionState.conf.isParquetINT96TimestampConversion) {
      meta.willNotWorkOnGpu("GpuParquetScan does not support int96 timestamp conversion")
    }

    // Spark 2.x doesn't have the rebase mode because the changes of calendar type weren't made
    // so just skip the checks, since this is just explain only it would depend on how
    // they set when they get to 3.x. The default in 3.x is EXCEPTION which would be good
    // for us.

    // Spark 2.x doesn't support the rebase mode
    /*
    sqlConf.get(SparkShimImpl.int96ParquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${SparkShimImpl.int96ParquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${SparkShimImpl.int96ParquetRebaseReadKey} is LEGACY")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }

    sqlConf.get(SparkShimImpl.parquetRebaseReadKey) match {
      case "EXCEPTION" => if (schemaMightNeedNestedRebase) {
        meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
            s"${SparkShimImpl.parquetRebaseReadKey} is EXCEPTION")
      }
      case "CORRECTED" => // Good
      case "LEGACY" => // really is EXCEPTION for us...
        if (schemaMightNeedNestedRebase) {
          meta.willNotWorkOnGpu("Nested timestamp and date values are not supported when " +
              s"${SparkShimImpl.parquetRebaseReadKey} is LEGACY")
        }
      case other =>
        meta.willNotWorkOnGpu(s"$other is not a supported read rebase mode")
    }
    */
  }

  /**
   * This estimates the number of nodes in a parquet footer schema based off of the parquet spec
   * Specifically https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
   */
  private def numNodesEstimate(dt: DataType): Long = dt match {
    case StructType(fields) =>
      // A struct has a group node that holds the children
      1 + fields.map(f => numNodesEstimate(f.dataType)).sum
    case ArrayType(elementType, _) =>
      // A List/Array has one group node to tag it as a list and another one
      // that is marked as repeating.
      2 + numNodesEstimate(elementType)
    case MapType(keyType, valueType, _) =>
      // A Map has one group node to tag it as a map and another one
      // that is marked as repeating, but holds the key/value
      2 + numNodesEstimate(keyType) + numNodesEstimate(valueType)
    case _ =>
      // All the other types are just value types and are represented by a non-group node
      1
  }

  /**
   * Adjust the footer reader type based off of a heuristic.
   */
  def footerReaderHeuristic(
      inputValue: ParquetFooterReaderType.Value,
      data: StructType,
      read: StructType): ParquetFooterReaderType.Value = {
    inputValue match {
      case ParquetFooterReaderType.AUTO =>
        val dnc = numNodesEstimate(data)
        val rnc = numNodesEstimate(read)
        if (rnc.toDouble/dnc <= 0.5 && dnc - rnc > 10) {
          ParquetFooterReaderType.NATIVE
        } else {
          ParquetFooterReaderType.JAVA
        }
      case other => other
    }
  }
}
