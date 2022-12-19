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

package com.nvidia.spark.rapids.shims

import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.rapids.shims.{GpuDivideYMInterval, GpuMultiplyYMInterval}
import org.apache.spark.sql.types.StructType

trait Spark330PlusShims extends Spark321PlusShims with Spark320PlusNonDBShims {

  override def neverReplaceShowCurrentNamespaceCommand: ExecRule[_ <: SparkPlan] = null

  override def getFileScanRDD(
      sparkSession: SparkSession,
      readFunction: PartitionedFile => Iterator[InternalRow],
      filePartitions: Seq[FilePartition],
      readDataSchema: StructType,
      metadataColumns: Seq[AttributeReference]): RDD[InternalRow] = {
    new FileScanRDD(sparkSession, readFunction, filePartitions, readDataSchema, metadataColumns)
  }

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
      .datetimeRebaseSpec(lookupFileMeta, dateTimeRebaseModeFromConf)
    new ParquetFilters(schema, pushDownDate, pushDownTimestamp, pushDownDecimal, pushDownStartWith,
      pushDownInFilterThreshold, caseSensitive, datetimeRebaseMode)
  }

  override def tagFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (meta.wrapped.expressions.exists {
      case FileSourceMetadataAttribute(_) => true
      case _ => false
    }) {
      meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")
    }
    super.tagFileSourceScanExec(meta)
  }

  // GPU support ANSI interval types from 330
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[MultiplyYMInterval](
        "Year-month interval * number",
        ExprChecks.binaryProject(
          TypeSig.YEARMONTH,
          TypeSig.YEARMONTH,
          ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[MultiplyYMInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuMultiplyYMInterval(lhs, rhs)
        }),
      GpuOverrides.expr[DivideYMInterval](
        "Year-month interval * operator",
        ExprChecks.binaryProject(
          TypeSig.YEARMONTH,
          TypeSig.YEARMONTH,
          ("lhs", TypeSig.YEARMONTH, TypeSig.YEARMONTH),
          ("rhs", TypeSig.gpuNumeric - TypeSig.DECIMAL_128, TypeSig.gpuNumeric)),
        (a, conf, p, r) => new BinaryExprMeta[DivideYMInterval](a, conf, p, r) {
          override def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression =
            GpuDivideYMInterval(lhs, rhs)
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map ++ DayTimeIntervalShims.exprs ++ RoundingShims.exprs
  }

  // GPU support ANSI interval types from 330
  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val map: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[BatchScanExec](
        "The backend for most file input",
        ExecChecks(
          (TypeSig.commonCudfTypes + TypeSig.STRUCT + TypeSig.MAP + TypeSig.ARRAY +
              TypeSig.DECIMAL_128 + TypeSig.BINARY +
              GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new BatchScanExecMeta(p, conf, parent, r)),
      GpuOverrides.exec[FileSourceScanExec](
        "Reading data from files, often from Hive tables",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.STRUCT + TypeSig.MAP +
            TypeSig.ARRAY + TypeSig.DECIMAL_128 + TypeSig.BINARY +
            GpuTypeShims.additionalCommonOperatorSupportedTypes).nested(),
          TypeSig.all),
        (fsse, conf, p, r) => new FileSourceScanExecMeta(fsse, conf, p, r))
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ map ++ PythonMapInArrowExecShims.execs
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
