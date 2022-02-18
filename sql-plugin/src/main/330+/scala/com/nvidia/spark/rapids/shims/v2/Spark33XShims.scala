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

package com.nvidia.spark.rapids.shims.v2

import com.nvidia.spark.rapids._
import org.apache.parquet.schema.MessageType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.json.rapids.shims.v2.Spark33XFileOptionsShims
import org.apache.spark.sql.connector.read.{Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.{CoalesceExec, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFilters
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types.StructType

trait Spark33XShims extends Spark33XFileOptionsShims {
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

  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = Seq(
    GpuOverrides.scan[ParquetScan](
      "Parquet parsing",
      (a, conf, p, r) => new ScanMeta[ParquetScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuParquetScanBase.tagSupport(this)
          // we are being overly cautious and that Parquet does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Parquet does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
          if (a.pushedAggregate.nonEmpty) {
            willNotWorkOnGpu(
              "aggregates pushed into Parquet read, which is a metadata only operation"
            )
          }
        }

        override def convertToGpu(): Scan = {
          GpuParquetScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.pushedFilters,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf)
        }
      }),
    GpuOverrides.scan[OrcScan](
      "ORC parsing",
      (a, conf, p, r) => new ScanMeta[OrcScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuOrcScanBase.tagSupport(this)
          // we are being overly cautious and that Orc does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Orc does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuOrcScan(a.sparkSession,
            a.hadoopConf,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.pushedFilters,
            a.partitionFilters,
            a.dataFilters,
            conf)
      }),
    GpuOverrides.scan[CSVScan](
      "CSV parsing",
      (a, conf, p, r) => new ScanMeta[CSVScan](a, conf, p, r) {
        override def tagSelfForGpu(): Unit = {
          GpuCSVScan.tagSupport(this)
          // we are being overly cautious and that Csv does not support this yet
          if (a.isInstanceOf[SupportsRuntimeFiltering]) {
            willNotWorkOnGpu("Csv does not support Runtime filtering (DPP)" +
              " on datasource V2 yet.")
          }
        }

        override def convertToGpu(): Scan =
          GpuCSVScan(a.sparkSession,
            a.fileIndex,
            a.dataSchema,
            a.readDataSchema,
            a.readPartitionSchema,
            a.options,
            a.partitionFilters,
            a.dataFilters,
            conf.maxReadBatchSizeRows,
            conf.maxReadBatchSizeBytes)
      })
  ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap

  override def tagFileSourceScanExec(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    if (meta.wrapped.expressions.exists(expr => expr match {
      case MetadataAttribute(expr) => true
      case _ => false
    })) {
      meta.willNotWorkOnGpu("hidden metadata columns are not supported on GPU")
    }
    super.tagFileSourceScanExec(meta)
  }

  // 330+ supports YEARMONTH and DAYTIME interval types
  override def getFileFormats: Map[FileFormatType, Map[FileFormatOp, FileFormatChecks]] = {
    Map(
      (ParquetFormatType, FileFormatChecks(
        cudfRead = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
        cudfWrite = (TypeSig.commonCudfTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT +
            TypeSig.ARRAY + TypeSig.MAP + TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
        sparkSig = (TypeSig.cpuAtomics + TypeSig.STRUCT + TypeSig.ARRAY + TypeSig.MAP +
            TypeSig.UDT + TypeSig.YEARMONTH + TypeSig.DAYTIME).nested())))
  }

  // 330+ supports YEARMONTH and DAYTIME interval types
  override def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64
    val map: Map[Class[_ <: Expression], ExprRule[_ <: Expression]] = Seq(
      GpuOverrides.expr[Coalesce](
        "Returns the first non-null argument if exists. Otherwise, null",
        ExprChecks.projectOnly(
          (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
              TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
          TypeSig.all,
          repeatingParamCheck = Some(RepeatingParamCheck("param",
            (_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.ARRAY + TypeSig.STRUCT +
                TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
            TypeSig.all))),
        (a, conf, p, r) => new ExprMeta[Coalesce](a, conf, p, r) {
          override def convertToGpu():
          GpuExpression = GpuCoalesce(childExprs.map(_.convertToGpu()))
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[Expression]), r)).toMap
    super.getExprs ++ map
  }

  // 330+ supports YEARMONTH and DAYTIME interval types
  override def getExecs: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = {
    val _gpuCommonTypes = TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64
    val map: Map[Class[_ <: SparkPlan], ExecRule[_ <: SparkPlan]] = Seq(
      GpuOverrides.exec[CoalesceExec](
        "The backend for the dataframe coalesce method",
        ExecChecks((_gpuCommonTypes + TypeSig.DECIMAL_128 + TypeSig.STRUCT + TypeSig.ARRAY +
            TypeSig.MAP + TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (coalesce, conf, parent, r) => new SparkPlanMeta[CoalesceExec](coalesce, conf, parent, r) {
          override def convertToGpu(): GpuExec =
            GpuCoalesceExec(coalesce.numPartitions, childPlans.head.convertIfNeeded())
        }),
      GpuOverrides.exec[DataWritingCommandExec](
        "Writing data",
        ExecChecks((TypeSig.commonCudfTypes + TypeSig.DECIMAL_128.withPsNote(
          TypeEnum.DECIMAL, "128bit decimal only supported for Orc and Parquet") +
            TypeSig.STRUCT.withPsNote(TypeEnum.STRUCT, "Only supported for Parquet") +
            TypeSig.MAP.withPsNote(TypeEnum.MAP, "Only supported for Parquet") +
            TypeSig.ARRAY.withPsNote(TypeEnum.ARRAY, "Only supported for Parquet") +
            TypeSig.YEARMONTH + TypeSig.DAYTIME).nested(),
          TypeSig.all),
        (p, conf, parent, r) => new SparkPlanMeta[DataWritingCommandExec](p, conf, parent, r) {
          override val childDataWriteCmds: scala.Seq[DataWritingCommandMeta[_]] =
            Seq(GpuOverrides.wrapDataWriteCmds(p.cmd, conf, Some(this)))

          override def convertToGpu(): GpuExec =
            GpuDataWritingCommandExec(childDataWriteCmds.head.convertToGpu(),
              childPlans.head.convertIfNeeded())
        })
    ).map(r => (r.getClassFor.asSubclass(classOf[SparkPlan]), r)).toMap
    super.getExecs ++ map
  }

}

// Fallback to the default definition of `deterministic`
trait GpuDeterministicFirstLastCollectShim extends Expression
