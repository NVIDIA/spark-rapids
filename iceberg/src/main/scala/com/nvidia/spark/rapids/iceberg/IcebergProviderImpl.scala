/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.iceberg

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.nvidia.spark.rapids.{AppendDataExecMeta, AtomicCreateTableAsSelectExecMeta, FileFormatChecks, GpuExec, GpuExpression, GpuRowToColumnarExec, GpuScan, IcebergFormatType, OverwriteByExpressionExecMeta, OverwritePartitionsDynamicExecMeta, RapidsConf, ReadFileOp, ScanMeta, ScanRule, ShimReflectionUtils, StaticInvokeMeta, TargetSize, WriteFileOp}
import org.apache.iceberg.spark.functions.{BucketFunction, GpuBucketExpression}
import org.apache.iceberg.spark.source.{GpuSparkBatchQueryScan, GpuSparkWrite}
import org.apache.iceberg.spark.supportsCatalog

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AtomicCreateTableAsSelectExec, GpuAppendDataExec, GpuOverwriteByExpressionExec, GpuOverwritePartitionsDynamicExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec}
import org.apache.spark.sql.execution.datasources.v2.rapids.GpuAtomicCreateTableAsSelectExec

class IcebergProviderImpl extends IcebergProvider {
  override def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]] = {
    val cpuIcebergScanClass = ShimReflectionUtils.loadClass(IcebergProvider.cpuScanClassName)
    Seq(new ScanRule[Scan](
      (a, conf, p, r) => new ScanMeta[Scan](a, conf, p, r) {
        private lazy val convertedScan: Try[GpuSparkBatchQueryScan] = GpuSparkBatchQueryScan
          .tryConvert(a, this.conf)

        override def supportsRuntimeFilters: Boolean = true

        override def tagSelfForGpu(): Unit = {
          if (!this.conf.isIcebergEnabled) {
            willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG} to true")
          }

          if (!this.conf.isIcebergReadEnabled) {
            willNotWorkOnGpu("Iceberg input has been disabled. To enable set " +
                s"${RapidsConf.ENABLE_ICEBERG_READ} to true")
          }

          FileFormatChecks.tag(this, a.readSchema(), IcebergFormatType, ReadFileOp)

          Try {
            GpuSparkBatchQueryScan.isMetadataScan(a)
          } match {
            case Success(true) => willNotWorkOnGpu("scan is a metadata scan")
            case Failure(e) => willNotWorkOnGpu(s"error examining CPU Iceberg scan: $e")
            case _ =>
          }

          convertedScan match {
            case Success(s) =>
              if (s.hasNestedType) {
                willNotWorkOnGpu("Iceberg current doesn't support nested types")
              }
            case Failure(e) => willNotWorkOnGpu(s"conversion to GPU scan failed: ${e.getMessage}")
          }
        }

        override def convertToGpu(): GpuScan = convertedScan.get
      },
      "Iceberg scan",
      ClassTag(cpuIcebergScanClass))
    ).map(r => (r.getClassFor.asSubclass(classOf[Scan]), r)).toMap
  }

  override def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      GpuBucketExpression.tagExprForGpu(meta)
    } else {
      meta.willNotWorkOnGpu(s"StaticInvoke of ${expr.staticObject.getName} is not supported on GPU")
    }
  }

  override def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression = {
    if (classOf[BucketFunction.BucketBase].isAssignableFrom(expr.staticObject)) {
      val Seq(left, right) = meta.childExprs.map(_.convertToGpu())
      GpuBucketExpression(left, right)
    } else {
      throw new IllegalStateException(
        s"Should have been caught in tagExprForGpu: ${expr.staticObject.getName}")
    }
  }

  override def isSupportedWrite(write: Class[_ <: Write]): Boolean = {
    GpuSparkWrite.supports(write)
  }

  override def isSupportedCatalog(catalogClass: Class[_]): Boolean = {
    supportsCatalog(catalogClass)
  }

  override def tagForGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG.key} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE.key} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpuCtas(cpuExec, meta)
  }

  override def convertToGpu(
      cpuExec: AtomicCreateTableAsSelectExec,
      meta: AtomicCreateTableAsSelectExecMeta): GpuExec = {
    GpuAtomicCreateTableAsSelectExec(
      cpuExec.catalog,
      cpuExec.ident,
      cpuExec.partitioning,
      cpuExec.query,
      cpuExec.tableSpec,
      cpuExec.writeOptions,
      cpuExec.ifNotExists)
  }

  override def tagForGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  override def convertToGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): GpuExec = {
    var child: SparkPlan = meta.childPlans.head.convertIfNeeded()
    if (!child.supportsColumnar) {
      child = GpuRowToColumnarExec(child, TargetSize(meta.conf.gpuTargetBatchSizeBytes))
    }
    GpuAppendDataExec(
      child,
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }

  override def tagForGpu(cpuExec: OverwritePartitionsDynamicExec,
                         meta: OverwritePartitionsDynamicExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  override def convertToGpu(cpuExec: OverwritePartitionsDynamicExec,
                            meta: OverwritePartitionsDynamicExecMeta): GpuExec = {
    var child: SparkPlan = meta.childPlans.head.convertIfNeeded()
    if (!child.supportsColumnar) {
      child = GpuRowToColumnarExec(child, TargetSize(meta.conf.gpuTargetBatchSizeBytes))
    }
    GpuOverwritePartitionsDynamicExec(
      child,
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }

  override def tagForGpu(cpuExec: OverwriteByExpressionExec,
                         meta: OverwriteByExpressionExecMeta): Unit = {
    if (!meta.conf.isIcebergEnabled) {
      meta.willNotWorkOnGpu("Iceberg input and output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG} to true")
    }

    if (!meta.conf.isIcebergWriteEnabled) {
      meta.willNotWorkOnGpu("Iceberg output has been disabled. To enable set " +
        s"${RapidsConf.ENABLE_ICEBERG_WRITE} to true")
    }

    FileFormatChecks.tag(meta, cpuExec.query.schema, IcebergFormatType, WriteFileOp)

    GpuSparkWrite.tagForGpu(cpuExec.write, meta)
  }

  override def convertToGpu(cpuExec: OverwriteByExpressionExec,
                            meta: OverwriteByExpressionExecMeta): GpuExec = {
    var child: SparkPlan = meta.childPlans.head.convertIfNeeded()
    if (!child.supportsColumnar) {
      child = GpuRowToColumnarExec(child, TargetSize(meta.conf.gpuTargetBatchSizeBytes))
    }
    GpuOverwriteByExpressionExec(
      child,
      cpuExec.refreshCache,
      GpuSparkWrite.convert(cpuExec.write))
  }
}
