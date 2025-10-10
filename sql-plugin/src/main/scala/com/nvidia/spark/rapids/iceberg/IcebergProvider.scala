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

import com.nvidia.spark.rapids.{AppendDataExecMeta, AtomicCreateTableAsSelectExecMeta, GpuExec, GpuExpression, ScanRule, ShimLoader, ShimLoaderTemp, SparkShimVersion, StaticInvokeMeta, VersionUtils}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, AtomicCreateTableAsSelectExec}

/** Interfaces to avoid accessing the optional Apache Iceberg jars directly in common code. */
trait IcebergProvider {
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]

  def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit
  def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression

  def isSupportedWrite(write: Class[_ <: Write]): Boolean
  def tagForGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): Unit
  def convertToGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): GpuExec

  def isSupportedCatalog(catalogClass: Class[_]): Boolean
  def tagForGpu(cpuExec: AtomicCreateTableAsSelectExec, meta: AtomicCreateTableAsSelectExecMeta): Unit
  def convertToGpu(cpuExec: AtomicCreateTableAsSelectExec, meta: AtomicCreateTableAsSelectExecMeta): GpuExec
}

object IcebergProvider {
  def apply(): IcebergProvider = ShimLoaderTemp.newIcebergProvider()

  val cpuScanClassName: String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"

  def isSupportedSparkVersion(): Boolean = {
    ShimLoader.getShimVersion match {
      case _: SparkShimVersion =>
        VersionUtils.cmpSparkVersion(3, 5, 0) >= 0 &&
        VersionUtils.cmpSparkVersion(4, 0, 0) < 0
      case _ => false
    }
  }
}
