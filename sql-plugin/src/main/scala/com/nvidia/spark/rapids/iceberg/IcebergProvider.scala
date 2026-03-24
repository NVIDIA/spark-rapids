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

import com.nvidia.spark.rapids.{GpuExec, GpuExpression, ScanRule, ShimLoader, ShimLoaderTemp, ShimReflectionUtils, SparkPlanMeta, SparkShimVersion, StaticInvokeMeta, VersionUtils}

import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan

/** Interfaces to avoid accessing the optional Apache Iceberg jars directly in common code. */
trait IcebergProvider {
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]

  def tagForGpu(expr: StaticInvoke, meta: StaticInvokeMeta): Unit
  def convertToGpu(expr: StaticInvoke, meta: StaticInvokeMeta): GpuExpression

  def isSupportedWrite(write: Class[_ <: Write]): Boolean
  def isSupportedCatalog(catalogClass: Class[_]): Boolean

  def tagForGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): Unit
  def convertToGpuPlan[P <: SparkPlan, M <: SparkPlanMeta[P]](cpuExec: P, meta: M): GpuExec
}

object IcebergProvider {
  def apply(): IcebergProvider = ShimLoaderTemp.newIcebergProvider()

  val cpuBatchQueryScanClassName: String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
  val cpuCopyOnWriteScanClassName: String = "org.apache.iceberg.spark.source.SparkCopyOnWriteScan"

  /**
   * Returns the version-specific sub-package for the current Spark + Iceberg combination.
   * Used by both [[ShimLoaderTemp]] and [[ShimUtils]] to load the correct shim classes.
   */
  lazy val shimPackage: String = {
    if (VersionUtils.cmpSparkVersion(4, 0, 0) >= 0) {
      // Spark 4.0.x only supports iceberg 1.10.x
      "com.nvidia.spark.rapids.iceberg.iceberg110x"
    } else if (VersionUtils.cmpSparkVersion(3, 5, 4) >= 0) {
      // Spark 3.5.4+ can use iceberg 1.9.x or 1.10.x.
      // Probe the iceberg-spark-runtime jar: IdentityPartitionConverters
      // exists in iceberg <= 1.9 and was removed in 1.10.
      try {
        ShimReflectionUtils.loadClass(
          "org.apache.iceberg.actions.ComputePartitionStats")
        "com.nvidia.spark.rapids.iceberg.iceberg110x"
      } catch {
        case _: ClassNotFoundException | _: LinkageError =>
          "com.nvidia.spark.rapids.iceberg.iceberg19x"
      }
    } else {
      // Spark 3.5.0-3.5.3 ships with iceberg 1.6.x
      "com.nvidia.spark.rapids.iceberg.iceberg16x"
    }
  }

  def isSupportedSparkVersion(): Boolean = {
    ShimLoader.getShimVersion match {
      case _: SparkShimVersion =>
        VersionUtils.cmpSparkVersion(3, 5, 0) >= 0 &&
        VersionUtils.cmpSparkVersion(4, 1, 0) < 0
      case _ => false
    }
  }
}
