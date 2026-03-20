/*
 * Copyright (c) 2022-2026, NVIDIA CORPORATION.
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

  // Iceberg 1.9.0 is the only release where IcebergBuild.version() does not return a
  // valid version (it returns "unspecified", fixed in 1.9.1). Identify it by its
  // known release commit ID.
  private val ICEBERG_190_COMMIT = "7dbafb438ee1e68d0047bebcb587265d7d87d8a1"

  /**
   * Returns the exact Iceberg version string (e.g. "1.6.1", "1.9.2", "1.10.1")
   * from the runtime jar via IcebergBuild.version().
   *
   * Falls back to commit ID matching for iceberg 1.9.0 (which has a known bug
   * where version() returns "unspecified"), and to "unknown" as a last resort.
   */
  lazy val detectedVersion: String = {
    try {
      val clazz = ShimReflectionUtils.loadClass("org.apache.iceberg.IcebergBuild")
      val version = clazz.getMethod("version").invoke(null).asInstanceOf[String]
      if (version.matches("\\d+\\.\\d+\\.\\d+.*")) {
        version
      } else {
        val commitId = clazz.getMethod("gitCommitId").invoke(null).asInstanceOf[String]
        if (commitId == ICEBERG_190_COMMIT) "1.9.0" else version
      }
    } catch {
      case _: Exception => "unknown"
    }
  }

  // (Spark major.minor.patch, Iceberg major.minor) -> shim sub-package
  private val sparkIcebergToShim: Map[(String, String), String] = Map(
    ("3.5.0", "1.6") -> "iceberg16x",
    ("3.5.1", "1.6") -> "iceberg16x",
    ("3.5.2", "1.6") -> "iceberg16x",
    ("3.5.3", "1.6") -> "iceberg16x",
    ("3.5.4", "1.9") -> "iceberg19x",
    ("3.5.4", "1.10") -> "iceberg110x",
    ("3.5.5", "1.9") -> "iceberg19x",
    ("3.5.5", "1.10") -> "iceberg110x",
    ("3.5.6", "1.9") -> "iceberg19x",
    ("3.5.6", "1.10") -> "iceberg110x",
    ("3.5.7", "1.9") -> "iceberg19x",
    ("3.5.7", "1.10") -> "iceberg110x",
    ("3.5.8", "1.9") -> "iceberg19x",
    ("3.5.8", "1.10") -> "iceberg110x"
  )

  /**
   * Returns the version-specific sub-package for the current Spark + Iceberg combination.
   * Used by both [[ShimLoaderTemp]] and [[ShimUtils]] to load the correct shim classes.
   */
  lazy val shimPackage: String = {
    val sparkVersion = ShimLoader.getShimVersion match {
      case SparkShimVersion(major, minor, patch) => s"$major.$minor.$patch"
      case v => v.toString
    }
    val icebergParts = detectedVersion.split("\\.")
    if (icebergParts.length < 2) {
      throw new UnsupportedOperationException(
        s"Unrecognized Iceberg version: $detectedVersion on Spark $sparkVersion")
    }
    val icebergMajorMinor = s"${icebergParts(0)}.${icebergParts(1)}"
    val key = (sparkVersion, icebergMajorMinor)
    val subpackage = sparkIcebergToShim.getOrElse(key,
      throw new UnsupportedOperationException(
        s"Unsupported Spark/Iceberg combination: Spark $sparkVersion, Iceberg $detectedVersion"))
    s"com.nvidia.spark.rapids.iceberg.$subpackage"
  }

  def isSupportedSparkVersion(): Boolean = {
    ShimLoader.getShimVersion match {
      case _: SparkShimVersion =>
        VersionUtils.cmpSparkVersion(3, 5, 0) >= 0 &&
        VersionUtils.cmpSparkVersion(4, 0, 0) < 0
      case _ => false
    }
  }
}
