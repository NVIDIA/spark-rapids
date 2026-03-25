/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.{ShimLoader, ShimReflectionUtils, SparkShimVersion}

import org.apache.iceberg.IcebergBuild

class IcebergProbeImpl extends IcebergProbe {
  // Git commit ID -> version for all supported Iceberg releases.
  // Commit IDs are from iceberg-build.properties embedded in each release jar.
  // https://github.com/apache/iceberg/releases
  private val commitToVersion: Map[String, String] = Map(
    "229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed" -> "1.6.0",
    "8e9d59d299be42b0bca9461457cd1e95dbaad086" -> "1.6.1",
    "7dbafb438ee1e68d0047bebcb587265d7d87d8a1" -> "1.9.0", // version() returns "unspecified"
    "f40208ae6fb2f33e578c2637d3dea1db18739f31" -> "1.9.1",
    "071d5606bc6199a0be9b3f274ec7fbf111d88821" -> "1.9.2",
    "2114bf631e49af532d66e2ce148ee49dd1dd1f1f" -> "1.10.0",
    "ccb8bc435062171e64bc8b7e5f56e6aed9c5b934" -> "1.10.1"
  )

  override def getDetectedVersion: String = {
    val commitId = IcebergBuild.gitCommitId()
    commitToVersion.getOrElse(commitId, commitId)
  }

  // (Spark feature.major.patch, Iceberg major.minor) -> shim sub-package
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

  override def shimPackage: String = {
    val sparkVersion = ShimLoader.getShimVersion match {
      case SparkShimVersion(major, minor, patch) => s"$major.$minor.$patch"
      case v => v.toString
    }
    val version = getDetectedVersion
    val icebergParts = version.split("\\.")
    if (icebergParts.length < 2) {
      throw new UnsupportedOperationException(
        s"Unrecognized Iceberg version: $version on Spark $sparkVersion")
    }
    val icebergMajorMinor = s"${icebergParts(0)}.${icebergParts(1)}"
    val key = (sparkVersion, icebergMajorMinor)
    val subpackage = sparkIcebergToShim.getOrElse(key,
      throw new UnsupportedOperationException(
        s"Unsupported Spark/Iceberg combination: Spark $sparkVersion, Iceberg $version"))
    s"com.nvidia.spark.rapids.iceberg.$subpackage"
  }

  override def getProvider: IcebergProvider = {
    ShimReflectionUtils.newInstanceOf[IcebergProvider](
      s"${shimPackage}.IcebergProviderImpl")
  }
}
