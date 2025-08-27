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

import com.nvidia.spark.rapids.{AppendDataExecMeta, ExprRule, GpuExec, ScanRule, ShimLoader, ShimLoaderTemp, SparkShimVersion, VersionUtils}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.datasources.v2.AppendDataExec

/** Interfaces to avoid accessing the optional Apache Iceberg jars directly in common code. */
trait IcebergProvider {
  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]
  def getExprs: Map[Class[_ <: Expression], ExprRule[_ <: Expression]]

  def isSupportedWrite(write: Class[_ <: Write]): Boolean
  def tagForGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): Unit
  def convertToGpu(cpuExec: AppendDataExec, meta: AppendDataExecMeta): GpuExec
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
