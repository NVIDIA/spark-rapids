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

package com.nvidia.spark.rapids.iceberg

import com.nvidia.spark.rapids.ScanRule

import org.apache.spark.sql.connector.read.Scan

/** Interfaces to avoid accessing the optional Apache Iceberg jars directly in common code. */
trait IcebergProvider {
  def isSupportedScan(scan: Scan): Boolean

  def getScans: Map[Class[_ <: Scan], ScanRule[_ <: Scan]]
}

object IcebergProvider {
  val cpuScanClassName: String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
}
