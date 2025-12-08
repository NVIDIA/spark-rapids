/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package org.apache.iceberg.spark.source

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.iceberg.FileScanTask
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.read.{Statistics, SupportsRuntimeFiltering}
import org.apache.spark.sql.sources.Filter

import java.util.Objects

class GpuSparkCopyOnWriteScan(
    override val cpuScan: SparkCopyOnWriteScan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean) extends
  GpuSparkPartitioningAwareScan[FileScanTask](cpuScan, rapidsConf, queryUsesInputFile)
  with SupportsRuntimeFiltering {

  override def filterAttributes(): Array[NamedReference] = cpuScan.filterAttributes()

  override def estimateStatistics(): Statistics = cpuScan.estimateStatistics()

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkCopyOnWriteScan =>
        this.cpuScan == that.cpuScan &&
          this.queryUsesInputFile == that.queryUsesInputFile
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuScan, Boolean.box(queryUsesInputFile))
  }

  override def toString: String = {
    s"GpuSparkCopyOnWriteScan(table=${cpuScan.table()}, " +
      s"branch=${cpuScan.branch()}, " +
      s"type=${cpuScan.expectedSchema().asStruct()}, " +
      s"filters=${cpuScan.filterExpressions()}, " +
      s"caseSensitive=${cpuScan.caseSensitive()}, " +
      s"queryUseInputFile=$queryUsesInputFile)"
  }

  /** Create a version of this scan with input file name support */
  override def withInputFile(): GpuScan = {
    new GpuSparkCopyOnWriteScan(cpuScan, rapidsConf, true)
  }

  override def filter(filters: Array[Filter]): Unit = cpuScan.filter(filters)
}