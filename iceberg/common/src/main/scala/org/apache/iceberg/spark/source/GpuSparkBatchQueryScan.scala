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

import java.util.Objects

import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.{GpuScan, RapidsConf}
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.FieldUtils
import org.apache.iceberg.PartitionScanTask
import org.apache.iceberg.expressions.Expression

import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Statistics, SupportsRuntimeV2Filtering}

class GpuSparkBatchQueryScan(
    override val cpuScan: SparkBatchQueryScan,
    override val rapidsConf: RapidsConf,
    override val queryUsesInputFile: Boolean) extends
  GpuSparkPartitioningAwareScan[PartitionScanTask](cpuScan, rapidsConf, queryUsesInputFile)
  with SupportsRuntimeV2Filtering {

  private val runtimeFilterExpressions: List[Expression] = FieldUtils.readField(
      cpuScan, "runtimeFilterExpressions", true)
    .asInstanceOf[java.util.List[Expression]]
    .asScala
    .toList

  override def filterAttributes(): Array[NamedReference] = cpuScan.filterAttributes()

  override def filter(predicates: Array[Predicate]): Unit = cpuScan.filter(predicates)

  override def estimateStatistics(): Statistics = cpuScan.estimateStatistics()

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: GpuSparkBatchQueryScan =>
        this.cpuScan == that.cpuScan &&
          this.queryUsesInputFile == that.queryUsesInputFile
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(cpuScan, Boolean.box(queryUsesInputFile))
  }

  override def toString: String = {
    s"GpuSparkBatchQueryScan(table=${cpuScan.table()}, )" +
      s"branch=${cpuScan.branch()}, " +
      s"type=${cpuScan.expectedSchema().asStruct()}, " +
      s"filters=${cpuScan.filterExpressions()}, " +
      s"runtimeFilters=$runtimeFilterExpressions, " +
      s"caseSensitive=${cpuScan.caseSensitive()}, " +
      s"queryUseInputFile=$queryUsesInputFile"
  }

  /** Create a version of this scan with input file name support */
  override def withInputFile(): GpuScan = {
    new GpuSparkBatchQueryScan(cpuScan, rapidsConf, true)
  }
}
