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

package com.nvidia.spark.rapids.shims

import com.google.common.base.Objects
import com.nvidia.spark.rapids.{GpuBatchScanExecMetrics, ScanWithMetrics}

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.rapids.DataSourceStrategyUtils
import org.apache.spark.sql.execution.datasources.v2._

case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression] = Seq.empty)
    extends DataSourceV2ScanExecBase with GpuBatchScanExecMetrics {
  @transient lazy val batch: Batch = scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: GpuBatchScanExec =>
      this.batch == other.batch && this.runtimeFilters == other.runtimeFilters
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  @transient override lazy val partitions: Seq[InputPartition] = batch.planInputPartitions()

  @transient private lazy val filteredPartitions: Seq[InputPartition] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceStrategyUtils.translateRuntimeFilter(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty && scan.isInstanceOf[SupportsRuntimeFiltering]) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeFiltering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: DataSourcePartitioning if p.numPartitions != newPartitions.size =>
          throw new SparkException(
            "Data source must have preserved the original partitioning during runtime filtering; " +
                s"reported num partitions: ${p.numPartitions}, " +
                s"num partitions after runtime filtering: ${newPartitions.size}")
        case _ =>
        // no validation is needed as the data source did not report any specific partitioning
      }

      newPartitions
    } else {
      partitions
    }
  }

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override lazy val inputRDD: RDD[InternalRow] = {
    scan match {
      case s: ScanWithMetrics => s.metrics = allMetrics
      case _ =>
    }

    if (filteredPartitions.isEmpty && outputPartitioning == SinglePartition) {
      // return an empty RDD with 1 partition if dynamic filtering removed the only split
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      new GpuDataSourceRDD(sparkContext, filteredPartitions, readerFactory)
    }
  }

  override def doCanonicalize(): GpuBatchScanExec = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString = s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }
}
