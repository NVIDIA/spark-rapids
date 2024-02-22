/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import com.google.common.base.Objects
import com.nvidia.spark.rapids.GpuScan

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.rapids.DataSourceStrategyUtils
import org.apache.spark.sql.execution.datasources.v2._

case class GpuBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: GpuScan,
    runtimeFilters: Seq[Expression] = Seq.empty)
    extends GpuBatchScanExecBase(scan, runtimeFilters) {

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: GpuBatchScanExec =>
      this.batch == other.batch && this.runtimeFilters == other.runtimeFilters
    case _ =>
      false
  }

  override def hashCode(): Int = Objects.hashCode(batch, runtimeFilters)

  @transient override lazy val partitions: Seq[InputPartition] = batch.planInputPartitions()

  @transient override protected lazy val filteredPartitions: Seq[InputPartition] = {
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

  override def doCanonicalize(): GpuBatchScanExec = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }
}